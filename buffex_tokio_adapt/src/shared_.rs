//! 本 crate 内两个适配器共用的装配件与容器助手。
//!
//! 这里只放「读端 / 写端都会用到」的东西，避免两个模块互相依赖：
//!
//! * [`CoreOf`]——适配器内核：设备 + 环容器 + 读写两半 + EOF / 错误暂存；
//! * [`RingBufOf`] / [`SharedRingOf`]——把 [`TrAllocConfig`](crate::TrAllocConfig)
//!   的分配点摊成具体类型；
//! * [`alloc_ring_`]——按容量与配置里的分配器造一个元素为 `u8` 的环；
//! * [`split_ring_`]——把 `Shared<Ring<…>>` 拆成一对**可长期持有**的半部；
//! * [`poll_once_`]——读端 / 写端共用的「同步 poll 一次」；
//! * [`InvalidCapacity`]——容量不在 ring 允许区间时的构造错误。
//!
//! # 分配点由 [`TrAllocConfig`](crate::TrAllocConfig) 决定
//!
//! 本模块涉及两个分配点，各自取配置里的对应关联类型：
//!
//! * 环的缓冲本体 `mm_ptr::Owned<[MaybeUninit<u8>], _>` ← `C::RingBodyAlloc`；
//! * 装环的 `mm_ptr::Shared<Ring<…>, _>` ← `C::RingSharedAlloc`。
//!
//! 两者都要求 `AllocatorClone`（`Shared<T, A>: Clone` 要求它，而 `Ring::split_unchecked`
//! 会 `clone()` 容器）。

use core::{
    alloc::AllocatorClone,
    borrow::{Borrow, BorrowMut},
    future::Future,
    mem::MaybeUninit,
    io,
    task::Poll,
};

use abs_buff::error::ReadErrTag;
use buffex::{
    ring::{Ring, RingReader, RingWriter},
    x_deps::abs_buff,
};
use mm_ptr::{Owned, Shared};

use crate::alloc_::TrAllocConfig;

/// 某个配置下的环缓冲本体类型：`mm_ptr::Owned<[MaybeUninit<u8>], C::RingBodyAlloc>`。
///
/// 元素固定 `u8`；本体用 `mm_ptr::Owned`（而不是 `Box`）是为了不依赖 `std` 的智能指针，
/// 它由 `Owned::new_uninit_slice` 在 `allocator_api` 上分配，容量校验交给 `Ring::try_new`。
pub type RingBufOf<C> = Owned<[MaybeUninit<u8>], <C as TrAllocConfig>::RingBodyAlloc>;

/// 某个配置下的共享容器类型：`mm_ptr::Shared<Ring<RingBufOf<C>, u8>, C::RingSharedAlloc>`。
///
/// `buffex` 自己不依赖 `alloc`，共享容器由调用方决定；这里给 `mm_ptr::Shared`
/// （非 `std` 的引用计数指针），分配器同样由配置选。
pub type SharedRingOf<C> =
    Shared<Ring<RingBufOf<C>, u8>, <C as TrAllocConfig>::RingSharedAlloc>;

/// 适配器内核：设备 + 环容器 + 读写两半 + EOF / 错误暂存。
///
/// 读端与写端共用这一份定义：读端只用 `dev_` / `ring_` / `rx_` / `tx_` / `eof_`
/// 与读错误，写端只用 `dev_` / `ring_` / `tx_` / 写错误字段（读写两端的适配器是
/// 两个独立类型，不会互相共享内核实例）。
///
/// 环缓冲本体固定为 [`RingBufOf<C>`]，分配器由 `RingBufOf<C>` 与 `S` 自己携带。
///
/// # 为什么手写 `Debug`
///
/// 环的两半（[`RingReader`] / [`RingWriter`]）只有在缓冲本体 `B: Debug` 时才 `Debug`，
/// 而本 crate 的 `B` 是 `Owned<[MaybeUninit<u8>], A>`：`MaybeUninit` 从不是 `Debug`，
/// `mm_ptr::Owned` 的 `Debug` 又是**派生**的（带 `T: Debug` 约束），因此那一侧永远
/// 不满足。旧的 `Box<T>` 能过只是因为 `impl<T: ?Sized> Debug for Box<T>` 不要求
/// `T: Debug`。这里只打印与设备有关的三项，跳过需要 `B: Debug` 的环内数据。
pub(crate) struct CoreOf<D, C, S = SharedRingOf<C>>
where
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
{
    /// 外部设备（tokio `AsyncRead` / `AsyncWrite` 对象）。
    pub(crate) dev_: D,
    /// 环的共享容器；适配器持有它，是为了直接查环的全局状态（如 `data_size`）。
    pub(crate) ring_: S,
    /// 读半部（调用方经适配器消费它）。
    pub(crate) rx_: RingReader<S, RingBufOf<C>, u8>,
    /// 写半部（内部泵持有）。
    pub(crate) tx_: RingWriter<S, RingBufOf<C>, u8>,
    /// 设备是否已到 EOF（tokio 约定：`read` 返回 0）。
    pub(crate) eof_: bool,
    /// 最近一次设备读错误及其标签（经 `take_error` 取回）。
    pub(crate) err_: Option<(io::Error, ReadErrTag)>,
}

impl<D, C, S> core::fmt::Debug for CoreOf<D, C, S>
where
    D: core::fmt::Debug,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CoreOf")
            .field("dev_", &self.dev_)
            .field("eof_", &self.eof_)
            .field("err_", &self.err_)
            // 环容器与两半需要 `RingBufOf<C>: Debug`（见类型文档），故略去。
            .finish_non_exhaustive()
    }
}

impl<D, C, S> CoreOf<D, C, S>
where
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
{
    /// 装配内核。调用方负责保证 `rx_` / `tx_` 确实来自 `ring_` 且只拆过一次。
    pub(crate) const fn new_(
        dev: D,
        ring: S,
        rx: RingReader<S, RingBufOf<C>, u8>,
        tx: RingWriter<S, RingBufOf<C>, u8>,
    ) -> Self {
        CoreOf {
            dev_: dev,
            ring_: ring,
            rx_: rx,
            tx_: tx,
            eof_: false,
            err_: Option::None,
        }
    }
}

/// 容量不在 ring 允许区间时的构造错误。
///
/// `Ring` 的容量有上下限（见 `buffex::ring::Ring::check_buffer_size`），超出即拒绝。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidCapacity(pub usize);

impl core::fmt::Display for InvalidCapacity {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "容量 {} 不在 ring 允许的区间内", self.0)
    }
}

impl core::error::Error for InvalidCapacity {}

/// 按容量与配置造一个元素为 `u8` 的环，并装进配置指定的共享容器。
///
/// 缓冲本体取 `C::RingBodyAlloc`、共享容器取 `C::RingSharedAlloc`（两者可以是不同的
/// 分配器）。
///
/// # Errors
///
/// 容量被 `Ring::try_new` 拒绝时返回 [`InvalidCapacity`]。
pub(crate) fn alloc_ring_<C>(
    cap: usize,
    cfg: &C,
) -> Result<SharedRingOf<C>, InvalidCapacity>
where
    C: TrAllocConfig,
{
    let body = cfg.ring_body_alloc();
    let buff = Owned::<
        [MaybeUninit<u8>],
        <C as TrAllocConfig>::RingBodyAlloc,
    >::new_uninit_slice(cap, body);
    match Ring::try_new(buff) {
        Result::Ok(ring) => Result::Ok(Shared::new(ring, cfg.ring_shared_alloc())),
        Result::Err(cap) => Result::Err(InvalidCapacity(cap)),
    }
}

/// 从 `Shared` 容器拆出的读 / 写半部（容器具体类型保留）。
type HalfPairOf<B, A> = (
    RingWriter<Shared<Ring<B, u8>, A>, B, u8>,
    RingReader<Shared<Ring<B, u8>, A>, B, u8>,
);

/// 把环容器拆成一对半部（`unsafe` 入口的安全封装）。
///
/// `Ring` 的两个半部共享同一块 `UnsafeCell` 缓冲，**SPSC（单生产者 / 单消费者）**
/// 是安全前提。这里由本 crate 保证排他性：
///
/// * 容器是本函数调用方**刚刚创建**的 `Shared<Ring<…>>`（[`alloc_ring_`] 的产物），
///   不存在第二个句柄；
/// * 拆分后两个半部只在本 crate 的同一个适配器内核里成对存活，不会被再次传入。
pub(crate) fn split_ring_<B, A>(
    ring: Shared<Ring<B, u8>, A>,
) -> HalfPairOf<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]>,
    A: AllocatorClone,
{
    // SAFETY: 见本函数文档——容器刚创建且独占，拆分只发生一次。
    unsafe { Ring::split_unchecked(ring) }
}

/// 同步 poll 一次：`Ready` 给 `Some(v)`，`Pending` 给 `None`（future 就地被丢弃）。
///
/// 两侧适配器的 `try_*` 是「try-once」语义：设备此刻没数据 / 收不下就立即返回、不阻塞
/// 调用者，因此不能把 future 交给调用方的执行体去挂起。这里用标准库的
/// [`Waker::noop`]（Rust 1.85 起 stable，内部就是 `RawWaker::NOOP`）在栈上 poll 一次，
/// 不需要自己写 vtable、也不需要缓存 waker 的 `OnceLock`。
///
/// 被丢弃的那一侧若尚未完成，其未完成的搬运不计入已搬出量（段按已搬出量提交），
/// 因此不丢数据。
pub(crate) fn poll_once_<F: Future>(fut: F) -> Option<F::Output> {
    use core::task::{Context, Waker};

    let mut fut = core::pin::pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(out) => Option::Some(out),
        Poll::Pending => Option::None,
    }
}
