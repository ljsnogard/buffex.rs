//! 出向适配：把**任意** smol 设备变成
//! [`TrBuffWrite`](abs_buff::TrBuffWrite) / [`TrBuffTryWrite`](abs_buff::TrBuffTryWrite)。
//!
//! # 两种驱动方式
//!
//! 「环 → 设备」的搬运总得有人跑。本适配器给出两个构造入口，选哪一个取决于
//! **上层协议栈会不会替适配器调收尾入口**：
//!
//! | 构造入口 | 谁在驱动「环 → 设备」 | 适用场合 |
//! | --- | --- | --- |
//! | [`BuffWrite::try_new`] | **调用方**：每次 `try_write` / `write_async` 顺带推一轮，`flush_async` / `close_async` 负责收尾 | 调用方自己就是搬运循环（单次收发、benchmark、测试里的假设备） |
//! | [`BuffWrite::try_new_local`] | **后台泵**：构造时经 `abs_art-smol` 的 `LocalScope` 投到**本线程**那条 `LocalExecutor` 上，此后自行搬运 | 上层只把本适配器当 `TrBuffWrite` 用、**不会**替它调 flush 的协议栈（例如 `smux_v1` 的写泵） |
//!
//! ## 为什么需要第二种：`TrBuffWrite` 没有 flush 钩子
//!
//! 按需驱动形态的搬运是**借段的副作用**：`write_async` 先冲一轮「环 → 设备」，再借出
//! 空段。上层写泵一旦「把最后一个分片提交进环、然后挂到『等下层有新字节』」，那个分片
//! 就再也没人搬——它在环里等着下一次 `write_async`，而下一次永远不来了。`smux_v1` 的
//! 写泵正是这个形状（连接写环一空就 park），于是握手帧只出去一部分、连接静止。
//!
//! 自驱动形态把这个缺口补上：后台泵**独立于调用方**地循环「借读段（环空则 park）→
//! 写设备 → 提交」，因此「提交进环」本身就等于「迟早送达设备」，不需要谁记得调 flush。
//!
//! ## 为什么后台泵挂在 `LocalScope` 上，而不是 `smol::spawn`
//!
//! 泵要 await `move_items_into_output_async`，而它的 future 由 `abs_buff` 经 **GAT 投影**
//! （`TrOutput::WriteAsync<'f>`）取得；GAT 投影**不传递 auto trait**：`smol::spawn`
//! 要求 `Send`，即使设备本身是 `Send`（socket 就是），编译器也证明不出泵 future 是
//! `Send`（rust-lang/rust#100013）。`LocalScope::spawn_local` 不要求 `Send`，因此它是
//! 本 crate 唯一可行的后台投递点。
//!
//! smol 与另两个后端有一处区别：它**没有**「环境运行时」这一前提，`Runtime::current()`
//! 与 `local_scope()` 在任何线程上都能调（队列是本线程的 `thread_local!`）。因此构造
//! 自驱动形态**不要求**调用点处于某个驱动栈内；但队列必须由**持有它的线程**驱动
//! （`smol::block_on(scope.run_until(..))`），否则泵不会推进——这就是「只在本线程内
//! 运行」的字面含义。
//!
//! # 按需驱动形态（`try_new`）的语义
//!
//! [`BuffWrite`] 自己持有环（`mm_ptr::Shared<Ring<…>>`，经 `Ring::split_unchecked` 拆成两半）
//! 与设备：
//!
//! * 调用方经 [`TrBuffWrite`](abs_buff::TrBuffWrite) /
//!   [`TrBuffTryWrite`](abs_buff::TrBuffTryWrite) 往环里写段；
//! * 「环 → 设备」的搬运由本适配器**按需推进**：`try_write` 借段前先同步冲一轮
//!   （try-once，设备此刻收不下就返回 `Stuffed`），`write_async` 在等空间的过程中持续
//!   推进；收尾用 [`BuffWrite::flush_async`] / [`BuffWrite::close_async`] 把数据送出去。
//!
//! 段 drop 只是把数据提交进环（`advance_write`），**不代表已经写进设备**；因此调用方
//! 在收尾时必须 await 一次 [`BuffWrite::flush_async`]（或 [`BuffWrite::close_async`]）。
//!
//! # 自驱动形态（`try_new_local`）的语义
//!
//! * 构造时把设备与环的读半部移进后台泵，泵经 `LocalScope` 投到本线程队列上；
//! * 适配器只保留环的写半部：`try_write` / `write_async` 就是**环**的借段（环满时
//!   park，由泵提交消费量后唤醒）；
//! * 泵退出时**关闭环的消费端**，因此此后借段一律拿到 `ProducerError::Closing`；
//! * [`BuffWrite::flush_async`] = 等环被泵取空；[`BuffWrite::close_async`] = 关生产端
//!   + 等泵把剩余数据交给设备、`close` 之后退出（设备错误经它记进 `err_`）；
//! * **`drop` 适配器 = 关生产端 + detach 泵**：已提交进环的字节仍会被搬完并 `close`。
//!   前提是本线程此后仍在驱动那条队列（丢完适配器再走几轮 `run_until`）。
//!
//! # 分配点（见 [`TrAllocConfig`](crate::TrAllocConfig)）
//!
//! 写向用到的分配点都从配置 `C` 里取：
//!
//! * 环的缓冲本体 `Owned<[MaybeUninit<u8>], _>` ← `C::RingBodyAlloc`；
//! * 装环的 `mm_ptr::Shared<Ring<…>, _>` ← `C::RingSharedAlloc`。
//!
//! # 零拷贝：段直连设备
//!
//! 搬运**不经中转缓冲**：借到环的读段后，直接把段交给
//! `move_items_into_output_async(&mut output, &demand)`（`output` 是 [`WriteAsOutput`]
//! 包的设备）。设备收下多少，段的 offset 就推进多少，drop 时按该量 `advance_read`
//! 释放环空间。
//!
//! 这条路径依赖「部分搬运是逐段提交的」：内部每次设备写完成才计入已搬量并推进 offset，
//! 因此「设备只收下一部分」不会丢字节——没写完的部分仍在段里，下一次继续。真正可能
//! 丢数据的是设备自己「已吃进字节、却没能把计数交出来」，那是设备侧的取消语义。
//!
//! # 不空转（no busy-wait）的三条保证
//!
//! `buffex_tokio_adapt` 的 `2026-10-02` 实验观察到写端 100% CPU 空转，根因都在出向泵的
//! 「进展」判据与收尾循环上。本模块沿用那边定下的三条保证：
//!
//! 1. **`pump_` 的返回值只表示「真的向设备交付了至少 1 个字节」**，不以「结果里含
//!    `Left`」为准——`SomeOf::Both(0, err)` 也含 `Left`，按它算进展就是无限循环；
//! 2. **任何带错误的结果都先记进 `err_`**（含 `Both(n, err)`），设备错误不允许被
//!    吞掉：调用方的 `flush_async` / `close_async` 据此终止并可用 `take_error` 取回
//!    真实原因（例如对端退出后的 `EPIPE`）；
//! 3. **收尾循环遇到「环里还有数据、但一整轮零字节交付且设备未报错」时记一条
//!    `WriteZero` 并终止**，绝不重试到天荒地老。
//!
//! 另外，设备在**非空**缓冲上返回 `Ok(0)` 时，上游 `move_items_into_output_async`
//! 自己会空转（内层退回、外层 `continue`），且不让出执行体。这个违约行为已经在
//! [`crate::device_`] 的设备边界被翻成 `WriteZero` 错误，适配器因此能按第 2、3 条正常
//! 收尾——不需要 `buffex_tokio_adapt` 里那层 `ZeroWriteAsErr` 设备包装。

use core::{
    borrow::{Borrow, BorrowMut},
    future::poll_fn,
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
    pin::Pin,
};
use std::io;

use futures_lite::io::AsyncWrite;

use abs_art::{SPAWN_LOCAL, TrJoinHandle, TrLocalScope};
use abs_art_smol::{JoinHandle, Runtime};
use abs_buff::{
    Demand, TrBuffWrite, TrBuffTryWrite,
    buffer::{TrBuffSegmRef, TrProducerState},
    error::{TaggedError, TrErrorWrapper, WriteErrTag},
    x_deps::{abs_cancel, anylr},
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use anylr::{SomeOf, some_of::SomeLR};
use buffex::{
    ring::{ProducerError, Ring, RingReader, RingSegmMut, RingWriter},
    x_deps::abs_buff,
};

use crate::{
    alloc_::{DefaultAllocConfig, TrAllocConfig},
    device_::WriteAsOutput,
    shared_::{InvalidCapacity, RingBufOf, SharedRingOf, poll_once_, split_ring_},
};

/// 出向适配器的写半部类型（容器与缓冲本体由配置 `C` 决定）。
pub type WriterOf<C, S> = RingWriter<S, RingBufOf<C>, u8>;

/// 出向适配器的读半部类型（内部泵持有）。
pub type ReaderOf<C, S> = RingReader<S, RingBufOf<C>, u8>;

/// 自驱动形态的后台泵句柄；它的输出是「设备错误」。
type LocalPump_ = JoinHandle<Option<(io::Error, WriteErrTag)>>;

/// 取本线程的本地作用域（`SPAWN_LOCAL` 能力）。
///
/// smol 侧没有「环境运行时」前提：本函数不读任何上下文，只是取出本线程那条
/// `LocalExecutor` 的别名（队列寿命 = 线程寿命）。它是否被驱动由调用方决定。
fn local_scope_() -> abs_art_smol::LocalScope {
    Runtime::<{ SPAWN_LOCAL }>::current().local_scope()
}

/// 把**任意** smol [`AsyncWrite`] 设备接上 `buffex` 环的写端适配器。
///
/// 实现 [`TrBuffTryWrite`](abs_buff::TrBuffTryWrite) 与
/// [`TrBuffWrite`](abs_buff::TrBuffWrite)；「环 → 设备」由谁驱动取决于构造入口，
/// 见模块文档的两种驱动方式。
///
/// # 类型参数
///
/// * `W`——设备类型（`AsyncWrite + Unpin`，被适配器拥有）；
/// * `C`——分配器配置（默认 [`DefaultAllocConfig`]）；
/// * `S`——环的共享容器（默认 `mm_ptr::Shared`）。
///
/// # Examples
///
/// ```no_run
/// #![feature(allocator_ext)]
/// use buffex_smol_adapt::{BuffWrite, DefaultAllocConfig};
///
/// # async fn demo(dev: futures_lite::io::Sink) {
/// let mut w = BuffWrite::<_, DefaultAllocConfig>::try_new(
///     dev,
///     4096,
///     DefaultAllocConfig,
/// )
/// .expect("容量须在 ring 允许范围内");
/// // 写入后记得收尾（把环内数据真正写进设备）：
/// w.close_async().await.expect("写方向应正常关闭");
/// # }
/// ```
pub struct BuffWrite<W, C = DefaultAllocConfig, S = SharedRingOf<C>>
where
    W: AsyncWrite + Unpin,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
{
    /// 环的共享容器（查容量 / 「已提交、尚未被设备收下」的字节数）。
    ring_: S,
    /// 调用方这一侧的写半部：**两种驱动方式都从这里借段**。
    tx_: RingWriter<S, RingBufOf<C>, u8>,
    /// 驱动方式（设备与环的读半部在谁手里）。
    drive_: Drive_<W, C, S>,
    /// 最近一次设备写错误及其标签（经 `take_error` 取回）。
    err_: Option<(io::Error, WriteErrTag)>,
}

/// [`BuffWrite`] 的驱动方式。
enum Drive_<W, C, S>
where
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
{
    /// **调用方驱动**（`try_new`）：设备与环的读半部就在适配器里。
    Manual {
        /// 外部设备。
        dev_: W,
        /// 环的读半部（借出后交给设备）。
        rx_: RingReader<S, RingBufOf<C>, u8>,
    },
    /// **本线程自驱动**（`try_new_local`）：设备与读半部在后台泵里。
    Local {
        /// 后台泵句柄；`None` 表示已经被 `close_async` 取走。
        pump_: Option<LocalPump_>,
    },
}

// 同 `BuffRead`：适配器自身不依赖固定地址（环由 `Shared` 分配，地址稳定）。
impl<W, C, S> Unpin for BuffWrite<W, C, S>
where
    W: AsyncWrite + Unpin,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
{}

// `B = RingBufOf<C>` 从不满足 `Debug`（`MaybeUninit`），环半部因而都不 `Debug`，
// 所以这里手写：只打印驱动方式的名字与错误槽。
impl<W, C, S> core::fmt::Debug for BuffWrite<W, C, S>
where
    W: AsyncWrite + Unpin,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let mode = match self.drive_ {
            Drive_::Manual { .. } => "manual",
            Drive_::Local { .. } => "local",
        };
        f.debug_struct("BuffWrite")
            .field("mode", &mode)
            .field("err_", &self.err_)
            .finish_non_exhaustive()
    }
}

impl<W, C, S> Drop for BuffWrite<W, C, S>
where
    W: AsyncWrite + Unpin,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
{
    /// 自驱动形态的收尾：**关生产端 + detach 泵**。
    ///
    /// 只丢适配器不能等于「丢掉还没上网的字节」：关生产端会让泵在取空环之后正常退出
    /// （并 `close` 设备），detach 则让它继续在本线程队列上跑完——本线程只要还在驱动
    /// 那条队列（`run_until`）就会推进。按需驱动形态没有后台任务，drop 即原样。
    fn drop(&mut self) {
        if let Drive_::Local { pump_ } = &mut self.drive_ {
            self.tx_.close();
            if let Option::Some(pump) = pump_.take() {
                // smol 的 `Task` 丢弃即**取消**，必须显式 detach。
                pump.detach();
            }
        }
    }
}

impl<W, C> BuffWrite<W, C, SharedRingOf<C>>
where
    W: AsyncWrite + Unpin,
    C: TrAllocConfig,
{
    /// 用「设备 + 环容量 + 分配器配置」构造**按需驱动**的写端适配器（容器 `Shared`、
    /// 缓冲本体 [`RingBufOf<C>`]）。
    ///
    /// 搬运由调用方在 `try_write` / `write_async` / `flush_async` / `close_async` 上
    /// 顺带推进，**不 spawn 任何任务**；调用方不调收尾入口，环里的数据就送不出去。
    ///
    /// # Errors
    ///
    /// `cap` 不在 ring 允许的容量区间内时返回 [`InvalidCapacity`]。
    pub fn try_new(dev: W, cap: usize, alloc_cfg: C) -> Result<Self, InvalidCapacity> {
        let ring = crate::shared_::alloc_ring_(cap, &alloc_cfg)?;
        let (tx, rx) = split_ring_(ring.clone());
        Ok(BuffWrite {
            ring_: ring,
            tx_: tx,
            drive_: Drive_::Manual { dev_: dev, rx_: rx },
            err_: Option::None,
        })
    }
}

impl<W, C> BuffWrite<W, C, SharedRingOf<C>>
where
    W: AsyncWrite + Unpin + 'static,
    C: TrAllocConfig + 'static,
    C::RingBodyAlloc: 'static,
    C::RingSharedAlloc: 'static,
{
    /// 用「设备 + 环容量 + 分配器配置」构造**自驱动**的写端适配器：设备与环的读半部
    /// 移进后台泵，泵经 `abs_art-smol` 的 [`LocalScope`](abs_art_smol::LocalScope) 投到
    /// **本线程**那条 `LocalExecutor` 上，此后自行把环里的字节搬给设备。
    ///
    /// 这是「上层协议栈只会借段、不会替适配器调 flush」时的正确入口（例如
    /// `smux_v1` 的写泵）。
    ///
    /// # 与 tokio 侧的差别
    ///
    /// smol 没有环境运行时：本入口**不要求**调用点处于某个驱动栈内（队列是本线程的
    /// `thread_local!`），但也因此**不保证**泵会被推进——调用方必须继续驱动本线程的
    /// 本地队列（`smol::block_on(scope.run_until(..))` 或等价物）。
    ///
    /// # Errors
    ///
    /// `cap` 不在 ring 允许的容量区间内时返回 [`InvalidCapacity`]。
    pub fn try_new_local(dev: W, cap: usize, alloc_cfg: C) -> Result<Self, InvalidCapacity> {
        let ring = crate::shared_::alloc_ring_(cap, &alloc_cfg)?;
        let (tx, rx) = split_ring_(ring.clone());
        let scope = local_scope_();
        let pump = scope.spawn_local(write_pump_::<W, C, SharedRingOf<C>>(dev, rx));
        Ok(BuffWrite {
            ring_: ring,
            tx_: tx,
            drive_: Drive_::Local {
                pump_: Option::Some(pump),
            },
            err_: Option::None,
        })
    }
}

impl<W, C, S> BuffWrite<W, C, S>
where
    W: AsyncWrite + Unpin,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
{
    /// 取回最近一次设备的写错误及其标签（取走即清空）。
    ///
    /// 自驱动形态下，错误由后台泵带出、在 [`close_async`](Self::close_async) 里落进
    /// 本槽；不调 `close_async` 就丢适配器时，错误随泵一起消失（只能从
    /// `ProducerError::Closing` 看出「写方向没了」）。
    pub fn take_error(&mut self) -> Option<(io::Error, WriteErrTag)> {
        self.err_.take()
    }

    /// 环容量（元素个数）。
    pub fn capacity(&self) -> usize {
        self.ring_.borrow().capacity()
    }

    /// 环里还有多少**已提交、尚未被设备收下**的字节。
    ///
    /// 按需驱动形态下由调用方推进，因此它只在 `flush_async` / `close_async` 之后才保证
    /// 归零；自驱动形态下泵会尽快把它清零。
    pub fn data_size(&self) -> usize {
        self.ring_.borrow().data_size()
    }

    /// 借出可写段（try-once）。
    ///
    /// * 按需驱动：先同步冲一轮「环 → 设备」，再尝试借出空段；
    /// * 自驱动：只借段（搬运在后台泵里）。
    ///
    /// # Errors
    ///
    /// 环满返回 `Stuffed`；写方向已关闭返回 `Closing`（错误详情经
    /// [`take_error`](Self::take_error) 取回）。
    pub fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<RingSegmMut<'f, RingBufOf<C>, u8>, ProducerError<usize>> {
        if matches!(&self.drive_, Drive_::Manual { .. }) {
            // 同步冲一轮：把环里已提交的数据交给设备（try-once，设备收不下就算了）。
            let _ = poll_once_(self.pump_());
            if self.err_.is_some() {
                return SomeOf::new_right(ProducerError::Closing);
            }
        }
        // 自驱动形态：设备那侧有泵，「写方向没了」由泵关消费端表达 —— 环自己给 Closing。
        self.tx_.try_write(demand)
    }

    /// 借出可写段（异步）：环满时先推进「环 → 设备」的搬运，再等生产端腾出空间。
    ///
    /// 调用方可经 [`may_cancel_with`](abs_cancel::TrMayCancel::may_cancel_with) 装入
    /// 取消信号。
    pub fn write_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> BuffWriteOpAsync<'f, 'f, W, C, S> {
        BuffWriteOpAsync::new(self, demand)
    }

    /// 把环里已提交的数据尽量搬给设备（异步，可取消）。
    ///
    /// 调用方在「不再写新数据、但要求已提交数据确实送达设备」时 await 它。自驱动形态下
    /// 它就是「等泵把环取空」。
    pub fn flush_async(&mut self) -> BuffWriteFlushOpAsync<'_, '_, W, C, S> {
        BuffWriteFlushOpAsync::new(self)
    }

    /// 冲刷剩余数据并 `close` 设备（关闭写方向）。
    ///
    /// 之后 [`try_write`](Self::try_write) 一律返回 `Closing`。
    ///
    /// # Errors
    ///
    /// 设备写 / `close` 出错时返回 `Closing`（详情经
    /// [`take_error`](Self::take_error) 取回）。
    pub fn close_async(&mut self) -> BuffWriteCloseOpAsync<'_, '_, W, C, S> {
        BuffWriteCloseOpAsync::new(self)
    }

    /// 推一轮「环 → 设备」的搬运（try-once），返回**本次是否真的向设备交付了至少
    /// 1 个字节**。
    ///
    /// 这是**按需驱动形态**的搬运路径：`try_write` 用 [`poll_once_`] 同步推一轮，
    /// `write_async` / `flush_async` / `close_async` 直接 await 它。自驱动形态的设备在
    /// 后台泵手里，本函数立即返回 `false`。
    ///
    /// # 为什么「有进展」的判据是「搬出量 > 0」
    ///
    /// [`SomeOf`] 的 `Both(0, err)` 也**含** `Left`：若按「含 `Left` 就算有进展」，
    /// 调用方的 `while pump_().await {}` 会永远转下去——段每次按已搬出量 0 提交、环里
    /// 数据一个不少，设备错误又没被记下，表现为进程 100% CPU 空转且真实错误（例如
    /// 对端退出后的 `EPIPE`）被吞掉。因此这里：**任何**带错误的结果都先记进 `err_`
    /// （`Both(n, err)` 也记），返回值只看搬出的字节数。
    async fn pump_(&mut self) -> bool {
        if self.err_.is_some() {
            return false;
        }
        // 自驱动形态：搬运不在本对象里。
        let Drive_::Manual { dev_, rx_ } = &mut self.drive_ else {
            return false;
        };
        let demand = Demand::at_least(1);
        let some = rx_.try_read(&demand);
        let Option::Some(mut segm) = some.pick_left() else {
            return false; // 环空
        };
        let moved = {
            let mut output = WriteAsOutput::new(&mut *dev_);
            segm.move_items_into_output_async(&mut output, &demand)
                .await
        };
        // 段在这里 drop：按已写出量 `advance_read`，释放环空间并唤醒等待的写端。
        drop(segm);
        match moved.into_inner() {
            // 正常交付。设备边界已把「0 字节且无错误」拦成错误，因此这里 `n` 正常
            // 都大于 0；真出现 0 也如实报「没有进展」。
            SomeLR::Left(n) => n > 0,
            // 一个字节都没写出、只有错误：记错误并收工。
            SomeLR::Right(err) => {
                self.err_ = Option::Some(take_io_err_(err));
                false
            }
            // 写出了一部分又出错：错误照记，但本次确实有进展。
            SomeLR::Both(n, err) => {
                self.err_ = Option::Some(take_io_err_(err));
                n > 0
            }
        }
    }

    /// 记一条「设备零进展」错误：环里还有数据，但一整轮搬运一个字节都没推出去、
    /// 设备也没有给出任何错误。
    ///
    /// 这种状态不会自愈（再试还是同样结果）。收尾循环（`flush_async` / `close_async`）
    /// 用它把「无进展」变成**终止性**错误，而不是继续重试空转 CPU。错误详情可经
    /// [`take_error`](Self::take_error) 取回。
    fn note_zero_progress_(&mut self) {
        self.err_.get_or_insert_with(|| {
            (
                io::Error::new(
                    io::ErrorKind::WriteZero,
                    "设备未接受任何字节且未报错（零进展）",
                ),
                WriteErrTag::Unknown,
            )
        });
    }
}

/// 环读半部的收尾守卫：**退出时关闭消费端**。
///
/// `RingReader` 被 drop **不**置位 `CONSUMER_CLOSED`（`buffex` 的关闭是显式动作），
/// 而泵退出后必须让写端从「环满而 park」变成「拿到 `Closing`」——否则上层会在没人
/// 消费的环上永久 park。因此泵把读半部交本守卫独占持有，退出（含提前 `return`）时
/// 统一关一次。
struct ReaderGuard_<S, B>(RingReader<S, B, u8>)
where
    S: Borrow<Ring<B, u8>>,
    B: BorrowMut<[MaybeUninit<u8>]>;

impl<S, B> Deref for ReaderGuard_<S, B>
where
    S: Borrow<Ring<B, u8>>,
    B: BorrowMut<[MaybeUninit<u8>]>,
{
    type Target = RingReader<S, B, u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S, B> DerefMut for ReaderGuard_<S, B>
where
    S: Borrow<Ring<B, u8>>,
    B: BorrowMut<[MaybeUninit<u8>]>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<S, B> Drop for ReaderGuard_<S, B>
where
    S: Borrow<Ring<B, u8>>,
    B: BorrowMut<[MaybeUninit<u8>]>,
{
    fn drop(&mut self) {
        self.0.close();
    }
}

/// 「环 → 设备」的后台泵（自驱动形态）：环空则 park，有数据就交给设备。
///
/// 返回 `Some(设备错误)` 表示设备写失败（或 `close` 失败）；`None` 表示生产端关闭、
/// 环已取空，随后已 `close` 设备。
async fn write_pump_<W, C, S>(
    dev: W,
    rx: RingReader<S, RingBufOf<C>, u8>,
) -> Option<(io::Error, WriteErrTag)>
where
    W: AsyncWrite + Unpin,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
{
    let mut dev = dev;
    let mut rx = ReaderGuard_(rx);
    loop {
        // 借读段：环空时 park，由生产者的 `advance_write` 唤醒；生产端关闭且环已排空时
        // 这里给 `Closing`，`pick_left()` 于是为 `None`，泵进入收尾。
        let demand = Demand::at_least(1);
        let some = rx.read_async(&demand).await;
        let Option::Some(mut segm) = some.pick_left() else {
            break;
        };
        // 段要握到设备写完再 drop：`advance_read` 在 drop 时发生，等待空间的写端据此被
        // 唤醒。「设备只收下一部分」不会丢字节——没写完的部分仍留在段里。
        let moved = {
            let mut output = WriteAsOutput::new(&mut dev);
            segm.move_items_into_output_async(&mut output, &Demand::at_least(1))
                .await
        };
        drop(segm);
        match moved.into_inner() {
            SomeLR::Left(_) => {}
            SomeLR::Right(err) => return Option::Some(take_io_err_(err)),
            SomeLR::Both(_n, err) => return Option::Some(take_io_err_(err)),
        }
    }
    // 正常收尾：关掉写方向，对端因此看到 EOF。
    let _ = poll_fn(|cx| Pin::new(&mut dev).poll_close(cx)).await;
    Option::None
}

/// 从设备带标签错误里取出 `std::io::Error` 与标签。
///
/// 设备适配器（`WriteAsOutput`）把 IO 错误包成 `TaggedError<io::Error, WriteErrTag>`；
/// `propagated_err()` 由 `abs_buff` 的 `TrErrorWrapper` 提供，能把内层错误原样取回。
fn take_io_err_(err: TaggedError<io::Error, WriteErrTag>) -> (io::Error, WriteErrTag) {
    let tag = err.tag();
    match TrErrorWrapper::propagated_err(err) {
        Result::Ok(e) => (e, tag),
        // 理论上设备错误都带 `Propagated` 标签；取不到内层错误就把整个标签错误
        // 包成 `io::Error`，保证信息不丢。
        Result::Err(other) => (io::Error::other(other), tag),
    }
}

impl<W, C, S> TrBuffTryWrite<u8> for BuffWrite<W, C, S>
where
    W: AsyncWrite + Unpin,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
{
    type SegmMut<'f>
        = RingSegmMut<'f, RingBufOf<C>, u8>
    where
        Self: 'f;

    type Err = ProducerError<usize>;

    #[inline]
    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        BuffWrite::try_write(self, demand)
    }
}

impl<W, C, S> TrBuffWrite<u8> for BuffWrite<W, C, S>
where
    W: AsyncWrite + Unpin,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
{
    type WriteAsync<'f>
        = BuffWriteOpAsync<'f, 'f, W, C, S>
    where
        Self: 'f;

    #[inline]
    fn write_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::WriteAsync<'f> {
        BuffWrite::write_async(self, demand)
    }
}

impl<W, C, S> TrProducerState for BuffWrite<W, C, S>
where
    W: AsyncWrite + Unpin,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
{
    fn producer_state(&self) -> Option<(usize, bool)> {
        self.tx_.producer_state()
    }
}


/// `write_async` 的循环。
///
/// * **按需驱动**：冲一轮 → 尝试借段 → 环满则 park 等读端消费；
/// * **自驱动**：直接借段 / park（搬运在后台泵里）。
#[abs_buff::gen_may_cancel_future(BuffWriteOp, pub, new(pub(crate)))]
#[allow(clippy::needless_lifetimes)]
async fn buff_write_async_<'f, W, C, S, K>(
    me: &'f mut BuffWrite<W, C, S>,
    demand: &'f Demand<usize>,
    cancel: K,
) -> SomeOf<RingSegmMut<'f, RingBufOf<C>, u8>, ProducerError<usize>>
where
    W: AsyncWrite + Unpin,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
    K: TrCancellationToken,
{
    if matches!(&me.drive_, Drive_::Local { .. }) {
        return me
            .tx_
            .write_async(demand)
            .may_cancel_with(cancel.child_token())
            .await;
    }
    loop {
        // 借段前先冲一轮：调用方持续借段时，设备才不会缺数据。
        me.pump_().await;
        if me.err_.is_some() {
            return SomeOf::new_right(ProducerError::Closing);
        }
        let some = me.tx_.try_write(demand);
        match some.as_ref().pick_right() {
            // 环满：等读端消费后唤醒。
            Option::Some(ProducerError::Stuffed(_)) => {}
            _ => return some,
        }
        drop(some);
        let ring_some = me
            .tx_
            .write_async(demand)
            .may_cancel_with(cancel.child_token())
            .await;
        match ring_some.as_ref().pick_right() {
            Option::Some(ProducerError::Stuffed(_)) => continue,
            Option::Some(ProducerError::Cancelled) => {
                return SomeOf::new_right(ProducerError::Cancelled);
            }
            _ => {}
        }
        return ring_some;
    }
}

/// `flush_async` 的循环：直到环被取空。
///
/// * **按需驱动**：反复 `pump_`（每轮把一个读段交给设备）；
/// * **自驱动**：借段的下限取**满容量**，只有环已空才借得到。
#[abs_buff::gen_may_cancel_future(BuffWriteFlushOp, pub, new(pub(crate)))]
#[allow(clippy::needless_lifetimes)]
async fn buff_write_flush_async_<'f, W, C, S, K>(
    buf_write: &'f mut BuffWrite<W, C, S>,
    cancel: K,
) -> Result<(), ProducerError<usize>>
where
    W: AsyncWrite + Unpin,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
    K: TrCancellationToken,
{
    if matches!(&buf_write.drive_, Drive_::Local { .. }) {
        return wait_drained_(buf_write, cancel).await;
    }
    loop {
        if cancel.is_cancelled() {
            return Result::Err(ProducerError::Cancelled);
        }
        // 反复推：`pump_` 每轮把一个读段交给设备（段按已写出量提交）。
        // 同时记下「本轮是否真的送出了字节」，供下面的零进展兜底判断使用。
        let mut progressed = false;
        while buf_write.pump_().await {
            progressed = true;
        }
        if buf_write.err_.is_some() {
            return Result::Err(ProducerError::Closing);
        }
        if buf_write.ring_.borrow().data_size() == 0 {
            return Result::Ok(());
        }
        // 环里还有数据，但这一整轮一个字节都没送出去、设备也没报错：再循环只会
        // 空转（100% CPU）。记一条零进展错误后终止，把控制权还给调用方。
        if !progressed {
            buf_write.note_zero_progress_();
            return Result::Err(ProducerError::Closing);
        }
    }
}

/// 自驱动形态的「等环被取空」：借段下限取满容量，只有 `data_size == 0` 时才借得到。
///
/// 等待本身复用环的生产端等待槽（由后台泵的 `advance_read` 唤醒），不需要自造原语。
async fn wait_drained_<W, C, S, K>(
    buf_write: &mut BuffWrite<W, C, S>,
    cancel: K,
) -> Result<(), ProducerError<usize>>
where
    W: AsyncWrite + Unpin,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
    K: TrCancellationToken,
{
    let cap = buf_write.ring_.borrow().capacity();
    let demand = Demand::at_least(cap);
    loop {
        if cancel.is_cancelled() {
            return Result::Err(ProducerError::Cancelled);
        }
        // 先试一次同步借段：环已空就直接成功。
        match buf_write.tx_.try_write(&demand).into_inner() {
            SomeLR::Left(segm) => {
                // 提交 0 字节：只是把「借到的那一刻」当作冲刷完成。
                drop(segm);
                return Result::Ok(());
            }
            SomeLR::Right(ProducerError::Stuffed(_)) => {}
            SomeLR::Right(err) => return Result::Err(err),
            SomeLR::Both(_, err) => return Result::Err(err),
        }
        let some = buf_write
            .tx_
            .write_async(&demand)
            .may_cancel_with(cancel.child_token())
            .await;
        match some.into_inner() {
            SomeLR::Left(segm) => {
                drop(segm);
                return Result::Ok(());
            }
            SomeLR::Right(ProducerError::Stuffed(_)) => continue,
            SomeLR::Right(err) => return Result::Err(err),
            SomeLR::Both(_, err) => return Result::Err(err),
        }
    }
}

/// `close_async`：冲净环内数据后 `close` 设备。
///
/// * **按需驱动**：自己把环推空，再 `close`；
/// * **自驱动**：关生产端 → 等后台泵把剩余数据交给设备、`close` 之后退出。
#[abs_buff::gen_may_cancel_future(BuffWriteCloseOp, pub, new(pub(crate)))]
#[allow(clippy::needless_lifetimes)]
async fn buff_write_close_async_<'f, W, C, S, K>(
    buf_write: &'f mut BuffWrite<W, C, S>,
    _cancel: K,
) -> Result<(), ProducerError<usize>>
where
    W: AsyncWrite + Unpin,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
    K: TrCancellationToken,
{
    if matches!(&buf_write.drive_, Drive_::Local { .. }) {
        // ① 关生产端：泵据此把环里剩余数据交给设备、`close` 之后自行退出。
        buf_write.tx_.close();
        // ② 取出泵句柄（`JoinHandle` 只能取一次），等它退出并接回设备错误。
        let pump = match &mut buf_write.drive_ {
            Drive_::Local { pump_ } => pump_.take(),
            Drive_::Manual { .. } => Option::None,
        };
        if let Option::Some(pump) = pump {
            match pump.await {
                // smol 的任务不会失败，因此 `Err` 分支不可达（保留以对齐三后端形状）。
                Result::Ok(Option::Some(err)) => {
                    buf_write.err_ = Option::Some(err);
                }
                Result::Ok(Option::None) => {}
                Result::Err(join_err) => {
                    buf_write.err_ = Option::Some((
                        io::Error::other(format!("后台写泵未能正常收尾：{join_err}")),
                        WriteErrTag::Unknown,
                    ));
                }
            }
        }
        return if buf_write.err_.is_some() {
            Result::Err(ProducerError::Closing)
        } else {
            Result::Ok(())
        };
    }
    // 环里可能还有数据，且设备可能背压：必须循环到环取空再 close。
    loop {
        let mut progressed = false;
        while buf_write.pump_().await {
            progressed = true;
        }
        if buf_write.err_.is_some() {
            break;
        }
        if buf_write.ring_.borrow().data_size() == 0 {
            break;
        }
        // 同 `flush_async`：零字节交付且无错误不会再自愈，记错误后终止。
        if !progressed {
            buf_write.note_zero_progress_();
            break;
        }
    }
    if buf_write.err_.is_none() {
        let Drive_::Manual { dev_, .. } = &mut buf_write.drive_ else {
            unreachable!("非自驱动形态的分支已在函数开头返回");
        };
        if let Result::Err(err) = poll_fn(|cx| Pin::new(&mut *dev_).poll_close(cx)).await {
            buf_write.err_ = Option::Some((err, WriteErrTag::Propagated));
        }
    }
    if buf_write.err_.is_some() {
        return Result::Err(ProducerError::Closing);
    }
    Result::Ok(())
}
