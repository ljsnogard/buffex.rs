//! 入向适配：把**任意** `tokio::io::AsyncRead` 设备变成
//! [`TrBuffRead`](abs_buff::TrBuffRead) / [`TrBuffTryRead`](abs_buff::TrBuffTryRead)。
//!
//! # 形态：按需驱动，不 spawn
//!
//! [`BuffRead`] 自己持有环（`mm_ptr::Shared<Ring<…>>`，经 `Ring::split_unchecked` 拆成两半）与
//! 设备：
//!
//! * 调用方经 [`TrBuffRead`](abs_buff::TrBuffRead) /
//!   [`TrBuffTryRead`](abs_buff::TrBuffTryRead) 消费环里的段；
//! * 「设备 → 环」的搬运由本适配器**按需推进**：`try_read` 同步推一轮（try-once，
//!   设备此刻没数据就返回 `Drained`），`read_async` 在等环里出现数据的过程中持续推进；
//!   设备暂时没数据时靠 `write_async` 挂起，不忙等。
//!
//! 不 spawn 任何任务，因此调用方不需要 `LocalSet` 之类的执行体上下文。之所以不能用
//! `tokio::spawn`（多线程）做后台泵：环的异步入口 future 无法被证明 `Send`
//! （`TrPark::ParkAsync` 是 GAT，GAT 投影不传递 auto trait，见 `buffex` 的
//! dev-notes §3）。按需驱动从根上绕开了这一约束。
//!
//! # 分配点（见 [`TrAllocConfig`]）
//!
//! 读向用到的分配点都从配置 `C` 里取：
//!
//! * 环的缓冲本体 `Box<[MaybeUninit<u8>], _>` ← `C::RingBodyAlloc`；
//! * 装环的 `mm_ptr::Shared<Ring<…>, _>` ← `C::RingSharedAlloc`。
//!
//! # 零拷贝：段直连设备
//!
//! 搬运**不经中转缓冲**：借到环的写段后，直接把段交给
//! `move_items_from_input_async(&mut input, &demand)`（`input` 是 [`EofInput`] 包装的设备）。
//!
//! 这里有一个契约细节：`abs_buff` 的输入搬移**不允许**「返回 0 个且无错误」（内层会
//! `assert!(cc > 0 || x.contains_right())`，提交 `e4092c8`——那正是它过去死循环的输入），
//! 而 tokio 的 `AsyncRead` 用「读到 0」表示 EOF。这一层翻译由设备适配器
//! [`ReadAsInput`] 负责：它在读到 0 时返回带 `Closing` 标签的错误。本适配器只需把
//! `Closing` 识别为「设备正常结束」（不记成设备错误）。
//!
//! # EOF 与错误
//!
//! `buffex::ring::Ring` 没有对外的 `close`，设备 EOF / IO 错误写不进环的状态字，因此
//! 本适配器自己记：
//!
//! * 设备 `read` 返回 0（tokio 的 EOF 约定）→ 置 `eof_`；
//! * 设备出错 → 记入 `err_` 并停止搬运，环内已搬入的数据仍可读；错误详情经
//!   [`BuffRead::take_error`] 取回；
//! * **环被取空**且设备已结束时，把 `ConsumerError::Drained` 合成为
//!   `ConsumerError::Closing`，调用方由此看到 EOF。
//!
//! # 消费语义
//!
//! 本适配器把环自己的读段原样借给调用方，因此沿用 `buffex` 的语义：
//! [`iter_slices`](abs_buff::buffer::TrBuffSegmView::iter_slices) 只是**窥视**，只有
//! `move_items_to_buff` / `move_items_into_output_async` 会推进消费，段 drop 时按已
//! 搬出量提交（`advance_read`）。

use core::{borrow::Borrow, io};

use tokio::io::AsyncRead;

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead,
    buffer::{TrBuffSegmMut, TrConsumerState},
    error::{ReadErrTag, TaggedError, TrErrorWrapper},
    x_deps::{anylr, abs_cancel},
};
use abs_buff_tokio_adapt::{
    ReadAsInput,
    x_deps::tokio,
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use anylr::{SomeOf, some_of::SomeLR};
use buffex::{
    ring::{ConsumerError, Ring, RingReader, RingSegmRef, RingWriter},
    x_deps::abs_buff,
};

use crate::{
    alloc_::{TrAllocConfig, DefaultAllocConfig},
    shared_::{
        CoreOf, InvalidCapacity, RingBufOf, SharedRingOf, poll_once_, split_ring_,
    },
};

/// 入向适配器的读半部类型（容器与缓冲本体由配置 `C` 决定）。
pub type ReaderOf<C, S> = RingReader<S, RingBufOf<C>, u8>;

/// 入向适配器的写半部类型（内部泵持有）。
pub type WriterOf<C, S> = RingWriter<S, RingBufOf<C>, u8>;

/// 把**任意** `tokio::io::AsyncRead` 设备接上 `buffex` 环的读端适配器。
///
/// 实现 [`TrBuffTryRead`](abs_buff::TrBuffTryRead) 与
/// [`TrBuffRead`](abs_buff::TrBuffRead)；设备数据由内部泵在读取时按需搬进环。
///
/// # 类型参数
///
/// * `C`——分配器配置（默认 [`DefaultAllocConfig`]：各分配点都用 `Global`）；
/// * `S`——环的共享容器（默认 `mm_ptr::Shared`，由配置选分配器）。
///
/// # Examples
///
/// ```no_run
/// #![feature(allocator_api)]
/// use buffex_tokio_adapt::{BuffRead, DefaultAllocConfig};
///
/// # async fn demo(dev: tokio::io::Empty) {
/// let mut r = BuffRead::<_, DefaultAllocConfig>::try_new(
///     dev,
///     4096,
///     DefaultAllocConfig,
/// )
/// .expect("容量须在 ring 允许范围内");
/// // 此后按 TrBuffRead / TrBuffTryRead 消费；设备错误经 take_error 取回。
/// let _ = r.take_error();
/// # }
/// ```
#[derive(Debug)]
pub struct BuffRead<R, C = DefaultAllocConfig, S = SharedRingOf<C>>
where
    R: AsyncRead + Unpin,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
{
    core_: CoreOf<R, C, S>,
}

// 适配器自身不依赖固定地址：`Ring` 虽含 `PhantomPinned`，但它由 `Shared` 分配、地址稳定，
// 代码里从不把 `Ring` 按值搬出堆指针。
impl<R, C, S> Unpin for BuffRead<R, C, S>
where
    R: AsyncRead + Unpin,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
{}

impl<R, C> BuffRead<R, C, SharedRingOf<C>>
where
    R: AsyncRead + Unpin,
    C: TrAllocConfig,
{
    /// 用「设备 + 环容量 + 分配器配置」构造读端适配器（容器 `Shared`、缓冲本体
    /// [`RingBufOf<C>`]）。
    ///
    /// # Errors
    ///
    /// `cap` 不在 ring 允许的容量区间内时返回 [`InvalidCapacity`]。
    pub fn try_new(dev: R, cap: usize, alloc_cfg: C) -> Result<Self, InvalidCapacity> {
        let ring = crate::shared_::alloc_ring_(cap, &alloc_cfg)?;
        let (tx, rx) = split_ring_(ring.clone());
        Ok(BuffRead {
            core_: CoreOf::new_(dev, ring, rx, tx),
        })
    }
}

impl<R, C, S> BuffRead<R, C, S>
where
    R: AsyncRead + Unpin,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
{
    /// 取回最近一次设备的读错误及其标签（取走即清空）。
    ///
    /// 设备错误不会打断「把环里已有数据读完」：搬运停止、错误暂存，调用方读完环内
    /// 数据后看到 `Closing`，再经本方法取回具体原因。
    pub fn take_error(&mut self) -> Option<(io::Error, abs_buff::error::ReadErrTag)> {
        self.core_.err_.take()
    }

    /// 借出可读段（try-once）。
    ///
    /// 语义同 [`TrBuffTryRead::try_read`](abs_buff::TrBuffTryRead::try_read)：先同步
    /// 推一轮搬运（设备此刻没数据就算了），再尝试借出环内已有的段；环空返回
    /// `Drained`，设备已结束且环已取空时返回 `Closing`。
    pub fn try_read<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<RingSegmRef<'f, RingBufOf<C>, u8>, ConsumerError<usize>> {
        // 同步推一轮：`poll_once_` 只 poll 一次，设备此刻没数据（Pending）就放弃本轮。
        let _ = poll_once_(self.pump_());
        // 先取「设备是否已结束」，避免在已借出读段之后再借 `self`。
        let ended = self.core_.eof_ || self.core_.err_.is_some();
        let some = self.core_.rx_.try_read(demand);
        if ended
            && matches!(
                some.as_ref().pick_right(),
                Option::Some(ConsumerError::Drained(_))
            )
        {
            return SomeOf::new_right(ConsumerError::Closing);
        }
        some
    }

    /// 借出可读段（异步）：在等环里出现数据的过程中持续推进设备 → 环的搬运。
    ///
    /// 调用方可经 [`may_cancel_with`](abs_cancel::TrMayCancel::may_cancel_with) 装入
    /// 取消信号。
    pub fn read_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> BuffReadOpAsync<'f, 'f, R, C, S> {
        BuffReadOpAsync::new(self, demand)
    }

    /// 记录「设备结束 / 出错」：`Closing` 标签只是设备正常结束（置 EOF 标志），
    /// 其余标签才算设备错误（记入错误槽，供 `take_error` 取回）。
    fn record_end_(&mut self, err: TaggedError<io::Error, ReadErrTag>) {
        let eof = err.tag() == ReadErrTag::Closing;
        self.core_.eof_ = eof;
        if !eof {
            self.core_.err_ = Some(take_io_err_(err));
        }
    }

    /// 推一轮「设备 → 环」的搬运（**不 park**），返回搬入的字节数。
    ///
    /// 零拷贝：反复借环的写段、直接把段交给 `move_items_from_input_async`，设备读到多少
    /// 就写进段里多少，drop 时按已写入量提交给环。
    ///
    /// # 「有数据就交付」：为什么每轮只 poll 一次搬移
    ///
    /// `move_items_from_input_async` 的语义是**填满**段（`Demand::no_more_than(room)`
    /// ⇒ 上限就是段长）：设备给出 25 字节后转为空闲时它只是 `Pending`——数据被扣在段里、
    /// 一个字节也不提交。对「请求 / 应答」式上层（握手帧、乒乓消息）来说这等于永久静止：
    /// 对端在等我们应答，而我们还在等它把段填满。上层协议栈只会把本适配器当
    /// `TrBuffRead` 用，没有「喂满一段」的义务，因此这一步必须在适配器里做。
    ///
    /// 只 poll 一次不会 park（`poll_fn` 立刻给出 `Ready`）；丢开那个未完成的搬移时，
    /// 已经读进段内的字节已经推进了段内偏移，drop 段即提交，没读到的部分不计入——
    /// 既不丢数据，也不吞掉设备就绪通知。
    ///
    /// 三种结束情形：
    ///
    /// * 设备此刻没有数据（本轮一个字节都没搬入）→ 返回本轮搬入量（可能为 0）；
    /// * 环满 → 返回（等调用方消费）；
    /// * 设备结束（`Closing`）或出错 → 记状态后返回（`Closing` 只置 EOF 标志，不算错误）。
    ///
    /// 设备读到 0 会被设备适配器翻成带 `Closing` 的 `Right`；`abs_buff` 不允许裸的
    /// `Left(0)`，因此 `Left(n)` 分支里 `n` 必定大于 0。
    async fn pump_(&mut self) -> usize {
        let mut moved_any = 0usize;
        loop {
            if self.core_.eof_ || self.core_.err_.is_some() {
                return moved_any;
            }
            let demand = Demand::at_least(1);
            let some = self.core_.tx_.try_write(&demand);
            let Some(mut segm) = some.pick_left() else {
                return moved_any; // 环满：等调用方消费
            };
            let before = segm.least_count();
            if before == 0 {
                return moved_any;
            }
            {
                let move_demand = Demand::no_more_than(before);
                let mut input = ReadAsInput::new(&mut self.core_.dev_);
                let mut moved_fut = core::pin::pin!(core::future::IntoFuture::into_future(
                    segm.move_items_from_input_async(&mut input, &move_demand),
                ));
                core::future::poll_fn(|cx| {
                    let _ = core::future::Future::poll(moved_fut.as_mut(), cx);
                    core::task::Poll::Ready(())
                })
                .await;
            }
            let left = segm.least_count();
            // 段在这里 drop：按已写入量 `advance_write`，唤醒等待的读端。
            drop(segm);
            if left < before {
                // 有进展：设备可能还有，回顶部再借段搬一轮。
                moved_any += before - left;
                continue;
            }
            // 设备此刻没数据：本轮到此为止（由调用方决定是等设备还是先交付环里的数据）。
            return moved_any;
        }
    }

    /// **等**设备给出至少 1 个字节，并把它提交进环；返回「这次等待是否等到了数据」。
    ///
    /// 这是 [`BuffRead::read_async`] 在「环空且设备还没结束」时的 park 点：借一个环写段，
    /// 用**恰好 1 字节**的搬移去等设备——这一次等待本身就是一次提交，数据不会被扣在段里。
    /// 等到的那个字节随即由调用方经环取走，因此它同时也是性能上的一个折衷：每次「设备由
    /// 空转有」的边界只提交 1 字节，随后的 `pump_` 会把这一批的余量整段搬走。
    async fn wait_input_<K>(&mut self, cancel: K) -> bool
    where
        K: TrCancellationToken,
    {
        if self.core_.eof_ || self.core_.err_.is_some() {
            return false;
        }
        let demand = Demand::at_least(1);
        let some = self.core_.tx_.write_async(&demand).await;
        let Some(mut segm) = some.pick_left() else {
            return false; // 环被拆掉
        };
        let one = Demand::exactly(1usize);
        let moved = {
            let mut input = ReadAsInput::new(&mut self.core_.dev_);
            segm.move_items_from_input_async(&mut input, &one)
                .may_cancel_with(cancel.child_token())
                .await
        };
        // 段在这里 drop：按已写入量 `advance_write`，唤醒等待的读端。
        drop(segm);
        match moved.into_inner() {
            // 设备适配层保证「读到 0」会翻成 `Right`，所以这里 `n > 0`。
            SomeLR::Left(n) => {
                debug_assert!(n > 0, "无错误的搬运必须至少搬入 1 个元素");
                true
            }
            // 只带错误、没搬进东西：记错误后交回调用方（它会先交付环里的余量）。
            SomeLR::Right(err) => {
                self.record_end_(err);
                false
            }
            // 搬了一部分又结束：这部分已随段提交进环，先让调用方取走。
            SomeLR::Both(n, err) => {
                self.record_end_(err);
                n > 0
            }
        }
    }
}

/// 从设备带标签错误里取出 `std::io::Error` 与标签。
///
/// 设备适配器（`ReadAsInput`）把 IO 错误包成 `TaggedError<io::Error, ReadErrTag>`；
/// `propagated_err()` 由 `abs_buff` 的 `TrErrorWrapper` 提供，能把内层错误原样取回。
fn take_io_err_(
    err: abs_buff_tokio_adapt::x_deps::abs_buff::error::TaggedError<
        io::Error,
        abs_buff::error::ReadErrTag,
    >,
) -> (io::Error, abs_buff::error::ReadErrTag) {
    let tag = err.tag();
    match TrErrorWrapper::propagated_err(err) {
        Result::Ok(e) => (e, tag),
        // 理论上设备错误都带 `Propagated` 标签；取不到内层错误就把整个标签错误
        // 包成 `io::Error`，保证信息不丢。
        Result::Err(other) => (io::Error::other(other), tag),
    }
}

impl<R, C, S> TrBuffTryRead<u8> for BuffRead<R, C, S>
where
    R: AsyncRead + Unpin,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
{
    type SegmRef<'f>
        = RingSegmRef<'f, RingBufOf<C>, u8>
    where
        Self: 'f;

    type Err = ConsumerError<usize>;

    #[inline]
    fn try_read<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        BuffRead::try_read(self, demand)
    }
}

impl<R, C, S> TrBuffRead<u8> for BuffRead<R, C, S>
where
    R: AsyncRead + Unpin,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
{
    type ReadAsync<'f>
        = BuffReadOpAsync<'f, 'f, R, C, S>
    where
        Self: 'f;

    #[inline]
    fn read_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> Self::ReadAsync<'f> {
        BuffRead::read_async(self, demand)
    }
}

impl<R, C, S> TrConsumerState for BuffRead<R, C, S>
where
    R: AsyncRead + Unpin,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
{
    fn consumer_state(&self) -> Option<(usize, bool)> {
        self.core_.rx_.consumer_state()
    }
}

/// `read_async` 的循环：推一轮搬运 → 尝试借段 → 环空则 park 等生产端提交。
#[abs_buff::gen_may_cancel_future(BuffReadOp, pub, new(pub(crate)))]
#[allow(clippy::needless_lifetimes)]
async fn buff_read_async_<'f, R, C, S, K>(
    me: &'f mut BuffRead<R, C, S>,
    demand: &'f Demand<usize>,
    cancel: K,
) -> SomeOf<RingSegmRef<'f, RingBufOf<C>, u8>, ConsumerError<usize>>
where
    R: AsyncRead + Unpin,
    C: TrAllocConfig,
    S: Borrow<Ring<RingBufOf<C>, u8>>,
    K: TrCancellationToken,
{
    loop {
        me.pump_().await;
        // 先取「设备是否已结束」，避免在已借出读段之后再借 `me`。
        let ended = me.core_.eof_ || me.core_.err_.is_some();
        let some = me.core_.rx_.try_read(demand);
        let drained = matches!(
            some.as_ref().pick_right(),
            Option::Some(ConsumerError::Drained(_))
        );
        // 「临时没数据」才继续等；`Closing` / `Cancelled` 是对端终态，
        // `Unsatisfiable` 是本轮请求不可能满足，都直接交给调用方。
        if !drained {
            return some;
        }
        // 释放对读半部的借用（`some` 的 `'f` 绑定在这里结束），才能再借一次。
        drop(some);
        if ended && me.core_.ring_.borrow().data_size() == 0 {
            return SomeOf::new_right(ConsumerError::Closing);
        }
        if cancel.is_cancelled() {
            return SomeOf::new_right(ConsumerError::Cancelled);
        }
        // 环空、设备未结束：park 在**设备**上等它的下一个字节（这一次等待本身就是一次
        // 提交，见 `wait_input_`）。`pump_` 不再自 park，正是为了「已提交的数据先交付
        // 给调用方」——否则调用方会被挡在 pump 后面，看不到环里已有的字节。
        let _ = me.wait_input_(cancel.child_token()).await;
        // 无论等到没有，都回顶部重试：等到 → 借段交付；设备结束/出错 → `ended` 生效，
        // 先把环里的余量交给调用方，再给终态。
    }
}
