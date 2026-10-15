use core::{
    borrow::{Borrow, BorrowMut},
    cell::UnsafeCell,
    marker::{PhantomData, PhantomPinned},
    mem::MaybeUninit,
    slice,
    sync::atomic::AtomicUsize,
};

use abs_buff::{
    Demand,
    buffer::{TrConsumerState, TrProducerState},
    error::{IoErrTag, TrErrTag, TrTaggedError},
    gen_may_cancel_future,
    x_deps::{abs_cancel, anylr},
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use anylr::{SomeOf, TrSomeOf};
use atomex::AtomicFlags;
use atomic_sync::x_deps::atomex;

use super::{
    error_::{ConsumerError, ProducerError},
    half_::{Consumer, Producer},
    hook_::TrPark,
    reclaim::{Reclaim, ReclSliceMut, ReclSliceRef, SegmSlicesMut, SegmSlicesRef},
};


#[derive(Debug)]
#[repr(C)]
pub struct Ring<B, T = u8>
where
    B: BorrowMut<[MaybeUninit<T>]>,
{
    /// `rp`（低 `POS_BITS` 位）| `wp`（次 `POS_BITS` 位）| 全部标志（高位：
    /// 关闭 ×2、待机 ×2、泵互斥 ×1、待办泵 ×2）。
    buf_stat_: RingState,
    buf_cell_: UnsafeCell<B>,

    /// 环形缓冲（拥有）：统一 `[MaybeUninit<T>]` 视图。基址经裸指针访问，
    /// `Owned` 只负责所有权与生命周期。
    producer_: UnsafeCell<Producer<T>>,
    consumer_: UnsafeCell<Consumer<T>>,
    _unuse_t_: PhantomData<fn() -> T>,
    _pinning_: PhantomPinned,
}

impl<B, T> Ring<B, T>
where
    B: BorrowMut<[MaybeUninit<T>]>,
{
    pub fn check_buffer_size(buff: &B) -> Result<usize, usize> {
        let capacity = buff.borrow().len();
        if !(MIN_CAPACITY..=MAX_CAPACITY).contains(&capacity) {
            Result::Err(capacity)
        } else {
            Result::Ok(capacity)
        }
    }

    pub fn new_unchecked(buffer: B) -> Self {
        debug_assert!(Self::check_buffer_size(&buffer).is_ok());
        let capacity = buffer.borrow().len();
        Ring {
            buf_stat_: RingState::new(capacity),
            buf_cell_: UnsafeCell::new(buffer),
            producer_: UnsafeCell::new(Producer::new()),
            consumer_: UnsafeCell::new(Consumer::new()),
            _unuse_t_: PhantomData,
            _pinning_: PhantomPinned,
        }
    }

    pub fn try_new(buffer: B) -> Result<Self, usize> {
        let chk = Self::check_buffer_size(&buffer);
        if let Result::Err(cap) = chk {
            return Result::Err(cap);
        }
        Result::Ok(Self::new_unchecked(buffer))
    }

    #[allow(clippy::type_complexity)]
    pub fn split<'f>(ring: &'f mut Self) -> (
        RingWriter<&'f Self, B, T>,
        RingReader<&'f Self, B, T>,
    ) {
        let ring: &'f Self = ring;
        let w = RingWriter::new_(ring);
        let r = RingReader::new_(ring);
        (w, r)
    }

    /// Split ring into a pair of reader and writer. The ring is stored via a
    /// smart pointer, like `Arc` and `Rc`.
    ///
    /// # Safety
    /// - It is the caller's responsibility to guarantee that the ring is held
    ///   exclusively by the smart pointer.
    /// - It is the caller's responsibility to guarantee that no any upgrade
    ///   from a weak pointer to the smart pointer is possible.
    pub unsafe fn split_unchecked<S>(ring: S) -> (
        RingWriter<S, B, T>,
        RingReader<S, B, T>,
    ) where
        S: Borrow<Self> + Clone,
    {
        let w = RingWriter::new_(ring.clone());
        let r = RingReader::new_(ring);
        (w, r)
    }

    // ------------------------------------------------------------------
    // 状态查询
    // ------------------------------------------------------------------

    /// The unchanged capacity
    #[inline]
    pub const fn capacity(&self) -> usize {
        self.buf_stat_.capacity()
    }

    /// 当前可读数据量。
    #[inline]
    pub fn data_size(&self) -> usize {
        self.buf_stat_.data_size()
    }

    /// 当前可写空间量。
    #[inline]
    pub fn free_size(&self) -> usize {
        self.buf_stat_.free_size()
    }

    #[inline]
    pub fn is_producer_closed(&self) -> bool {
        self.buf_stat_.is_producer_closed()
    }

    #[inline]
    pub fn is_consumer_closed(&self) -> bool {
        self.buf_stat_.is_consumer_closed()
    }

    /// 关闭生产端（EOF）：此后不再有新数据，并唤醒**已经 park 的消费者**。
    ///
    /// 幂等：已关闭时直接返回（只有抢到置位的一方负责唤醒）。
    ///
    /// 关闭之后的读取语义：
    ///
    /// * 环里还有已提交数据 → 照常交付（即使不足 `Demand` 下限，见
    ///   [`Ring::try_read_internal_`] 的「EOF 例外」）；
    /// * 环已取空 → [`ConsumerError::Closing`]；
    /// * 正在 park 的消费者会被唤醒去重新判定，拿到 `Closing` 而不是永久挂起。
    ///
    /// 生产者一侧的对应入口是 [`RingWriter::close`]（语义相同）。
    ///
    /// 收 `&mut self`：关闭是**不可逆**的状态迁移，关闭权归环的独占持有者。半部里的
    /// [`RingWriter`] / [`RingReader`] 各代表环的一端，它们的 `close` 同样收 `&mut self`
    /// ——即「谁独占拥有那一端的半部，谁才有权关闭它」。
    #[inline]
    pub fn close_producer(&mut self) {
        self.close_producer_()
    }

    /// 关闭消费端：此后不再有消费者，并唤醒**已经 park 的生产者**。
    ///
    /// 幂等。关闭之后 `try_write` / `write_async` 一律返回
    /// [`ProducerError::Closing`]；正在 park 的生产者会被唤醒去重新判定。
    ///
    /// 消费者一侧的对应入口是 [`RingReader::close`]（语义相同）。
    ///
    /// 收 `&mut self`：理由同 [`Ring::close_producer`]。
    #[inline]
    pub fn close_consumer(&mut self) {
        self.close_consumer_()
    }

    // ------------------------------------------------------------------
    // 同步快速读写
    // ------------------------------------------------------------------

    #[inline]
    pub fn try_read<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<RingSegmRef<'f, B, T>, ConsumerError<usize>> {
        self.try_read_(demand)
    }

    #[inline]
    pub fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<RingSegmMut<'f, B, T>, ProducerError<usize>> {
        self.try_write_(demand)
    }
}

impl<B, T> Ring<B, T>
where
    B: BorrowMut<[MaybeUninit<T>]>,
{
    pub fn read_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> RingReadAsync<'f, 'f, B, T> {
        RingReadAsync::new(self, demand)
    }
}

impl<B, T> Ring<B, T>
where
    B: BorrowMut<[MaybeUninit<T>]>,
{
    pub fn write_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> RingWriteAsync<'f, 'f, B, T> {
        RingWriteAsync::new(self, demand)
    }
}

// -- ---- ---- ---- ---- ---- ---- ---- ----
// Ring internals
// -- ---- ---- ---- ---- ---- ---- ---- ----

impl<B, T> Ring<B, T>
where
    B: BorrowMut<[MaybeUninit<T>]>,
{
    // ------------------------------------------------------------------
    // 同步读写完整封装，仅允许内部调用
    // ------------------------------------------------------------------

    fn try_read_<'f>(
        &'f self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<ReclSliceRef<'f, T, Reclaim<'f, Self>>, ConsumerError<usize>> {
        self.try_read_internal_(demand)
            .map(|(start, take)| self.create_read_segm_(start, take))
            .into()
    }

    fn try_write_<'f>(
        &'f self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<ReclSliceMut<'f, T, Reclaim<'f, Self>>, ProducerError<usize>> {
        self.try_write_internal_(demand)
            .map(|(start, take)| self.create_write_segm_(start, take))
            .into()
    }

    /// 借出可读区，返回 `(start, take)`。
    ///
    /// 尊重 `Demand` 的 `[min, max]` 区间：可读数据不足下限且未关闭时**不返回**
    /// （返回 `Drained`）；**EOF 例外**——写端已关闭（不再会有更多数据）时，
    /// 返回现有部分（可能不足下限）；读端已关闭或缓冲区已空时返回 `Closing` /
    /// `Drained`。
    fn try_read_internal_(
        &self,
        demand: &Demand<usize>,
    ) -> Result<(usize, usize), ConsumerError<usize>> {
        let cap = self.capacity();
        let min_len = demand.min().unwrap_or(0);
        if min_len > cap {
            return Result::Err(ConsumerError::Unsatisfiable);
        }
        let s = self.buf_stat_.value();
        if has_flag(s, CONSUMER_CLOSED) {
            return Result::Err(ConsumerError::Closing);
        }
        let max_len = demand.max().unwrap_or(usize::MAX);
        let pos = IoPos::unpack(s, cap);
        let ready = pos.data_size();
        if ready == 0 {
            if has_flag(s, PRODUCER_CLOSED) {
                return Err(ConsumerError::Closing); // EOF：写端已关且读空
            }
            return Err(ConsumerError::Drained(pos.rp));
        }
        if ready < min_len {
            // EOF 例外：写端已关闭（不会再有数据）时交付现有部分，即使不足下限。
            // 少了这条，「已关闭 ⇒ can_consume_ 为真」会让 park 循环在一次 poll 内
            // 反复醒来又睡下（同步空转），而不是把 Closing 交给调用方。
            if !has_flag(s, PRODUCER_CLOSED) {
                return Err(ConsumerError::Drained(pos.rp)); // 不足下限且未关闭：等待更多
            }
        }
        let take = core::cmp::min(max_len, ready);
        debug_assert!(take > 0, "[Ring::try_read_internal_] take({})", take);
        Ok((pos.rp, take))
    }

    /// 借出可写区，返回 `(start, take)`。
    ///
    /// 尊重 `Demand` 的 `[min, max]` 区间：**可写空间不足下限时不返回**（返回
    /// `Stuffed`），满足时最多借出 `max`。区域可能跨末端环绕（由段类型表达）。
    fn try_write_internal_(
        &self,
        demand: &Demand<usize>,
    ) -> Result<(usize, usize), ProducerError<usize>> {
        let cap = self.capacity();
        let min_len = demand.min().unwrap_or(0);
        if min_len > cap {
            return Result::Err(ProducerError::Unsatisfiable);
        }
        let s = self.buf_stat_.value();
        if has_flag(s, PRODUCER_CLOSED) || has_flag(s, CONSUMER_CLOSED) {
            return Result::Err(ProducerError::Closing);
        }
        let max_len = demand.max().unwrap_or(usize::MAX);
        let pos = IoPos::unpack(s, cap);
        let free = pos.free_size();
        if free == 0 || free < min_len {
            return Result::Err(ProducerError::Stuffed(pos.wp));
        }
        let take = core::cmp::min(max_len, free);
        debug_assert!(take > 0, "[Ring::try_write_internal_] take({})", take);
        Ok((pos.wp, take))
    }

    // ------------------------------------------------------------------
    // 状态迁移（提交路径：推进位置 / 关闭，随后触发对端事件）
    // ------------------------------------------------------------------

    /// 写提交：按已消费量推进写位置，触发消费端事件。
    fn advance_write(&self, amount: usize) -> usize {
        let cap = self.capacity();
        let s = self.update_pos_(|s| {
            let pos = IoPos::unpack(s, cap);
            pos.advance_wp(amount).pack(s)
        });
        let pos = IoPos::unpack(s, cap);
        if has_flag(s, CONSUMER_STNDBY) {
            let consumer = unsafe { self.consumer_.as_ref_unchecked() };
            consumer.wake(&self.buf_stat_);
        }
        pos.free_size()
    }

    /// 读提交：按已消费量推进读位置，触发生产端事件。
    fn advance_read(&self, amount: usize) -> usize {
        let cap = self.capacity();
        let s = self.update_pos_(|s| {
            let pos = IoPos::unpack(s, cap);
            pos.advance_rp(amount).pack(s)
        });
        let pos = IoPos::unpack(s, cap);
        if has_flag(s, PRODUCER_STNDBY) {
            let producer = unsafe { self.producer_.as_ref_unchecked() };
            producer.wake(&self.buf_stat_);
        }
        pos.data_size()
    }

    /// 关闭生产端：置 `PRODUCER_CLOSED`，并唤醒等待中的消费者。
    ///
    /// 唤醒走的是与 [`Ring::advance_write`] 完全相同的一套协议：先看兴趣位快照，
    /// 再让 [`Consumer::wake`] 在锁内做「判定 → 认领 → 取走」，唤醒放在锁外。
    /// 判定面之所以能通过，是因为 [`RingState::can_consume_`] 与 `Consumer::wake`
    /// 都把「生产端已关闭」视作可唤醒条件（关闭即「条件永久不可满足」）。
    fn close_producer_(&self) {
        // 只有抢到「置位」的一方负责唤醒；重复 close 直接返回。
        if !self.buf_stat_.try_set_producer_closed_() {
            return;
        }
        if has_flag(self.buf_stat_.value(), CONSUMER_STNDBY) {
            let consumer = unsafe { self.consumer_.as_ref_unchecked() };
            consumer.wake(&self.buf_stat_);
        }
    }

    /// 关闭消费端：置 `CONSUMER_CLOSED`，并唤醒等待中的生产者。见 [`Ring::close_producer_`]。
    fn close_consumer_(&self) {
        if !self.buf_stat_.try_set_consumer_closed_() {
            return;
        }
        if has_flag(self.buf_stat_.value(), PRODUCER_STNDBY) {
            let producer = unsafe { self.producer_.as_ref_unchecked() };
            producer.wake(&self.buf_stat_);
        }
    }

    fn update_pos_<F>(&self, f: F) -> usize
    where
        F: Fn(usize) -> usize,
    {
        let expect = |_| true;
        let desire = f;
        self.buf_stat_
            .atm_flag_
            .try_spin_compare_exchange_weak(expect, desire)
            .into_inner()
    }

    // ------------------------------------------------------------------
    // 段构建（提交目标：本核心，经 TrCircBuffCore）
    // ------------------------------------------------------------------

    /// 构建写段：覆盖 `[start, start+take)`，跨末端时拆成两段物理空间。
    ///
    /// 段 drop 时经 [`WriterReclaim`] 提交回本核心（推进写位置并触发消费端
    /// 事件）。
    fn create_write_segm_<'s>(
        &'s self,
        start: usize,
        take: usize,
    ) -> ReclSliceMut<'s, T, Reclaim<'s, Self>> {
        // SAFETY: `start`/`take` 来自 `try_write_at`，区域在缓冲内；可写区与
        // 其他活段 / 泵操作不重叠是调用者义务（SPSC）。
        let whole: &'s mut [MaybeUninit<T>] = unsafe {
            let buff = &mut *self.buf_cell_.get();
            buff.borrow_mut()
        };
        let first = core::cmp::min(take, self.capacity() - start);
        let pieces = if first < take {
            let (head, tail) = whole.split_at_mut(start);
            let b = &mut head[..take - first];
            SegmSlicesMut::Two(tail, b)
        } else {
            SegmSlicesMut::One(&mut whole[start..start + take])
        };
        let reclaim = Reclaim::new(self, Self::advance_write);
        ReclSliceMut::new(pieces, reclaim)
    }

    /// 构建读段：覆盖 `[start, start+take)`，跨末端时拆成两段物理空间。
    ///
    /// 段 drop 时经 [`ReaderReclaim`] 提交回本核心（推进读位置并触发生产端
    /// 事件）。
    fn create_read_segm_<'s>(
        &'s self,
        start: usize,
        take: usize,
    ) -> ReclSliceRef<'s, T, Reclaim<'s, Self>> {
        // SAFETY: 同 [`CircCore::write_segm`]。
        let base = unsafe {
            let buff = self.buf_cell_.as_ref_unchecked();
            buff.borrow().as_ptr().cast::<T>()
        };
        let first = core::cmp::min(take, self.capacity() - start);
        let pieces = if first < take {
            let a = unsafe { slice::from_raw_parts(base.add(start), first) };
            let b = unsafe { slice::from_raw_parts(base, take - first) };
            SegmSlicesRef::Two(a, b)
        } else {
            let a = unsafe { slice::from_raw_parts(base.add(start), take) };
            SegmSlicesRef::One(a)
        };
        let reclaim = Reclaim::new(self, Self::advance_read);
        ReclSliceRef::new(pieces, reclaim)
    }
}

// -- ---- ---- ---- ---- ---- ---- ---- ----
// Ring TrBuffRead TrBuffWrite
// -- ---- ---- ---- ---- ---- ---- ---- ----

impl<B, T> TrConsumerState for Ring<B, T>
where
    B: BorrowMut<[MaybeUninit<T>]>,
{
    fn consumer_state(&self) -> Option<(usize, bool)> {
        let size = self.data_size();
        let sign = self.is_producer_closed();
        Option::Some((size, sign))
    }
}

impl<B, T> TrProducerState for Ring<B, T>
where
    B: BorrowMut<[MaybeUninit<T>]>,
{
    fn producer_state(&self) -> Option<(usize, bool)> {
        let size = self.free_size();
        let sign = self.is_consumer_closed();
        Option::Some((size, sign))
    }
}

impl<B, T> abs_buff::TrBuffTryWrite<T> for Ring<B, T>
where
    B: BorrowMut<[MaybeUninit<T>]>,
{
    type SegmMut<'f> = ReclSliceMut<'f, T, Reclaim<'f, Self>> where Self: 'f;
    type Err = ProducerError<usize>;

    #[inline]
    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        Ring::try_write_(self, demand)
    }
}

impl<B, T> abs_buff::TrBuffWrite<T> for Ring<B, T>
where
    B: BorrowMut<[MaybeUninit<T>]>,
{
    type WriteAsync<'f> = RingWriteAsync<'f, 'f, B, T> where Self: 'f;

    #[inline]
    fn write_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> Self::WriteAsync<'f> {
        Ring::write_async(self, demand)
    }
}

// -- ---- ---- ---- ---- ---- ---- ---- ----
// Ring: Send Sync
// -- ---- ---- ---- ---- ---- ---- ---- ----

unsafe impl<B, T> Send for Ring<B, T>
where
    B: BorrowMut<[MaybeUninit<T>]>,
{}

unsafe impl<B, T> Sync for Ring<B, T>
where
    B: BorrowMut<[MaybeUninit<T>]>,
{}

// ---------------------------------------------------------------------------
// 状态字布局
// ---------------------------------------------------------------------------

/// 保留高8位作为状态字（关闭 ×2 + 待机 ×2 + REVERSION）。
const RSV_BITS: u32 = 8;

/// 生产者（写端）已关闭。
const PRODUCER_CLOSED: usize = 1usize << (usize::BITS - 1);
/// 消费者（读端）已关闭。
const CONSUMER_CLOSED: usize = 1usize << (usize::BITS - 2);
/// 生产端等待唤醒。被动模式下 demand 有值
const PRODUCER_STNDBY: usize = 1usize << (usize::BITS - 3);
/// 消费端等待唤醒。
const CONSUMER_STNDBY: usize = 1usize << (usize::BITS - 4);

/// 写入端已跨段标志，即此时 wp <= rp 是合法状态
pub(super) const REVERSION: usize = 1usize << (usize::BITS - 5);

/// 状态字全部标志的掩码（位置更新（`update_state`）保留这些位）。
#[allow(unused)]
pub(super) const FLAG_MASK: usize = PRODUCER_CLOSED
    | CONSUMER_CLOSED
    | PRODUCER_STNDBY
    | CONSUMER_STNDBY
    | REVERSION;
/// 每个位置占用的位数（两个位置共享低位，两个标志占高位）。
pub(super) const POS_BITS: u32 = (usize::BITS - RSV_BITS) / 2;
/// 位置掩码。
pub(super) const POS_MASK: usize = (1usize << POS_BITS) - 1;

/// 环形缓冲的最小容量。
///
/// **1 是下限**，而不是 2：当前位置编码用 `REVERSION` 位区分空 / 满（见 [`IoPos`]），
/// 容量**全部可用**，不再依赖「专门空出一格」来区分空与满，因此 1 格也能正确工作。
///
/// 容量 1 的语义边界（值得写下来，因为它没有绕行路径）：
///
/// * `rp` 与 `wp` 只可能为 0，`wp == rp` 恒成立，一切由 `REVERSION` 决定——
///   写入后置位（满，`data_size == 1`），读出后清除（空，`data_size == 0`）；
/// * **不存在实际的跨末端绕行**：`rp + amount >= capacity` 在 `amount == 1` 时即
///   越过末端，因此每次读写都会走「跨末端」分支（`create_*_segm_` 的
///   `first == take` 情形），借出的段永远是单片的 1 格。
///
/// 容量 0 仍然被拒：零长度缓冲上任何读写都不可满足，构造出来只会是个陷阱。
pub(super) const MIN_CAPACITY: usize = 1;
/// 环形缓冲的最大容量（与 `ring_buffer` 的 `MAX_CAPACITY` 同量级）。
pub(super) const MAX_CAPACITY: usize = POS_MASK;

/// 环形位置状态：读者位置 `rp` / 写者位置 `wp`（均为 `[0, capacity_)` 内的
/// 物理索引）+ 跨末端标志 `rv`（即状态字中的 [`REVERSION`] 位）。
///
/// # 位置约定（REVERSION 方案，不再使用「空一槽」）
///
/// 读写位置打包进状态字低位（`rp` 占低 `POS_BITS` 位、`wp` 占次 `POS_BITS`
/// 位），容量**全部可用**——不再刻意保留一个空槽。满 / 空由 `rv` 区分：
///
/// * `rv == false`（写者未跨过物理末端）：原始 `wp >= rp`，数据量 = `wp - rp`；
///   其中 `wp == rp` 表示**空**（data = 0）；
/// * `rv == true`（写者已跨过物理末端，原始 `wp <= rp`）：数据量 =
///   `wp + capacity - rp`；其中 `wp == rp` 表示**满**（整环都是数据，
///   data = capacity）。
///
/// `rv` 只随位置推进而置位 / 清除（其余标志位原样保留）：
///
/// * [`IoPos::advance_wp`]：写者越过物理末端（`wp + amount >= capacity`）时
///   置位，且置位后一直保持——写者始终「在读者之后（含追上成满环）」；
/// * [`IoPos::advance_rp`]：读者越过物理末端（`rp + amount >= capacity`）时
///   清除——读者跨过末端后，写者的原始位置重新位于读者之前，恢复未跨状态。
#[derive(Clone, Copy, Debug)]
pub struct IoPos {
    /// 与 REVERSION flag 含义一致：写者是否已越过缓冲区物理末端（此时原始
    /// `wp <= rp`；`wp == rp` 表示环满）。
    pub rv: bool,
    /// 读者位置（物理索引，`[0, capacity_)`）。
    pub rp: usize,
    /// 写者位置（物理索引，`[0, capacity_)`）。
    pub wp: usize,
    /// circular buff 的容量。
    capacity_: usize,
}

impl IoPos {
    /// 本类型在状态字中「拥有」的位：两个位置字段（`rp` 低 `POS_BITS` 位、
    /// `wp` 次 `POS_BITS` 位）与 REVERSION 位。`pack` 只覆盖这些位，其余
    /// 标志位由传入的基座状态字原样保留。
    pub const MASK: usize = REVERSION | POS_MASK | (POS_MASK << POS_BITS);

    /// 从 `atm_stat_` 的状态字解出位置与保留标志。
    pub fn unpack(state: usize, cap: usize) -> Self {
        let rp = state & POS_MASK;
        let wp = (state >> POS_BITS) & POS_MASK;
        let rv = has_flag(state, REVERSION);
        IoPos { rv, rp, wp, capacity_: cap }
    }

    /// 当前可读数据量（遵循上述约定：空环 = 0、满环 = 容量）。
    #[inline]
    pub fn data_size(&self) -> usize {
        if self.rv && self.wp == self.rp {
            // 写者跨过末端后恰好追上读者：整环都是数据（满）。
            self.capacity_
        } else {
            // 未跨（wp > rp）或已跨未满（wp < rp）：`(wp - rp) mod capacity`。
            (self.wp + self.capacity_ - self.rp) % self.capacity_
        }
    }

    /// 当前可写空间量：`capacity - data_size`（满环 = 0、空环 = 容量）。
    #[inline]
    pub fn free_size(&self) -> usize {
        self.capacity_ - self.data_size()
    }

    /// 以传入的基座状态字 `state` 打包回完整状态字：`state` 中本类型不拥有的
    /// 位（[`IoPos::MASK`] 之外——关闭、待机、待办泵等标志）**原样保留**；
    /// 本类型拥有的位（`rp` / `wp` 位置字段与 REVERSION 位）用自身的新值
    /// **覆盖**（先清除基座中的旧值再写入，而非按位或——否则旧位置会残留并
    /// 与新位置混合）。
    pub fn pack(&self, state: usize) -> usize {
        let s = (state & !Self::MASK) | self.rp | (self.wp << POS_BITS);
        if self.rv { s | REVERSION } else { s & !REVERSION }
    }

    /// 推进写者位置：`wp += amount`（物理上环绕）；**写者越过缓冲区物理末端
    /// （`wp + amount >= capacity`）时设置 REVERSION flag**，且一旦置位保持
    /// 到读者追上来为止。返回推进后的**新位置状态**（不含任何标志位）；需要
    /// 写回 `atm_stat_` 时以原状态字为基座调用 [`IoPos::pack`]。
    ///
    /// 前置：`amount <= free_size`（不允许写过头；恰好写满时进入
    /// `wp == rp && rv` 的满态）。
    pub fn advance_wp(&self, amount: usize) -> Self {
        debug_assert!(amount <= self.free_size());
        let new_wp = self.wp + amount;
        let crossed = new_wp >= self.capacity_;
        Self {
            // 越过末端置位；已置位则保持（写者仍在读者之后，直到读者追上）。
            rv: self.rv || crossed,
            rp: self.rp,
            wp: new_wp % self.capacity_,
            capacity_: self.capacity_,
        }
    }

    /// 推进读者位置：`rp += amount`（物理上环绕）；**读者越过缓冲区物理末端
    /// （`rp + amount >= capacity`）时清除 REVERSION flag**——读者跨过末端后，
    /// 写者的原始位置重新位于读者之前，恢复未跨状态。返回推进后的**新位置状态**
    /// （不含任何标志位）；需要写回 `atm_stat_` 时以原状态字为基座调用
    /// [`IoPos::pack`]。
    ///
    /// 前置：`amount <= data_size`（不允许读过头；恰好读空时回到
    /// `wp == rp && !rv` 的空态）。
    pub fn advance_rp(&self, amount: usize) -> Self {
        debug_assert!(amount <= self.data_size());
        let new_rp = self.rp + amount;
        let crossed = self.rp + amount >= self.capacity_;
        Self {
            rv: self.rv && !crossed,
            rp: new_rp % self.capacity_,
            wp: self.wp,
            capacity_: self.capacity_,
        }
    }
}

#[inline]
fn has_flag(state: usize, flag: usize) -> bool {
    state & flag != 0
}

#[gen_may_cancel_future(RingRead, pub, new(pub(super)))]
async fn ring_read_async<'f, B, T, K>(
    ring: &'f Ring<B, T>,
    demand: &'f Demand<usize>,
    cancel: K,
) -> SomeOf<
    ReclSliceRef<'f, T, Reclaim<'f, Ring<B, T>>>,
    ConsumerError<usize>,
>
where
    B: BorrowMut<[MaybeUninit<T>]>,
    K: TrCancellationToken,
{
    loop {
        if true {
            let x = ring.try_read_(demand);
            if x.contains_left() {
                return x
            }
            if x.contains_right_and(err_should_term_op_) {
                return x;
            }
            // x 在此处结束对 ring 的借用
        }
        if cancel.is_cancelled() {
            return SomeOf::new_right(ConsumerError::Cancelled);
        }
        // 「登记 → 发布 → 复检」三步在 park future 的首次 poll 里完成（见 half_.rs
        // 顶部的协议说明）。等待槽自带自旋锁，因此这里只需要共享引用。
        let consumer = unsafe { ring.consumer_.as_ref_unchecked() };
        let opt_err = consumer
            .park_async(demand, &ring.buf_stat_)
            .may_cancel_with(cancel.child_token())
            .await;
        if opt_err.as_ref().is_some_and(err_should_term_op_) {
            return SomeOf::new_right(opt_err.unwrap())
        }
    }
}

#[gen_may_cancel_future(RingWrite, pub, new(pub(super)))]
async fn ring_write_async<'f, B, T, K>(
    ring: &'f Ring<B, T>,
    demand: &'f Demand<usize>,
    cancel: K,
) -> SomeOf<
    ReclSliceMut<'f, T, Reclaim<'f, Ring<B, T>>>,
    ProducerError<usize>,
>
where
    B: BorrowMut<[MaybeUninit<T>]>,
    K: TrCancellationToken,
{
    loop {
        if true {
            let x = ring.try_write_(demand);
            if x.contains_left() {
                return x
            }
            if x.contains_right_and(err_should_term_op_) {
                return x;
            }
        }
        if cancel.is_cancelled() {
            return SomeOf::new_right(ProducerError::Cancelled);
        }
        // 同 `ring_read_async`：协议三步在 park future 的首次 poll 里完成。
        let producer = unsafe { ring.producer_.as_ref_unchecked() };
        let opt_err = producer
            .park_async(demand, &ring.buf_stat_)
            .may_cancel_with(cancel.child_token())
            .await;
        if opt_err.as_ref().is_some_and(err_should_term_op_) {
            return SomeOf::new_right(opt_err.unwrap())
        }
    }
}

#[derive(Debug)]
pub struct RingState {
    atm_flag_: AtomicFlags<usize>,
    capacity_: usize,
}

impl RingState {
    pub(super) const fn new(capacity: usize) -> Self {
        RingState {
            atm_flag_: AtomicFlags::new(AtomicUsize::new(0usize)),
            capacity_: capacity,
        }
    }

    #[inline]
    pub const fn capacity(&self) -> usize {
        self.capacity_
    }

    #[inline]
    pub fn value(&self) -> usize {
        self.atm_flag_.value()
    }

    #[inline]
    pub fn io_pos(&self) -> IoPos {
        IoPos::unpack(self.atm_flag_.value(), self.capacity())
    }

    #[inline]
    pub fn data_size(&self) -> usize {
        self.io_pos().data_size()
    }

    /// 当前可写空间量。
    #[inline]
    pub fn free_size(&self) -> usize {
        self.io_pos().free_size()
    }

    #[inline]
    pub fn is_consumer_closed(&self) -> bool {
        let state = self.atm_flag_.value();
        has_flag(state, CONSUMER_CLOSED)
    }

    #[inline]
    pub fn is_producer_closed(&self) -> bool {
        let state = self.atm_flag_.value();
        has_flag(state, PRODUCER_CLOSED)
    }

    // ------------------------------------------------------------------
    // 等待槽的「兴趣位」与判据
    //
    // 兴趣位（STNDBY）的语义是「当前有一个已登记的等待者」，由等待者发布、由认领方
    // （对端唤醒或等待者撤回）清除；它同时是唤醒路径的廉价快照判断。具体的「判定 +
    // 认领 + 取走」由 `half_` 里的等待槽自旋锁串起来。
    // ------------------------------------------------------------------

    /// 兴趣位当前是否置位（供 `half_` 的单测断言协议中间状态）。
    #[cfg(test)]
    #[inline]
    pub(super) fn is_consumer_standby_(&self) -> bool {
        has_flag(self.atm_flag_.value(), CONSUMER_STNDBY)
    }

    /// 见 `is_consumer_standby_`。
    #[cfg(test)]
    #[inline]
    pub(super) fn is_producer_standby_(&self) -> bool {
        has_flag(self.atm_flag_.value(), PRODUCER_STNDBY)
    }

    /// 读端现在是否已能满足 `demand`：判据与 `Ring::try_read_internal_` 一致
    /// （下限缺省为 1：没有显式下限时，只要有 1 个可读就够）。
    ///
    /// 「生产端已关闭」也算可满足：关闭意味着条件**永久**不可能再被满足，等待者必须
    /// 被唤醒去拿 `Closing`，否则会永久挂起（`wake` 侧的判据必须与这里保持一致）。
    #[inline]
    pub(super) fn can_consume_(&self, demand: &Demand<usize>) -> bool {
        self.data_size() >= demand.min().unwrap_or(1usize) || self.is_producer_closed()
    }

    /// 写端现在是否已能满足 `demand`：判据与 `Ring::try_write_internal_` 一致。
    ///
    /// 同 [`Self::can_consume_`]：「消费端已关闭」也算可满足。
    #[inline]
    pub(super) fn can_produce_(&self, demand: &Demand<usize>) -> bool {
        self.free_size() >= demand.min().unwrap_or(1usize) || self.is_consumer_closed()
    }

    /// 置位 `PRODUCER_CLOSED`（CAS `0 → 1`）；返回是否**由本次调用**置位。
    ///
    /// 调用方（[`Ring::close_producer_`]）据此决定谁负责唤醒对端，重复关闭是幂等的。
    pub(super) fn try_set_producer_closed_(&self) -> bool {
        let expect = |s| !has_flag(s, PRODUCER_CLOSED);
        let desire = |s| s | PRODUCER_CLOSED;
        self.atm_flag_
            .try_spin_compare_exchange_weak(expect, desire)
            .is_succ()
    }

    /// 置位 `CONSUMER_CLOSED`。语义同 [`Self::try_set_producer_closed_`]。
    pub(super) fn try_set_consumer_closed_(&self) -> bool {
        let expect = |s| !has_flag(s, CONSUMER_CLOSED);
        let desire = |s| s | CONSUMER_CLOSED;
        self.atm_flag_
            .try_spin_compare_exchange_weak(expect, desire)
            .is_succ()
    }

    /// 等待者发布读端兴趣位：CAS `0 → 1`（保留位置位）。
    ///
    /// 返回 `false` 表示兴趣位已经是 1 —— 正常协议下不该发生，由调用方处理成「重试」。
    pub(super) fn try_publish_consumer_standby_(&self) -> bool {
        let expect = |s| !has_flag(s, CONSUMER_STNDBY);
        let desire = |s| s | CONSUMER_STNDBY;
        self.atm_flag_
            .try_spin_compare_exchange_weak(expect, desire)
            .is_succ()
    }

    /// 认领 / 撤回读端兴趣位：CAS `1 → 0`。抢到的一方负责取走登记。
    pub(super) fn try_claim_consumer_standby_(&self) -> bool {
        let expect = |s| has_flag(s, CONSUMER_STNDBY);
        let desire = |s| s & !CONSUMER_STNDBY;
        self.atm_flag_
            .try_spin_compare_exchange_weak(expect, desire)
            .is_succ()
    }

    /// 等待者发布写端兴趣位。语义同 [`Self::try_publish_consumer_standby_`]。
    pub(super) fn try_publish_producer_standby_(&self) -> bool {
        let expect = |s| !has_flag(s, PRODUCER_STNDBY);
        let desire = |s| s | PRODUCER_STNDBY;
        self.atm_flag_
            .try_spin_compare_exchange_weak(expect, desire)
            .is_succ()
    }

    /// 认领 / 撤回写端兴趣位。语义同 [`Self::try_claim_consumer_standby_`]。
    pub(super) fn try_claim_producer_standby_(&self) -> bool {
        let expect = |s| has_flag(s, PRODUCER_STNDBY);
        let desire = |s| s & !PRODUCER_STNDBY;
        self.atm_flag_
            .try_spin_compare_exchange_weak(expect, desire)
            .is_succ()
    }
}

fn err_should_term_op_<E, T>(err: &E) -> bool
where
    E: TrTaggedError<T>,
    T: TrErrTag + Into<IoErrTag>,
{
    err.err_tag().into().should_terminate()
}

// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// RingReader
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

pub type RingSegmRef<'f, B, T> =
    ReclSliceRef<'f, T, Reclaim<'f, Ring<B, T>>>;

#[derive(Debug)]
pub struct RingReader<S, B, T>
where
    S: Borrow<Ring<B, T>>,
    B: BorrowMut<[MaybeUninit<T>]>,
{
    ring_ref_: S,
    _using_r_: PhantomData<Ring<B, T>>,
}

impl<S, B, T> RingReader<S, B, T>
where
    S: Borrow<Ring<B, T>>,
    B: BorrowMut<[MaybeUninit<T>]>,
{
    const fn new_(ring: S) -> Self {
        RingReader { ring_ref_: ring, _using_r_: PhantomData }
    }

    #[inline]
    pub fn try_read<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<RingSegmRef<'f, B, T>, ConsumerError<usize>> {
        self.ring_ref_.borrow().try_read_(demand)
    }

    #[inline]
    pub fn ring_state(&self) -> &RingState {
        &self.ring_ref_.borrow().buf_stat_
    }

    #[inline]
    pub fn consumer_state(&self) -> Option<(usize, bool)> {
        self.ring_ref_.borrow().consumer_state()
    }

    /// 关闭消费端：此后不再消费，并唤醒等待中的生产者（见 [`Ring::close_consumer`]）。
    ///
    /// 收 `&mut self`：关闭权归**独占持有本半部**的一方。关闭本身只动状态字、不碰数据区，
    /// 但它是不可逆的状态迁移，因此不允许再从共享句柄（`&Ring`）旁路触发——那样会在
    /// 本条环仍有活段 / 活半部时并发地改变契约。
    #[inline]
    pub fn close(&mut self) {
        self.ring_ref_.borrow().close_consumer_()
    }
}

impl<S, B, T> RingReader<S, B, T>
where
    S: Borrow<Ring<B, T>>,
    B: BorrowMut<[MaybeUninit<T>]>,
{
    #[inline]
    pub fn read_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> RingReadAsync<'f, 'f, B, T> {
        let ring = self.ring_ref_.borrow();
        RingReadAsync::new(ring, demand)
    }
}

impl<S, B, T> TrConsumerState for RingReader<S, B, T>
where
    S: Borrow<Ring<B, T>>,
    B: BorrowMut<[MaybeUninit<T>]>,
{
    #[inline]
    fn consumer_state(&self) -> Option<(usize, bool)> {
        RingReader::consumer_state(self)
    }
}

impl<S, B, T> abs_buff::TrBuffTryRead<T> for RingReader<S, B, T>
where
    S: Borrow<Ring<B, T>>,
    B: BorrowMut<[MaybeUninit<T>]>,
{
    type SegmRef<'f> = RingSegmRef<'f, B, T> where Self: 'f;
    type Err = ConsumerError<usize>;

    #[inline]
    fn try_read<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        RingReader::try_read(self, demand)
    }
}

impl<S, B, T> abs_buff::TrBuffRead<T> for RingReader<S, B, T>
where
    S: Borrow<Ring<B, T>>,
    B: BorrowMut<[MaybeUninit<T>]>,
{
    type ReadAsync<'f> = RingReadAsync<'f, 'f, B, T> where Self: 'f;

    #[inline]
    fn read_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> Self::ReadAsync<'f> {
        RingReader::read_async(self, demand)
    }
}

// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// RingWriter
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

pub type RingSegmMut<'f, B, T> =
    ReclSliceMut<'f, T, Reclaim<'f, Ring<B, T>>>;

#[derive(Debug)]
pub struct RingWriter<S, B, T>
where
    S: Borrow<Ring<B, T>>,
    B: BorrowMut<[MaybeUninit<T>]>,
{
    ring_ref_: S,
    _using_r_: PhantomData<Ring<B, T>>,
}

impl<S, B, T> RingWriter<S, B, T>
where
    S: Borrow<Ring<B, T>>,
    B: BorrowMut<[MaybeUninit<T>]>,
{
    const fn new_(ring: S) -> Self {
        RingWriter { ring_ref_: ring, _using_r_: PhantomData }
    }

    #[inline]
    pub fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<RingSegmMut<'f, B, T>, ProducerError<usize>> {
        self.ring_ref_.borrow().try_write_(demand)
    }

    #[inline]
    pub fn ring_state(&self) -> &RingState {
        &self.ring_ref_.borrow().buf_stat_
    }

    #[inline]
    pub fn producer_state(&self) -> Option<(usize, bool)> {
        let ring = &self.ring_ref_.borrow();
        Ring::producer_state(ring)
    }

    /// 关闭生产端（EOF）：此后不再有新数据，并唤醒等待中的消费者
    /// （见 [`Ring::close_producer`]）。
    ///
    /// 收 `&mut self`：理由同 [`RingReader::close`]——关闭权归独占持有本半部的一方。
    #[inline]
    pub fn close(&mut self) {
        self.ring_ref_.borrow().close_producer_()
    }
}

impl<S, B, T> RingWriter<S, B, T>
where
    S: Borrow<Ring<B, T>>,
    B: BorrowMut<[MaybeUninit<T>]>,
{
    #[inline]
    pub fn write_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> RingWriteAsync<'f, 'f, B, T> {
        let ring = self.ring_ref_.borrow();
        RingWriteAsync::new(ring, demand)
    }
}

impl<S, B, T> TrProducerState for RingWriter<S, B, T>
where
    S: Borrow<Ring<B, T>>,
    B: BorrowMut<[MaybeUninit<T>]>,
{
    #[inline]
    fn producer_state(&self) -> Option<(usize, bool)> {
        RingWriter::producer_state(self)
    }
}

impl<S, B, T> abs_buff::TrBuffTryWrite<T> for RingWriter<S, B, T>
where
    S: Borrow<Ring<B, T>>,
    B: BorrowMut<[MaybeUninit<T>]>,
{
    type SegmMut<'f> = RingSegmMut<'f, B, T> where Self: 'f;
    type Err = ProducerError<usize>;

    #[inline]
    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        RingWriter::try_write(self, demand)
    }
}

impl<S, B, T> abs_buff::TrBuffWrite<T> for RingWriter<S, B, T>
where
    S: Borrow<Ring<B, T>>,
    B: BorrowMut<[MaybeUninit<T>]>,
{
    type WriteAsync<'f> = RingWriteAsync<'f, 'f, B, T> where Self: 'f;

    #[inline]
    fn write_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> Self::WriteAsync<'f> {
        RingWriter::write_async(self, demand)
    }
}
