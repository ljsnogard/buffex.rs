//! 环形缓冲的段类型：写段 / 读段（本次重构从 `ring_buffer::reclaim_`
//! 复制而来，模块头注释仍残留「RingBuffer 专用」字样）。
//!
//! 普通的 abs_buff `SegmRef` / `SegmMut` 只能表达**一段物理连续**的缓冲区。
//! 但环形缓冲的可用 / 可读区域在环绕缓冲区末端时会被物理拆成两段
//! （例如末端 2 格 + 开端 2 格）。因此这里定制了专用段类型，内部用一个 enum
//! 表达"只有一段连续空间"或"拥有两段连续空间"两种可能性，把两段物理空间
//! 视作**逻辑上的一段**——这样生产者 / 消费者可以一次性拿到跨末端的全部空间。
//!
//! 这些类型同样实现 `TrBuffSegmRef` / `TrBuffSegmMut`，因此 abs_buff 的管道
//! 机制（PipeJoin）可以直接使用；`as_segm_ref` / `as_segm_mut` 每次交出
//! 当前物理段的一个子段，父段的 offset 在子段 drop 时累计，段整体 drop 时
//! 按已消费量提交给核心（逐段回收粒度）。
//!
//! # 与旧 `segm_` 的关系
//!
//! 旧 `segm_` 的 `WrSegm` / `RdSegm` 是旧设计（提交器直接引用单参数
//! `CircCore<T>`），已随重构删除；本模块的 `ReclSliceMut` / `ReclSliceRef`
//! 把提交器泛型化于 `TyCore: TrCircBuffCore`
//! （`super::abs_comp_::TrCircBuffCore`），是重构后的替代实现。
//!
//! # 设计意图
//!
//! 段 = 物理空间（两段式 enum）+ 已消费 offset + 提交器（drop 时回收）。提交器
//! 只依赖 `TrCircBuffCore` 窄接口，因此段
//! 类型本身**不指名核心的具体类型**——这是本重构解开类型级循环的关键拼图
//! （循环发生在端类型携带段类型关联时，见
//! `abs_comp_` 模块文档）。

use core::{
    mem::MaybeUninit,
    pin::Pin,
};

use abs_buff::{
    Demand,
    buffer::{
        SegmMut, SegmReclaim, SegmRef,
        TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView, TrReclaim,
    }
};

// ---------------------------------------------------------------------------
// 物理空间的两段式表示
// ---------------------------------------------------------------------------

fn non_empty_slice<T>(s: &&[T]) -> bool {
    !s.is_empty()
}

/// 写段持有的物理空间：一段连续，或两段连续（跨越缓冲区末端）。
pub(super) enum SegmSlicesMut<'a, T> {
    One(&'a mut [MaybeUninit<T>]),
    Two(&'a mut [MaybeUninit<T>], &'a mut [MaybeUninit<T>]),
}

impl<'a, T> SegmSlicesMut<'a, T> {
    #[inline]
    fn len(&self) -> usize {
        match self {
            SegmSlicesMut::One(a) => a.len(),
            SegmSlicesMut::Two(a, b) => a.len() + b.len(),
        }
    }

    /// 按 `offset` 返回剩余空间的两段（不足两段时以空段补齐）。
    fn remaining_mut(&mut self, offset: usize) -> [&mut [MaybeUninit<T>]; 2] {
        match self {
            SegmSlicesMut::One(a) => [&mut a[offset..], &mut []],
            SegmSlicesMut::Two(a, b) => {
                let la = a.len();
                if offset < la {
                    [&mut a[offset..], b]
                } else {
                    [&mut b[offset - la..], &mut []]
                }
            }
        }
    }

    /// `offset` 所在物理段的剩余部分（子段的基础）。
    fn current_mut(&mut self, offset: usize) -> &mut [MaybeUninit<T>] {
        match self {
            SegmSlicesMut::One(a) => &mut a[offset..],
            SegmSlicesMut::Two(a, b) => {
                let la = a.len();
                if offset < la {
                    &mut a[offset..]
                } else {
                    &mut b[offset - la..]
                }
            }
        }
    }

    /// 只读视图版本的 [`SegmSlicesMut::remaining_mut`]。
    fn remaining_ref(&self, offset: usize) -> [&[MaybeUninit<T>]; 2] {
        match self {
            SegmSlicesMut::One(a) => [&a[offset..], &[]],
            SegmSlicesMut::Two(a, b) => {
                let la = a.len();
                if offset < la {
                    [&a[offset..], b]
                } else {
                    [&b[offset - la..], &[]]
                }
            }
        }
    }
}

/// 读段 / 窥视段持有的物理空间：一段连续，或两段连续。
pub(super) enum SegmSlicesRef<'a, T> {
    One(&'a [T]),
    Two(&'a [T], &'a [T]),
}

impl<'a, T> SegmSlicesRef<'a, T> {
    #[inline]
    fn len(&self) -> usize {
        match self {
            SegmSlicesRef::One(a) => a.len(),
            SegmSlicesRef::Two(a, b) => a.len() + b.len(),
        }
    }

    fn current(&self, offset: usize) -> &[T] {
        match self {
            SegmSlicesRef::One(a) => &a[offset..],
            SegmSlicesRef::Two(a, b) => {
                let la = a.len();
                if offset < la {
                    &a[offset..]
                } else {
                    &b[offset - la..]
                }
            }
        }
    }

    fn remaining_ref(&self, offset: usize) -> [&[T]; 2] {
        match self {
            SegmSlicesRef::One(a) => [&a[offset..], &[]],
            SegmSlicesRef::Two(a, b) => {
                let la = a.len();
                if offset < la {
                    [&a[offset..], b]
                } else {
                    [&b[offset - la..], &[]]
                }
            }
        }
    }
}

pub(crate) type Adv<X> = fn(&X, usize) -> usize;

// ---------------------------------------------------------------------------
// 回收器
// ---------------------------------------------------------------------------

/// 提交器：写段 drop 时按已消费量推进写位置。
///
/// 泛型于 `TyCore: TrCircBuffCore`——通过窄接口提交，**不指名核心的具体类型**
/// （若指名 `CircCore<P, C, T>` 且该段出现在端类型的关联类型中，就构成类型级
/// 循环，见 `abs_comp_` 模块文档）。
pub struct Reclaim<'a, TyCtx> {
    ctx_: &'a TyCtx,
    adv_: Adv<TyCtx>,
}

impl<'a, TyCtx> Reclaim<'a, TyCtx> {
    pub(crate) const fn new(ctx: &'a TyCtx, adv: Adv<TyCtx>) -> Self {
        Reclaim {
            ctx_: ctx,
            adv_: adv,
        }
    }
}

impl<TyCtx> TrReclaim for Reclaim<'_, TyCtx> {
    fn reclaim(&mut self, amount: usize) -> usize {
        (self.adv_)(self.ctx_, amount)
    }
}

unsafe impl<TyCtx> Send for Reclaim<'_, TyCtx>
{}

unsafe impl<TyCtx> Sync for Reclaim<'_, TyCtx>
{}

pub struct NoReclaim;

impl TrReclaim for NoReclaim {
    fn reclaim(&mut self, _: usize) -> usize {
        0usize
    }
}

pub type ChildReclaim<'a> = SegmReclaim<'a>;

// ---------------------------------------------------------------------------
// 写段
// ---------------------------------------------------------------------------

/// 环形核心专用写段：两段物理空间视作逻辑上的一段（见模块文档）。
///
/// drop 时把已消费量经 `R`（通常是 [`WriterReclaim`]）提交给核心，推进写位置
/// 并触发消费端 hook。
pub struct ReclSliceMut<'a, T, R>
where
    R: TrReclaim,
{
    pieces: SegmSlicesMut<'a, T>,
    /// 已消费（已提交给 ring）的逻辑单元数，跨两段累计。
    offset_: usize,
    reclaim_: Option<R>,
}

impl<'a, T, R> ReclSliceMut<'a, T, R>
where
    R: TrReclaim,
{
    pub(super) fn new(
        pieces: SegmSlicesMut<'a, T>,
        reclaim: R,
    ) -> Self {
        ReclSliceMut {
            pieces,
            offset_: 0,
            reclaim_: Option::Some(reclaim),
        }
    }

    /// 逻辑上剩余（未消费）的单元数：两段物理空间之和减去已消费量。
    #[inline]
    pub fn least_count(&self) -> usize {
        self.pieces.len() - self.offset_
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.least_count() == 0
    }

    /// 本段逻辑上的总容量（两段物理空间之和）。
    #[inline]
    pub fn capacity(&self) -> usize {
        self.pieces.len()
    }

    /// 剩余可写空间按物理段切出（最多两段），供调用方直接写入。
    /// 空段会被过滤，调用方看到的每一段都非空。
    pub fn iter_slices_mut(&mut self) -> impl Iterator<Item = &mut [MaybeUninit<T>]> {
        self.pieces
            .remaining_mut(self.offset_)
            .into_iter()
            .filter(|s| !s.is_empty())
    }

    /// 当前物理段的剩余部分作为一个 abs_buff 子段；子段 drop 时通过
    /// [`ChildReclaim`] 把其已消费量累计到父段的 `offset`。
    pub fn as_segm_mut<'f>(
        &'f mut self,
    ) -> SegmMut<'f, T, ChildReclaim<'f>> {
        // 直接借用 `self.pieces` 与 `self.offset`（不同字段，借用检查器
        // 可判定互斥），与 abs_buff 内部 `SegmRef::as_segm_ref` 的做法一致。
        let slice = self.pieces.current_mut(self.offset_);
        let reclaim = ChildReclaim::new(Pin::new(&mut self.offset_));
        SegmMut::new(slice, reclaim)
    }

    pub fn take_segm_mut<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> Option<SegmMut<'f, T, ChildReclaim<'f>>> {
        let c = self.least_count();
        if c == 0 {
            return Option::None;
        }
        let available = Demand::no_more_than(c);
        let agreement = demand.compromise(&available)?;
        let max_len = agreement.max()?;
        // 子段只能覆盖当前物理段；跨段部分由下一次 take 处理。
        let cur = self.pieces.current_mut(self.offset_);
        let take = core::cmp::min(max_len, cur.len());
        let slice = &mut cur[..take];
        let reclaim = ChildReclaim::new(Pin::new(&mut self.offset_));
        Option::Some(SegmMut::new(slice, reclaim))
    }

    /// See `TrBuffSegmMut::move_items_from_buff`.
    ///
    /// ## Safety
    ///
    /// - See `TrBuffSegmMut::move_items_from_buff`
    #[inline]
    pub fn move_items_from_buff(&mut self, src: &[MaybeUninit<T>]) -> usize {
        TrBuffSegmMut::move_items_from_buff(self, src)
    }
}

impl<'a, T, R> Drop for ReclSliceMut<'a, T, R>
where
    R: TrReclaim,
{
    fn drop(&mut self) {
        let Option::Some(mut r) = self.reclaim_.take() else {
            return;
        };
        r.reclaim(self.offset_);
    }
}

impl<'a, T, R> TrBuffSegmView for ReclSliceMut<'a, T, R>
where
    R: TrReclaim,
{
    type SlicesIter<'f>
        = core::iter::Filter<
            core::array::IntoIter<&'f [MaybeUninit<T>], 2>,
            fn(&&'f [MaybeUninit<T>]) -> bool,
        >
    where
        Self: 'f,
        T: 'f;
    type Item = MaybeUninit<T>;

    #[inline]
    fn is_empty(&self) -> bool {
        ReclSliceMut::is_empty(self)
    }

    #[inline]
    fn least_count(&self) -> usize {
        ReclSliceMut::least_count(self)
    }

    /// 剩余可写空间按物理段切出（最多两段，逻辑上是一段）；空段被过滤。
    fn iter_slices(&self) -> Self::SlicesIter<'_> {
        let filter: fn(&&[MaybeUninit<T>]) -> bool = non_empty_slice;
        self.pieces
            .remaining_ref(self.offset_)
            .into_iter()
            .filter(filter)
    }
}

impl<'a, T, R> TrBuffSegmMut<'a, T> for ReclSliceMut<'a, T, R>
where
    R: TrReclaim,
{
    type Reclaimer<'f> = ChildReclaim<'f> where Self: 'f;

    type TakeSegmMut<'f>
        = Option<SegmMut<'f, T, ChildReclaim<'f>>>
    where
        Self: 'f,
        T: 'f;

    #[inline]
    fn as_segm_mut<'f>(&'f mut self) -> SegmMut<'f, T, Self::Reclaimer<'f>> {
        ReclSliceMut::as_segm_mut(self)
    }

    #[inline]
    fn take_segm_mut<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> Self::TakeSegmMut<'f> {
        ReclSliceMut::take_segm_mut(self, demand)
    }
}

// ---------------------------------------------------------------------------
// 读段 / 窥视段
// ---------------------------------------------------------------------------

/// 环形核心专用读段（窥视段是同一类型、只是 drop 时不提交）。
///
/// drop 时把已消费量经 `R`（通常是 [`ReaderReclaim`]）提交给核心，推进读位置
/// 并触发生产端 hook。
pub struct ReclSliceRef<'a, T, R>
where
    R: TrReclaim,
{
    pieces: SegmSlicesRef<'a, T>,
    offset_: usize,
    reclaim_: Option<R>,
}

/// RingBuffer 专用窥视段：窥视不消费，drop 时不推进读位置。
pub type ReclPeekRef<'a, T, R> = ReclSliceRef<'a, T, R>;

impl<'a, T, R> ReclSliceRef<'a, T, R>
where
    R: TrReclaim,
{
    pub(super) fn new(
        pieces: SegmSlicesRef<'a, T>,
        reclaim: R,
    ) -> Self {
        ReclSliceRef {
            pieces,
            offset_: 0,
            reclaim_: Option::Some(reclaim),
        }
    }

    /// 逻辑上剩余（未消费）的单元数。
    #[inline]
    pub fn least_count(&self) -> usize {
        self.pieces.len() - self.offset_
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.least_count() == 0
    }

    /// 本段逻辑上的总容量（两段物理空间之和）。
    #[inline]
    pub fn capacity(&self) -> usize {
        self.pieces.len()
    }

    /// 剩余可读空间按物理段切出（最多两段，逻辑上是一段）；空段被过滤。
    pub fn iter_slices(&self) -> impl Iterator<Item = &[T]> {
        self.pieces
            .remaining_ref(self.offset_)
            .into_iter()
            .filter(|s| !s.is_empty())
    }

    pub fn take_segm_ref<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> Option<SegmRef<'f, T, ChildReclaim<'f>>> {
        let c = self.least_count();
        if c == 0 {
            return Option::None;
        }
        let available = Demand::no_more_than(c);
        let agreement = demand.compromise(&available)?;
        let max_len = agreement.max()?;
        let cur = self.pieces.current(self.offset_);
        let take = core::cmp::min(max_len, cur.len());
        let slice = &cur[..take];
        let reclaim = ChildReclaim::new(Pin::new(&mut self.offset_));
        Option::Some(SegmRef::new(slice, reclaim))
    }

    /// 当前物理段的剩余部分作为一个 abs_buff 子段（同写段的设计）。
    pub fn as_segm_ref<'f>(&'f mut self) -> SegmRef<'f, T, ChildReclaim<'f>> {
        let slice = self.pieces.current(self.offset_);
        let reclaim = ChildReclaim::new(Pin::new(&mut self.offset_));
        SegmRef::new(slice, reclaim)
    }

    /// See [abs_buff::buffer::TrBuffSegmRef::move_items_to_buff]
    ///
    /// ## Safety
    ///
    /// - See [abs_buff::buffer::TrBuffSegmRef::move_items_to_buff]
    pub unsafe fn move_items_to_buff(&mut self, dst: &mut [MaybeUninit<T>]) -> usize {
        TrBuffSegmRef::move_items_to_buff(self, dst)
    }
}

impl<'a, T, R> Drop for ReclSliceRef<'a, T, R>
where
    R: TrReclaim,
{
    fn drop(&mut self) {
        let Option::Some(mut r) = self.reclaim_.take() else {
            return;
        };
        r.reclaim(self.offset_);
    }
}

impl<'a, T, R> TrBuffSegmView for ReclSliceRef<'a, T, R>
where
    R: TrReclaim,
{
    type SlicesIter<'f>
        = core::iter::Filter<
            core::array::IntoIter<&'f [T], 2>,
            fn(&&'f [T]) -> bool,
        >
    where
        Self: 'f,
        T: 'f;
    type Item = T;

    #[inline]
    fn is_empty(&self) -> bool {
        ReclSliceRef::is_empty(self)
    }

    #[inline]
    fn least_count(&self) -> usize {
        ReclSliceRef::least_count(self)
    }

    /// 剩余可读空间按物理段切出（最多两段，逻辑上是一段）；空段被过滤。
    fn iter_slices(&self) -> Self::SlicesIter<'_> {
        let filter: fn(&&[T]) -> bool = non_empty_slice;
        self.pieces
            .remaining_ref(self.offset_)
            .into_iter()
            .filter(filter)
    }
}

impl<'a, T, R> TrBuffSegmRef<'a, T> for ReclSliceRef<'a, T, R>
where
    R: TrReclaim,
{
    type Reclaimer<'f> = ChildReclaim<'f> where Self: 'f;

    type TakeSegmRef<'f>
        = Option<SegmRef<'f, T, ChildReclaim<'f>>>
    where
        Self: 'f,
        T: 'f;

    #[inline]
    fn as_segm_ref<'f>(&'f mut self) -> SegmRef<'f, T, Self::Reclaimer<'f>> {
        ReclSliceRef::as_segm_ref(self)
    }

    #[inline]
    fn take_segm_ref<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> Self::TakeSegmRef<'f> {
        ReclSliceRef::take_segm_ref(self, demand)
    }
}
