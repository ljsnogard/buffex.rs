use core::{
    borrow::{Borrow, BorrowMut},
    error::Error,
    fmt,
    future::Future,
    marker::{PhantomData, PhantomPinned},
    mem::MaybeUninit,
    pin::Pin,
    slice,
    task::{Context, Poll},
};

use abs_cancel::{TrCancellationToken, TrMayCancel};
use anylr::SomeOf;

use crate::{
    Demand, TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
    buffer::{
        SegmMut, SegmReclaim, SegmRef,
        TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView,
        TrConsumerState, TrProducerState,
    },
    error::{ReadErrTag, TrErrTag, TrTaggedError, WriteErrTag},
};

/// Error returned when a borrowed byte slice is empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BorrowedSliceError<TyTag>
where
    TyTag: TrErrTag,
{
    Empty(TyTag),
}

impl<TyTag> fmt::Display for BorrowedSliceError<TyTag>
where
    TyTag: TrErrTag,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BorrowedSliceError::Empty(tag) => {
                write!(f, "{}: borrowed byte slice is empty", tag)
            }
        }
    }
}

impl<TyTag: TrErrTag> Error for BorrowedSliceError<TyTag> {}

impl<TyTag> TrTaggedError<TyTag> for BorrowedSliceError<TyTag>
where
    TyTag: TrErrTag,
{
    fn err_tag(&self) -> TyTag {
        match self {
            BorrowedSliceError::Empty(tag) => *tag,
        }
    }
}

/// 一个「立即就绪」的 future，产出 [`SomeOf<S, E>`]。
///
/// 本类型既服务于本 crate 的切片实现（`TrInput` / `TrOutput` 的异步方法直接
/// 返回它），也作为**公开工具**提供给下游实现者：任何自定义 `TrInput` /
/// `TrOutput` 都可以在同步就绪的场合把它当作 `ReadAsync<'f>` /
/// `WriteAsync<'f>` 的具体类型，而不必自己再写一个 `Future`。
///
/// # Panics
///
/// [`Future::poll`] 在同一个实例上第二次被调用时会 panic：它只承载「一次就绪
/// 结果」，被 poll 一次后内部值已被取走。正常使用（`await` 一次）不会触发。
pub struct ReadySegm<S, E>(Option<SomeOf<S, E>>);

impl<S, E> ReadySegm<S, E> {
    /// 用一个已经就绪的结果构造本 future。
    ///
    /// # Examples
    ///
    /// ```
    /// use abs_buff::{ReadySegm, x_deps::anylr::SomeOf};
    ///
    /// let ready: ReadySegm<usize, ()> = ReadySegm::new(SomeOf::new_left(3usize));
    /// ```
    pub fn new(value: SomeOf<S, E>) -> Self {
        ReadySegm(Option::Some(value))
    }
}

impl<S, E> Future for ReadySegm<S, E> {
    type Output = SomeOf<S, E>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        Poll::Ready(this.0.take().expect("a ready future must be polled once"))
    }
}

impl<'a, S, E> TrMayCancel<'a> for ReadySegm<S, E>
where
    S: 'a,
    E: 'a,
{
    type MayCancelFuture<'f, C> = ReadySegm<S, E>
    where
        'f: 'a,
        Self: 'f,
        C: 'f + TrCancellationToken;

    type MayCancelOutput = SomeOf<S, E>;

    fn may_cancel_with<C>(self, _: C) -> Self::MayCancelFuture<'a, C>
    where
        C: 'a + TrCancellationToken,
    {
        self
    }
}

// ---------------------------------------------------------------------------
// 段被回收（drop）时对借入的切片引用做推进。
//
// 语义与 `std::io::Read for &[u8]` / `std::io::Write for &mut [u8]` 一致：
// 已消费/已写入的前缀会在段 drop 时从**调用方持有的那个引用**里摘除，而不
// 只是在段内部记录偏移。因此承载切片的段必须记住调用方的引用本身，并在
// drop 时把推进后的后缀写回——这正是下面两个 trait 需要对切片引用做特化、
// 且段需要额外携带「容器类型参数 `C`」的原因。
// ---------------------------------------------------------------------------

/// 段回收时把「读源引用」推进到未消费的后缀。
///
/// blanket 实现覆盖所有类型并默认为空操作（例如 `Vec<T>`、`Box<[T]>` 之类
/// 不支持借出后再推进的容器）；对 `&[T]` / `&mut [T]` 两个切片引用特化出
/// 与 `std::io::Read for &[u8]` 一致的推进语义。
pub trait TrReadAdvance {
    /// 从引用头部移除 `amount` 个元素。
    fn advance_(&mut self, amount: usize);
}

impl<C> TrReadAdvance for C {
    default fn advance_(&mut self, _amount: usize) {}
}

impl<T> TrReadAdvance for &[T] {
    fn advance_(&mut self, amount: usize) {
        let old = *self;
        *self = &old[amount..];
    }
}

impl<T> TrReadAdvance for &mut [T] {
    fn advance_(&mut self, amount: usize) {
        let old = core::mem::take(self);
        *self = &mut old[amount..];
    }
}

/// 段回收时把「写目标引用」推进到尚未写入的后缀。
///
/// 语义与 `std::io::Write for &mut [u8]` 一致；blanket 实现默认为空操作。
pub trait TrWriteAdvance {
    /// 从引用头部移除 `amount` 个已写入的槽位。
    fn advance_(&mut self, amount: usize);
}

impl<C> TrWriteAdvance for C {
    default fn advance_(&mut self, _amount: usize) {}
}

impl<T> TrWriteAdvance for &mut [T] {
    fn advance_(&mut self, amount: usize) {
        let old = core::mem::take(self);
        *self = &mut old[amount..];
    }
}

// ---------------------------------------------------------------------------
// 读段：借入一个「切片引用」容器 `C`（`&[T]` 或 `&mut [T]`）
// ---------------------------------------------------------------------------

/// 借入切片引用容器 `C` 的读段。
///
/// `C` 是调用方持有的那个切片引用（`&'x [T]` 或 `&'x mut [T]`）；段自身只
/// 记录已消费长度，真正的推进发生在 [`Drop`]：把 `C` 改写为剩余后缀，从而
/// 与 `std::io::Read for &[u8]` 的行为保持一致。
pub struct BorrowedReadSegm<'a, T, C>
where
    C: Borrow<[T]> + TrReadAdvance,
{
    source_: &'a mut C,
    offset_: usize,
    end_: usize,
    _item_: PhantomData<T>,
    _pinned_: PhantomPinned,
}

impl<'a, T, C> BorrowedReadSegm<'a, T, C>
where
    C: Borrow<[T]> + TrReadAdvance,
{
    fn with_limit(source: &'a mut C, max: Option<usize>) -> Self {
        let len = Borrow::<[T]>::borrow(&*source).len();
        let end_ = match max {
            Option::Some(m) if m < len => m,
            _ => len,
        };
        BorrowedReadSegm {
            source_: source,
            offset_: 0,
            end_,
            _item_: PhantomData,
            _pinned_: PhantomPinned,
        }
    }

    fn remaining(&self) -> &[T] {
        &Borrow::<[T]>::borrow(&*self.source_)[self.offset_..self.end_]
    }

    fn as_segm_ref<'f>(&'f mut self) -> SegmRef<'f, T, SegmReclaim<'f>> {
        let data =
            &Borrow::<[T]>::borrow(&*self.source_)[self.offset_..self.end_];
        SegmRef::new(
            data,
            SegmReclaim::new(Pin::new(&mut self.offset_))
        )
    }

    fn take_segm_ref<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> Option<SegmRef<'f, T, SegmReclaim<'f>>> {
        let c = self.end_ - self.offset_;
        if c == 0 {
            return Option::None;
        }
        let available = Demand::no_more_than(c);
        let agreement = demand.compromise(&available)?;
        let max_len = agreement.max()?;
        let data = &Borrow::<[T]>::borrow(&*self.source_)
            [self.offset_..self.offset_ + max_len];
        let reclaim = SegmReclaim::new(Pin::new(&mut self.offset_));
        Option::Some(SegmRef::new(data, reclaim))
    }
}

impl<T, C> Drop for BorrowedReadSegm<'_, T, C>
where
    C: Borrow<[T]> + TrReadAdvance,
{
    fn drop(&mut self) {
        // 把已消费的前缀写回调用方的引用。`advance_` 只做切片取子集，不会
        // panic；即便此前已 panic 展开，也只是「未推进」，不存在悬垂。
        self.source_.advance_(self.offset_);
    }
}

impl<T, C> TrBuffSegmView for BorrowedReadSegm<'_, T, C>
where
    C: Borrow<[T]> + TrReadAdvance,
{
    type SlicesIter<'f> = Option<&'f [T]> where Self: 'f;
    type Item = T;

    #[inline]
    fn is_empty(&self) -> bool {
        self.remaining().is_empty()
    }

    #[inline]
    fn least_count(&self) -> usize {
        self.remaining().len()
    }

    fn iter_slices(&self) -> Self::SlicesIter<'_> {
        let data = self.remaining();
        if data.is_empty() {
            Option::None
        } else {
            Option::Some(data)
        }
    }
}

impl<'a, T, C> TrBuffSegmRef<'a, T> for BorrowedReadSegm<'a, T, C>
where
    C: Borrow<[T]> + TrReadAdvance,
{
    type Reclaimer<'f> = SegmReclaim<'f> where Self: 'f;

    type TakeSegmRef<'f> = Option<SegmRef<'f, T, SegmReclaim<'f>>>
        where Self: 'f;

    #[inline]
    fn take_segm_ref<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> Self::TakeSegmRef<'f> {
        BorrowedReadSegm::take_segm_ref(self, demand)
    }

    #[inline]
    fn as_segm_ref<'f>(&'f mut self) -> SegmRef<'f, T, Self::Reclaimer<'f>> {
        BorrowedReadSegm::as_segm_ref(self)
    }
}

// ---------------------------------------------------------------------------
// 写段：借入一个 `&mut [T]` 引用容器 `C`
// ---------------------------------------------------------------------------

/// 借入 `&mut [T]` 引用容器 `C` 的写段。
///
/// `C` 是调用方持有的那个可变切片引用；段在 [`Drop`] 时把已写入的前缀从
/// `C` 中摘除，从而与 `std::io::Write for &mut [u8]` 的行为保持一致。
pub struct BorrowedWriteSegm<'a, T, C>
where
    C: BorrowMut<[T]> + TrWriteAdvance,
{
    target_: &'a mut C,
    offset_: usize,
    end_: usize,
    _item_: PhantomData<T>,
    _pinned_: PhantomPinned,
}

impl<'a, T, C> BorrowedWriteSegm<'a, T, C>
where
    C: BorrowMut<[T]> + TrWriteAdvance,
{
    fn with_limit(target: &'a mut C, max: Option<usize>) -> Self {
        let len = Borrow::<[T]>::borrow(&*target).len();
        let end_ = match max {
            Option::Some(m) if m < len => m,
            _ => len,
        };
        BorrowedWriteSegm {
            target_: target,
            offset_: 0,
            end_,
            _item_: PhantomData,
            _pinned_: PhantomPinned,
        }
    }

    fn remaining(&self) -> &[MaybeUninit<T>] {
        let bytes =
            &Borrow::<[T]>::borrow(&*self.target_)[self.offset_..self.end_];
        // SAFETY: `MaybeUninit<T>` has the same layout as `T`, and the
        // slice lifetime is tied to the underlying borrowed bytes.
        unsafe {
            slice::from_raw_parts(
                bytes.as_ptr().cast::<MaybeUninit<T>>(),
                bytes.len(),
            )
        }
    }

    fn as_segm_mut<'f>(&'f mut self) -> SegmMut<'f, T, SegmReclaim<'f>> {
        let bytes = &mut BorrowMut::<[T]>::borrow_mut(&mut *self.target_)
            [self.offset_..self.end_];
        // SAFETY: `MaybeUninit<T>` has the same layout as `T`, and the
        // mutable slice is exclusively borrowed from `T`.
        let data = unsafe {
            slice::from_raw_parts_mut(
                bytes.as_mut_ptr().cast::<MaybeUninit<T>>(),
                bytes.len(),
            )
        };
        let p_offs = Pin::new(&mut self.offset_);
        SegmMut::new(data, SegmReclaim::new(p_offs))
    }

    fn take_segm_mut<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> Option<SegmMut<'f, T, SegmReclaim<'f>>> {
        let c = self.end_ - self.offset_;
        if c == 0 {
            return Option::None;
        }
        let available = Demand::no_more_than(c);
        let agreement = demand.compromise(&available)?;
        let max_len = agreement.max()?;
        let data = &mut BorrowMut::<[T]>::borrow_mut(&mut *self.target_)
            [self.offset_..self.offset_ + max_len];
        let data = unsafe {
            slice::from_raw_parts_mut(
                data.as_mut_ptr().cast::<MaybeUninit<T>>(),
                data.len(),
            )
        };
        let reclaim = SegmReclaim::new(Pin::new(&mut self.offset_));
        Option::Some(SegmMut::new(data, reclaim))
    }
}

impl<T, C> Drop for BorrowedWriteSegm<'_, T, C>
where
    C: BorrowMut<[T]> + TrWriteAdvance,
{
    fn drop(&mut self) {
        // 把已写入的前缀写回调用方的可变引用，与
        // `std::io::Write for &mut [u8]` 一致。
        self.target_.advance_(self.offset_);
    }
}

impl<T, C> TrBuffSegmView for BorrowedWriteSegm<'_, T, C>
where
    C: BorrowMut<[T]> + TrWriteAdvance,
{
    type SlicesIter<'f> = Option<&'f [MaybeUninit<T>]> where Self: 'f;
    type Item = MaybeUninit<T>;

    #[inline]
    fn is_empty(&self) -> bool {
        self.remaining().is_empty()
    }

    #[inline]
    fn least_count(&self) -> usize {
        self.remaining().len()
    }

    fn iter_slices(&self) -> Self::SlicesIter<'_> {
        let data = self.remaining();
        if data.is_empty() {
            Option::None
        } else {
            Option::Some(data)
        }
    }
}

impl<'a, T, C> TrBuffSegmMut<'a, T> for BorrowedWriteSegm<'a, T, C>
where
    C: BorrowMut<[T]> + TrWriteAdvance,
{
    type Reclaimer<'f> = SegmReclaim<'f> where Self: 'f;

    type TakeSegmMut<'f> = Option<SegmMut<'f, T, SegmReclaim<'f>>> where Self: 'f;

    #[inline]
    fn take_segm_mut<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> Self::TakeSegmMut<'f> {
        BorrowedWriteSegm::take_segm_mut(self, demand)
    }

    #[inline]
    fn as_segm_mut<'f>(&'f mut self) -> SegmMut<'f, T, Self::Reclaimer<'f>> {
        BorrowedWriteSegm::as_segm_mut(self)
    }
}

// ---------------------------------------------------------------------------
// Blanket impls
// ---------------------------------------------------------------------------

impl<T> TrConsumerState for &[T] {
    fn consumer_state(&self) -> Option<(usize, bool)> {
        Option::Some((self.len(), self.is_empty()))
    }
}

impl<T> TrBuffTryRead<T> for &[T] {
    type SegmRef<'f> = BorrowedReadSegm<'f, T, Self> where Self: 'f;

    type Err = BorrowedSliceError<ReadErrTag>;

    fn try_read<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        let len = self.len();
        let min_len = demand.min().unwrap_or(0);
        // 空需求（`less_than(0)`）的取值集合为空集，任何可用量都满足不了它；
        // 它的 `min()` / `max()` 都是 `None`，所以必须显式判空，否则会被当成
        // "至少 0 个、无上界"而借出整段。下面各实现同理。
        if demand.is_empty() || len == 0 || len < min_len {
            let err = BorrowedSliceError::Empty(ReadErrTag::Closing);
            return SomeOf::new_right(err);
        }
        let max_len = demand.max();
        SomeOf::new_left(BorrowedReadSegm::with_limit(self, max_len))
    }
}

impl<T> TrBuffRead<T> for &[T] {
    type ReadAsync<'f> = ReadySegm<Self::SegmRef<'f>, Self::Err> where Self: 'f;

    fn read_async<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> Self::ReadAsync<'f> {
        let len = self.len();
        let min_len = demand.min().unwrap_or(0);
        if demand.is_empty() || len == 0 || len < min_len {
            return ReadySegm::new(SomeOf::new_right(
                BorrowedSliceError::Empty(ReadErrTag::Closing),
            ));
        }
        let max_len = demand.max();
        ReadySegm::new(SomeOf::new_left(BorrowedReadSegm::with_limit(
            self, max_len,
        )))
    }
}

// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<T> TrConsumerState for &mut [T] {
    fn consumer_state(&self) -> Option<(usize, bool)> {
        Option::Some((self.len(), self.is_empty()))
    }
}

impl<T> TrBuffTryRead<T> for &mut [T] {
    type SegmRef<'f> = BorrowedReadSegm<'f, T, Self> where Self: 'f;

    type Err = BorrowedSliceError<ReadErrTag>;

    fn try_read<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        let len = self.len();
        let min_len = demand.min().unwrap_or(0);
        if demand.is_empty() || len == 0 || len < min_len {
            let err = BorrowedSliceError::Empty(ReadErrTag::Closing);
            return SomeOf::new_right(err);
        }
        let max_len = demand.max();
        SomeOf::new_left(BorrowedReadSegm::with_limit(self, max_len))
    }
}

impl<T> TrBuffRead<T> for &mut [T] {
    type ReadAsync<'f> = ReadySegm<Self::SegmRef<'f>, Self::Err> where Self: 'f;

    fn read_async<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> Self::ReadAsync<'f> {
        let len = self.len();
        let min_len = demand.min().unwrap_or(0);
        if demand.is_empty() || len == 0 || len < min_len {
            return ReadySegm::new(SomeOf::new_right(
                BorrowedSliceError::Empty(ReadErrTag::Closing),
            ));
        }
        let max_len = demand.max();
        ReadySegm::new(SomeOf::new_left(BorrowedReadSegm::with_limit(
            self, max_len,
        )))
    }
}

// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<T> TrProducerState for &mut [T] {
    fn producer_state(&self) -> Option<(usize, bool)> {
        Option::Some((self.len(), self.is_empty()))
    }
}

impl<T> TrBuffTryWrite<T> for &mut [T] {
    type SegmMut<'f> = BorrowedWriteSegm<'f, T, Self> where Self: 'f;

    type Err = BorrowedSliceError<WriteErrTag>;

    fn try_write<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        let len = self.len();
        let min_len = demand.min().unwrap_or(0);
        if demand.is_empty() || len == 0 || len < min_len {
            let err = BorrowedSliceError::Empty(WriteErrTag::Closing);
            return SomeOf::new_right(err);
        }
        let max_len = demand.max();
        SomeOf::new_left(BorrowedWriteSegm::with_limit(self, max_len))
    }
}

impl<T> TrBuffWrite<T> for &mut [T] {
    type WriteAsync<'f> = ReadySegm<Self::SegmMut<'f>, Self::Err>
    where
        Self: 'f;

    fn write_async<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> Self::WriteAsync<'f> {
        let len = self.len();
        let min_len = demand.min().unwrap_or(0);
        if demand.is_empty() || len == 0 || len < min_len {
            return ReadySegm::new(SomeOf::new_right(
                BorrowedSliceError::Empty(WriteErrTag::Closing),
            ));
        }
        let max_len = demand.max();
        ReadySegm::new(SomeOf::new_left(BorrowedWriteSegm::with_limit(
            self, max_len,
        )))
    }
}


#[cfg(test)]
mod tests_ {
    use super::*;

    /// 验证 `&[T]` 作为读源时，段被回收后会像 `std::io::Read for &[u8]` 一样
    /// 推进调用方持有的切片引用。
    /// - 手段：以 `b"hello"` 构造 `&[u8]`，经 `try_read` 借出段并搬走 2 个
    ///   元素，随后依次 drop 子段与父段。
    /// - 判断：`data` 应推进为 `b"llo"`（而非仍指向 `b"hello"`），且
    ///   `consumer_state` 报告的长度非零、未关闭。
    #[test]
    fn read_borrowed_slice_advances_like_std() {
        let mut data: &[u8] = b"hello";

        let demand = Demand::no_more_than(5);
        let mut segm = data
            .try_read(&demand)
            .pick_left()
            .expect("read should return a segment");
        let mut child = segm.as_segm_ref();
        let mut dst = [MaybeUninit::<u8>::uninit(); 2];
        let n = unsafe { child.move_items_to_buff(&mut dst) };
        assert_eq!(n, 2);
        drop(child);
        drop(segm);

        assert_eq!(data, b"llo");
        assert!(!data.consumer_state().is_none_or(|(c, b)| c == 0 && b));
    }

    /// 验证 `&mut [T]` 作为读源时，段被回收后同样推进调用方的可变切片引用。
    /// - 手段：以 `[1, 2, 3, 4]` 构造 `&mut [u8]`，经 `try_read` 借出段并搬走
    ///   前 3 个元素，随后依次 drop 子段与父段。
    /// - 判断：`data` 应只剩下第 4 个元素 `[4]`。
    #[test]
    fn read_borrowed_mut_slice_advances() {
        let mut storage = [1u8, 2, 3, 4];
        let mut data: &mut [u8] = &mut storage;

        let demand = Demand::no_more_than(4);
        let mut segm = data
            .try_read(&demand)
            .pick_left()
            .expect("read should return a segment");
        let mut child = segm.as_segm_ref();
        let mut dst = [MaybeUninit::<u8>::uninit(); 3];
        let n = unsafe { child.move_items_to_buff(&mut dst) };
        assert_eq!(n, 3);
        drop(child);
        drop(segm);

        assert_eq!(data, &[4u8][..]);
    }

    /// 验证 `&mut [T]` 作为写目标时，段被回收后会像
    /// `std::io::Write for &mut [u8]` 一样推进调用方的可变切片引用。
    /// - 手段：以 5 字节零数组构造 `&mut [u8]`，经 `try_write` 借出段并写入
    ///   `b"abc"`，随后依次 drop 子段与父段。
    /// - 判断：`data` 应推进为剩余的两个未写槽位 `[0, 0]`，同时底层 `storage`
    ///   的前 3 字节应为 `b"abc"`（证明写入确实落到了原存储上）。
    #[test]
    fn write_borrowed_mut_slice_advances_like_std() {
        let mut storage = [0u8; 5];
        {
            let mut data: &mut [u8] = &mut storage;
            let demand = Demand::no_more_than(5);
            let mut segm = data
                .try_write(&demand)
                .pick_left()
                .expect("write should return a segment");
            let mut child = segm.as_segm_mut();
            let src = [
                MaybeUninit::new(b'a'),
                MaybeUninit::new(b'b'),
                MaybeUninit::new(b'c'),
            ];
            let n = unsafe { child.move_items_from_buff(&src.clone()) };
            assert_eq!(n, 3);
            drop(child);
            drop(segm);

            // `&mut [u8]` advances past the written prefix, exactly like
            // `std::io::Write for &mut [u8]`.
            assert_eq!(data, [0u8, 0]);
        }
        assert_eq!(&storage[..3], b"abc");
    }

    /// 验证空需求（`less_than(0)`）会被拒绝，而不是被当成"至少 0 个、无上界"借出整段。
    /// - 测试目标：空需求的取值集合是空集，任何可用量都满足不了它；它的 `min()` /
    ///   `max()` 都是 `None`，若只按 `unwrap_or(0)` + `with_limit(None)` 处理，就会把一个
    ///   不可满足的需求变成"整段都借出去"。
    /// - 测试手段：`&[u8]` 读源与 `&mut [u8]` 写目标各传入 `Demand::less_than(0)`，
    ///   取返回结果的右侧错误；随后再传一个正常需求 `no_more_than(1)` 作对照。
    /// - 判定标准：两次都得到 `BorrowedSliceError::Empty(Closing)`；对照用的正常需求
    ///   仍能借出段（证明报错来自"空集"这一语义，而不是通路失灵）。
    #[test]
    fn empty_demand_is_rejected_by_borrowed_slice() {
        let demand = Demand::less_than(0);
        assert!(demand.is_empty(), "less_than(0) 是空集");
        assert_eq!((demand.min(), demand.max()), (Option::None, Option::None));

        let mut data: &[u8] = b"hello";
        let err = data
            .try_read(&demand)
            .pick_right()
            .expect("空需求必须报错，而不是借出整段");
        assert!(matches!(err, BorrowedSliceError::Empty(ReadErrTag::Closing)));

        let mut storage = [0u8; 4];
        let mut sink: &mut [u8] = &mut storage;
        let err = sink
            .try_write(&demand)
            .pick_right()
            .expect("空需求必须报错，而不是借出整段");
        assert!(matches!(err, BorrowedSliceError::Empty(WriteErrTag::Closing)));

        // 对照：非空需求照常借出段。
        let ok = Demand::no_more_than(1);
        assert!(data.try_read(&ok).pick_left().is_some());
    }
}
