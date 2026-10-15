//! 读侧适配器：[`IrohReader`]。
//!
//! iroh 的 [`RecvStream`] 本身就是 tokio 的 `AsyncRead`，因此本模块不再自己实现
//! 「设备 → 环」的搬运泵，而是把 `RecvStream` 直接交给
//! [`buffex_tokio_adapt::BuffRead`]（本 workspace 内已迁移到 `buffex::ring` 的通用
//! 适配器），自己只做**薄封装**：
//!
//! * 把 [`TrBuffRead`] / [`TrBuffTryRead`] / [`TrConsumerState`] 原样转发给内层；
//! * 保留异步形状的构造入口 [`IrohReader::try_new`]（可装载取消令牌）与收尾入口
//!   [`IrohReader::close_async`]；
//! * 经 [`IrohReader::take_error`] 取回设备侧的读错误。
//!
//! # 无后台任务
//!
//! 本适配器**不 spawn 任何任务**：网络数据只在用户操作时被拉取——`try_read` 先同步推
//! 一轮「设备 → 环」（try-once，设备此刻没数据就返回 `Drained`），`read_async` 在等环里
//! 出现数据的过程中持续推进。这正是 [`BuffRead`] 的「按需驱动」形态。
//!
//! # EOF 与错误
//!
//! `RecvStream` 读到 0（tokio 的 EOF 约定）会被 `abs_buff` 的 tokio 设备适配层翻成带
//! `Closing` 标签的错误，[`BuffRead`] 据此置「设备已结束」标记：环内已搬入的数据仍可
//! 读完，之后读取一律给 [`ConsumerError::Closing`]。真正的 IO 错误则被暂存，经
//! [`take_error`](IrohReader::take_error) 取回。

use core::marker::PhantomData;
use std::io;

use iroh::endpoint::RecvStream;

use buffex_tokio_adapt::{
    BuffRead, BuffReadOpAsync, DefaultAllocConfig, InvalidCapacity, RingBufOf, SharedRingOf,
    x_deps::{abs_buff, buffex},
};

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead,
    buffer::TrConsumerState,
    error::{ReadErrTag, TaggedError},
};
use abs_cancel::TrCancellationToken;
use anylr::SomeOf;
use buffex::{
    ring::{ConsumerError, RingSegmRef},
    x_deps::{abs_cancel, anylr, gen_mcf2},
};
use gen_mcf2::gen_may_cancel_future;

/// 读端设备错误的类型：网络读错误 + 它的 [`ReadErrTag`]。
pub type StreamInputErr = TaggedError<io::Error, ReadErrTag>;

/// 读端借出的段类型（内层环的读段）。
///
/// 段只在**消费推进**时提交：`iter_slices` 只是窥视，
/// `move_items_to_buff` / `move_items_into_output_async` 搬出多少才消费多少。
pub type IrohReaderSegmRef<'f> = RingSegmRef<'f, RingBufOf<DefaultAllocConfig>, u8>;

/// 读端 [`TrBuffRead::read_async`] 的返回类型。
///
/// 可直接 `.await`（不可取消），也可先
/// [`may_cancel_with`](abs_cancel::TrMayCancel::may_cancel_with) 再 `.await`。
pub type IrohReaderReadAsync<'f> =
    BuffReadOpAsync<'f, 'f, RecvStream, DefaultAllocConfig, SharedRingOf<DefaultAllocConfig>>;

/// 读半部（读端适配器）of a buffered iroh stream。
///
/// 实现 [`TrBuffRead`] / [`TrBuffTryRead`]，把 QUIC 接收流当「设备」接上 `buffex` 环：
/// 环的读段借给调用方，环的写段由内层适配器在按需驱动时填充。
///
/// # Examples
///
/// ```no_run
/// use abs_buff::{Demand, TrBuffRead};
/// use buffex_iroh::IrohReader;
/// use iroh::endpoint::RecvStream;
///
/// # async fn demo(stream: RecvStream) {
/// // 构造是异步形状的：可直接 await，也可先 `may_cancel_with(token)`。
/// let mut reader = IrohReader::try_new(stream, 4096)
///     .await
///     .expect("容量须在环允许的范围内");
/// let demand = Demand::at_least(1);
/// let some = TrBuffRead::read_async(&mut reader, &demand).await;
/// // 段是**借**出来的：`let _` 立刻丢弃它（按已搬出量提交），才能再借 `reader`。
/// let _ = some.pick_left();
/// reader.close_async().await;
/// # }
/// ```
pub struct IrohReader {
    rx_: BuffRead<RecvStream, DefaultAllocConfig>,
}

impl IrohReader {
    /// 把一个 QUIC 接收流包装成 `cap` 字节的缓冲读端。
    ///
    /// 返回的 future 可直接 `.await`（不可取消），也可先
    /// [`may_cancel_with`](abs_cancel::TrMayCancel::may_cancel_with) 再 `.await`。
    /// 构造本身不 spawn 任何任务，也不拉取网络数据——第一轮拉取发生在第一次
    /// `try_read` / `read_async` 时。
    ///
    /// # Errors
    ///
    /// `cap` 不在环允许的容量区间内时返回 `Err(cap)`，内层适配器的
    /// [`InvalidCapacity`] 在此摊平成被拒绝的容量本身。
    pub fn try_new(stream: RecvStream, cap: usize) -> IrohReaderTryNewAsync<'static, 'static> {
        IrohReaderTryNewAsync::new(&PhantomData, stream, cap)
    }

    /// 取回最近一次设备的读错误及其标签（取走即清空）。
    ///
    /// 设备错误不会打断「把环里已有数据读完」：搬运停止、错误暂存，调用方读完环内数据
    /// 后看到 [`ConsumerError::Closing`]，再经本方法取回具体原因。
    pub fn take_error(&mut self) -> Option<StreamInputErr> {
        // 内层把设备错误存成 `(io::Error, ReadErrTag)` 二元组，这里还原成带标签的错误。
        self.rx_.take_error().map(Into::into)
    }

    /// 借出可读段（try-once）。
    ///
    /// 先同步推一轮「设备 → 环」（设备此刻没数据就算了），再尝试借出环内已有的段；
    /// 环空返回 [`ConsumerError::Drained`]，设备已结束且环已取空时返回
    /// [`ConsumerError::Closing`]。
    pub fn try_read<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<IrohReaderSegmRef<'f>, ConsumerError<usize>> {
        self.rx_.try_read(demand)
    }

    /// 借出可读段（异步）：在等环里出现数据的过程中持续推进设备 → 环的搬运。
    pub fn read_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> IrohReaderReadAsync<'f> {
        self.rx_.read_async(demand)
    }

    /// 读端收尾。
    ///
    /// 读向没有「必须冲刷出去的残留数据」：设备 → 环的搬运完全由调用方按需推进，环与
    /// `RecvStream` 都随 [`IrohReader`] 的 drop 一起关闭（`RecvStream` 的 drop 会向对端
    /// 发 `STOP_SENDING`）。因此本方法只是一次**可 await 的空操作**，存在的意义是让读写
    /// 两端的收尾流程对称——调用方按同一套写法收尾即可，不必区分自己拿的是哪一端。
    pub fn close_async(&mut self) -> IrohReaderCloseAsync<'_, '_> {
        IrohReaderCloseAsync::new(self)
    }
}

impl TrConsumerState for IrohReader {
    #[inline]
    fn consumer_state(&self) -> Option<(usize, bool)> {
        self.rx_.consumer_state()
    }
}

impl TrBuffTryRead<u8> for IrohReader {
    type SegmRef<'f>
        = IrohReaderSegmRef<'f>
    where
        Self: 'f;

    type Err = ConsumerError<usize>;

    #[inline]
    fn try_read<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        IrohReader::try_read(self, demand)
    }
}

impl TrBuffRead<u8> for IrohReader {
    type ReadAsync<'f>
        = IrohReaderReadAsync<'f>
    where
        Self: 'f;

    #[inline]
    fn read_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::ReadAsync<'f> {
        IrohReader::read_async(self, demand)
    }
}

/// [`IrohReader::try_new`] 的实现：建好内层适配器即完成，无需 await 任何设备操作。
// `'f` 只是宏要求的**显式**生命周期（生成的 future 类型按它持有对入参的借用）；
// 这里没有任何真实借用，所以用 `PhantomData` 承载它，并按 clippy 的提示放行。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(IrohReaderTryNew, pub, new(pub(crate)))]
async fn iroh_reader_try_new_impl_<'f, C>(
    _marker: &'f PhantomData<()>,
    stream: RecvStream,
    cap: usize,
    _cancel: C,
) -> Result<IrohReader, usize>
where
    C: TrCancellationToken,
{
    let rx = BuffRead::<RecvStream, DefaultAllocConfig>::try_new(stream, cap, DefaultAllocConfig)
        .map_err(|InvalidCapacity(rejected)| rejected)?;
    Result::Ok(IrohReader { rx_: rx })
}

/// [`IrohReader::close_async`] 的实现：读向没有待冲刷数据，因此是空操作
/// （关闭发生在 [`IrohReader`] 被 drop 时，见方法文档）。
// `'f` 是宏要求的**显式**生命周期（生成的 future 类型按它持有对被适配器的借用），
// 不能省略，见 `gen_mcf2` 的使用约定。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(IrohReaderClose, pub, new(pub(crate)))]
async fn iroh_reader_close_async_<'f, C>(_reader: &'f mut IrohReader, _cancel: C) -> ()
where
    C: TrCancellationToken,
{
}
