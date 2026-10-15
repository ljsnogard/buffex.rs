//! 写侧适配器：[`IrohWriter`]。
//!
//! iroh 的 [`SendStream`] 本身就是 tokio 的 `AsyncWrite`（其 `poll_shutdown` 正是
//! `SendStream::finish`），因此本模块不再自己实现「环 → 设备」的搬运泵，而是把
//! `SendStream` 直接交给 [`buffex_tokio_adapt::BuffWrite`]（本 workspace 内已迁移到
//! `buffex::ring` 的通用适配器），自己只做**薄封装**：
//!
//! * 把 [`TrBuffWrite`] / [`TrBuffTryWrite`] 原样转发给内层；
//! * 保留异步形状的构造入口 [`IrohWriter::try_new`]（可装载取消令牌）与收尾入口
//!   [`IrohWriter::close_async`]；
//! * 经 [`IrohWriter::take_error`] 取回设备侧的写错误。
//!
//! # 无后台任务
//!
//! 本适配器**不 spawn 任何任务**，走的是 [`BuffWrite`] 的「按需驱动」形态：搬运是
//! **借段的副作用**——`try_write` 先同步冲一轮「环 → 设备」再借出空段，`write_async`
//! 在等空间的过程中持续推进。
//!
//! # 段 drop 只提交进环，不等于送达设备
//!
//! 写入的段 drop 时只把数据**提交进环**（`advance_write`）；真正写进 QUIC 流由搬运
//! 完成。因此调用方在收尾时**必须** await 一次 [`IrohWriter::close_async`]：它先冲刷
//! 环内残留数据，再 `shutdown` 设备——对 iroh 的 `SendStream` 来说就是 `finish()`，
//! 对端读侧由此看到 EOF。少了这一步，最后一段会留在环里。

use core::marker::PhantomData;
use std::io;

use iroh::endpoint::SendStream;

use buffex_tokio_adapt::{
    BuffWrite, BuffWriteOpAsync, DefaultAllocConfig, InvalidCapacity, RingBufOf, SharedRingOf,
    x_deps::{abs_buff, buffex},
};

use abs_buff::{
    Demand, TrBuffTryWrite, TrBuffWrite,
    error::{TaggedError, WriteErrTag},
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use anylr::SomeOf;
use buffex::{
    ring::{ProducerError, RingSegmMut},
    x_deps::{abs_cancel, anylr, gen_mcf2},
};
use gen_mcf2::gen_may_cancel_future;

/// 写端设备错误的类型：网络写错误 + 它的 [`WriteErrTag`]。
pub type StreamOutputErr = TaggedError<io::Error, WriteErrTag>;

/// 写端借出的段类型（内层环的写段）。
///
/// 段 drop 时按**已写入量**提交进环；未写入的部分不计入，因此不丢数据。
pub type IrohWriterSegmMut<'f> = RingSegmMut<'f, RingBufOf<DefaultAllocConfig>, u8>;

/// 写端 [`TrBuffWrite::write_async`] 的返回类型。
///
/// 可直接 `.await`（不可取消），也可先
/// [`may_cancel_with`](abs_cancel::TrMayCancel::may_cancel_with) 再 `.await`。
pub type IrohWriterWriteAsync<'f> =
    BuffWriteOpAsync<'f, 'f, SendStream, DefaultAllocConfig, SharedRingOf<DefaultAllocConfig>>;

/// 写半部（写端适配器）of a buffered iroh stream。
///
/// 实现 [`TrBuffWrite`] / [`TrBuffTryWrite`]，把 QUIC 发送流当「设备」接上 `buffex` 环：
/// 环的写段借给调用方，环的读段由内层适配器在按需驱动时搬给流。
///
/// # Examples
///
/// ```no_run
/// use abs_buff::{Demand, TrBuffWrite};
/// use buffex_iroh::IrohWriter;
/// use iroh::endpoint::SendStream;
///
/// # async fn demo(stream: SendStream) {
/// // 构造是异步形状的：可直接 await，也可先 `may_cancel_with(token)`。
/// let mut writer = IrohWriter::try_new(stream, 4096)
///     .await
///     .expect("容量须在环允许的范围内");
/// let demand = Demand::at_least(1);
/// let some = TrBuffWrite::write_async(&mut writer, &demand).await;
/// // 段是**借**出来的：`let _` 立刻丢弃它（按已写入量提交进环），才能再借 `writer`。
/// let _ = some.pick_left();
/// // 收尾必须 await：先冲刷环内残留数据，再 finish() 流。
/// writer.close_async().await;
/// # }
/// ```
pub struct IrohWriter {
    tx_: BuffWrite<SendStream, DefaultAllocConfig>,
}

impl IrohWriter {
    /// 把一个 QUIC 发送流包装成 `cap` 字节的缓冲写端。
    ///
    /// 返回的 future 可直接 `.await`（不可取消），也可先
    /// [`may_cancel_with`](abs_cancel::TrMayCancel::may_cancel_with) 再 `.await`。
    /// 构造本身不 spawn 任何任务。
    ///
    /// # Errors
    ///
    /// `cap` 不在环允许的容量区间内时返回 `Err(cap)`，内层适配器的
    /// [`InvalidCapacity`] 在此摊平成被拒绝的容量本身。
    pub fn try_new(stream: SendStream, cap: usize) -> IrohWriterTryNewAsync<'static, 'static> {
        IrohWriterTryNewAsync::new(&PhantomData, stream, cap)
    }

    /// 取回最近一次设备的写错误及其标签（取走即清空）。
    ///
    /// 设备错误不会立刻炸掉调用方：搬运停止、错误暂存，之后借段一律给
    /// [`ProducerError::Closing`]，详情经本方法取回。
    pub fn take_error(&mut self) -> Option<StreamOutputErr> {
        // 内层把设备错误存成 `(io::Error, WriteErrTag)` 二元组，这里还原成带标签的错误。
        self.tx_.take_error().map(Into::into)
    }

    /// 借出可写段（try-once）。
    ///
    /// 先同步冲一轮「环 → 设备」（try-once，设备此刻收不下就算了），再尝试借出空段；
    /// 环满返回 [`ProducerError::Stuffed`]，写方向已关闭返回 [`ProducerError::Closing`]。
    pub fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<IrohWriterSegmMut<'f>, ProducerError<usize>> {
        self.tx_.try_write(demand)
    }

    /// 借出可写段（异步）：环满时先推进「环 → 设备」的搬运，再等生产端腾出空间。
    pub fn write_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> IrohWriterWriteAsync<'f> {
        self.tx_.write_async(demand)
    }

    /// 冲刷环内残留数据并 `shutdown` 设备（关闭写方向）。
    ///
    /// 对 iroh 的 `SendStream` 来说，`shutdown` 就是 `finish()`：对端读侧由此看到 EOF。
    /// 此后 [`try_write`](IrohWriter::try_write) 一律返回 [`ProducerError::Closing`]。
    ///
    /// 返回 `true` 表示干净关闭；`false` 表示环里还有数据没送出去或设备收尾出错——
    /// 具体原因经 [`take_error`](IrohWriter::take_error) 取回（内层已把设备错误记进
    /// 错误槽）。
    pub fn close_async(&mut self) -> IrohWriterCloseAsync<'_, '_> {
        IrohWriterCloseAsync::new(self)
    }
}

impl TrBuffTryWrite<u8> for IrohWriter {
    type SegmMut<'f>
        = IrohWriterSegmMut<'f>
    where
        Self: 'f;

    type Err = ProducerError<usize>;

    #[inline]
    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        IrohWriter::try_write(self, demand)
    }
}

impl TrBuffWrite<u8> for IrohWriter {
    type WriteAsync<'f>
        = IrohWriterWriteAsync<'f>
    where
        Self: 'f;

    #[inline]
    fn write_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::WriteAsync<'f> {
        IrohWriter::write_async(self, demand)
    }
}

/// [`IrohWriter::try_new`] 的实现：建好内层适配器即完成，无需 await 任何设备操作。
// `'f` 只是宏要求的**显式**生命周期（生成的 future 类型按它持有对入参的借用）；
// 这里没有任何真实借用，所以用 `PhantomData` 承载它，并按 clippy 的提示放行。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(IrohWriterTryNew, pub, new(pub(crate)))]
async fn iroh_writer_try_new_impl_<'f, C>(
    _marker: &'f PhantomData<()>,
    stream: SendStream,
    cap: usize,
    _cancel: C,
) -> Result<IrohWriter, usize>
where
    C: TrCancellationToken,
{
    let tx = BuffWrite::<SendStream, DefaultAllocConfig>::try_new(stream, cap, DefaultAllocConfig)
        .map_err(|InvalidCapacity(rejected)| rejected)?;
    Result::Ok(IrohWriter { tx_: tx })
}

/// [`IrohWriter::close_async`] 的实现：冲净环内数据后 `shutdown` 设备。
///
/// 设备错误由内层记进错误槽（经 [`IrohWriter::take_error`] 取回），因此这里只回报
/// 「是否干净关闭」。
// `'f` 是宏要求的**显式**生命周期（生成的 future 类型按它持有对被适配器的借用），
// 不能省略，见 `gen_mcf2` 的使用约定。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(IrohWriterClose, pub, new(pub(crate)))]
async fn iroh_writer_close_async_<'f, C>(writer: &'f mut IrohWriter, cancel: C) -> bool
where
    C: TrCancellationToken,
{
    writer
        .tx_
        .close_async()
        .may_cancel_with(cancel)
        .await
        .is_ok()
}
