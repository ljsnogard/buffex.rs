//! 读侧适配器：[`IrohReader`]。
//!
//! 基于 `circular_buff`：`RecvStream` 作为**主动生产端**（设备见
//! [`super::device::StreamInput`]，try-once 读），用户经被动消费端半部以
//! [`TrBuffTryRead`] / [`TrBuffRead`] 消费。
//!
//! # 无后台任务模型
//!
//! 不 spawn 任何任务。网络数据只在用户操作时被拉取：
//!
//! * [`TrBuffTryRead::try_read`] 先显式 `drive()` 拉一轮（try-once、非阻塞），
//!   再借出缓冲段；当前确无数据则返回 `Drained`（正确的 try 语义）；
//! * [`TrBuffRead::read_async`] 返回一个循环 future：反复 `drive()` + 尝试，
//!   数据未到则 `yield` 让运行时处理网络，直到有数据 / EOF / 错误；
//! * 流结束（EOF）或读错误经共享状态合成 [`RxError::Closing`]（错误详情经
//!   [`IrohReader::take_error`] 取回）。

use core::marker::PhantomData;

use std::{
    mem::MaybeUninit,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::sync::Mutex;

use iroh::endpoint::RecvStream;

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead, gen_may_cancel_future,
};
// `abs_buff` 及其底层依赖（`abs_cancel`）经 `abs_buff_tokio_adapt::x_deps`
// 再导出，无需在 Cargo.toml 重复声明；`buffex` 是直接依赖。
use abs_buff_tokio_adapt::x_deps::abs_buff;
use abs_cancel::{TrCancellationToken, TrMayCancel};
use anylr::SomeOf;
use buffex::{
    circular_buff::{
        Consumer, CoreAlloc, DevProducer, ConsumerError,
        builder::CircularBuffBuilder,
    },
    x_deps::{abs_cancel, mm_ptr},
};
use mm_ptr::Owned;

use super::{
    device::{StreamInput, StreamInputErr},
};

/// 内部缓冲的消费端半部（被动消费端 = 调用者驱动）。
type Inner = Consumer<
    DevProducer<StreamInput, u8>,
    Owned<[MaybeUninit<u8>], CoreAlloc>,
    u8, CoreAlloc>;

/// 读半部（读端适配器）of a buffered iroh stream。
///
/// 实现 [`TrBuffRead`] / [`TrBuffTryRead`]，底层是 `circular_buff` 的被动
/// 消费端半部。
pub struct IrohReader {
    rx: Inner,
    /// 输入设备是否已到 EOF（或出错）：之后缓冲读空即合成 `Closing`。
    eof: Arc<AtomicBool>,
    /// 最近一次网络读错误（经 [`IrohReader::take_error`] 取回）。
    err: Arc<Mutex<Option<StreamInputErr>>>,
}

impl IrohReader {
    /// 把一个 QUIC 接收流包装成 `cap` 字节的缓冲读端。
    ///
    /// 构造即 `drive()` 拉一轮（有数据则入缓冲，无则立即返回，不阻塞）。
    /// 不 spawn 任何任务。
    pub fn try_new(stream: RecvStream, cap: usize) -> IrohReaderTryNewAsync<'static> {
        IrohReaderTryNewAsync(&PhantomData, stream, cap)
    }

    /// Report the last network read error, if any.
    pub async fn take_error(&self) -> Option<StreamInputErr> {
        self.err.lock().await.take()
    }

    /// Close the read side. 无后台任务可等待；数据流随后经 read 端报
    /// `Closing`（EOF）或 `Drained`。
    pub fn close_async(&mut self) -> IrohReaderCloseAsync<'_> {
        IrohReaderCloseAsync(self)
    }

    pub fn read_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> IrohReaderReadAsync<'f> {
        IrohReaderReadAsync(self, demand)
    }

    pub fn try_read<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<<Inner as TrBuffRead<u8>>::SegmRef<'f>, <Inner as TrBuffRead<u8>>::Err> {
        // 底层半部在对端（生产端）为主动时已自动 drive 拉一轮（try-once、
        // 非阻塞）——适配器无需手动驱动。
        let some = self.rx.try_read(demand);
        // EOF 合成：空 + 设备已 EOF → Closing（错误详情经 take_error 取回）。
        if let Some(ConsumerError::Drained(_)) = some.as_ref().pick_right()
            && self.eof.load(Ordering::Acquire)
        {
            return SomeOf::new_right(ConsumerError::Closing);
        }
        some
    }

    /// EOF 已到且缓冲已空：之后读取应报 `Closing`。
    fn eof_drained(&self) -> bool {
        self.eof.load(Ordering::Acquire) && self.rx.data_size() == 0
    }
}

impl Drop for IrohReader {
    fn drop(&mut self) {
        let t = self.close_async().into_future();
        abs_art_bridge::Runtime::block_on(async move {
            t.await;
        });
    }
}

impl TrBuffRead<u8> for IrohReader {
    type ReadAsync<'f> = IrohReaderReadAsync<'f> where Self: 'f;

    type SegmRef<'f> = <Inner as TrBuffRead<u8>>::SegmRef<'f> where Self: 'f;

    type Err = <Inner as TrBuffRead<u8>>::Err;

    fn is_drained_closing(&self) -> bool {
        self.rx.is_drained_closing() || self.eof_drained()
    }

    #[inline]
    fn read_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> Self::ReadAsync<'f> {
        IrohReader::read_async(self, demand)
    }
}

impl TrBuffTryRead<u8> for IrohReader {
    #[inline]
    fn try_read<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        IrohReader::try_read(self, demand)
    }
}

#[gen_may_cancel_future(IrohReaderTryNew)]
async fn iroh_reader_try_new_impl_<'f, C>(
    _marker: &'f PhantomData<()>,
    stream: RecvStream,
    cap: usize,
    cancel: &'f mut C,
) -> Result<IrohReader, usize>
where
    C: TrCancellationToken + Clone,
{
    let eof = Arc::new(AtomicBool::new(false));
    let err = Arc::new(Mutex::new(None));
    let input = StreamInput::new(stream, eof.clone(), err.clone());
    let Result::Ok(builder) = CircularBuffBuilder::with_capacity(cap) else {
        return Result::Err(cap);
    };
    let mut ready = builder
        .pipe_from_input(input)
        .consumer_passive();
    let rx = ready
        .build_async()
        .may_cancel_with(cancel)
        .await
        .map_err(|_| cap)?;
    Result::Ok(IrohReader { rx, eof, err })
}

#[gen_may_cancel_future(IrohReaderRead)]
async fn iroh_read_async_<'f, C>(
    reader: &'f mut IrohReader,
    demand: &'f Demand<usize>,
    cancel: &'f mut C,
) -> SomeOf<
    <Inner as TrBuffRead<u8>>::SegmRef<'f>,
    <Inner as TrBuffRead<u8>>::Err,
>
where
    C: TrCancellationToken + Clone,
{
    reader.rx
        .read_async(demand)
        .may_cancel_with(cancel)
        .await
}

#[gen_may_cancel_future(IrohReaderClose)]
async fn iroh_read_close_async_<'f, C>(
    reader: &'f mut IrohReader,
    cancel: &'f mut C,
) -> ()
where
    C: TrCancellationToken + Clone,
{
    reader.rx.close_async().may_cancel_with(cancel).await;
}
