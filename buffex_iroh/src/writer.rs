//! 写侧适配器：[`IrohWriter`]。
//!
//! 基于 `circular_buff`：`SendStream` 作为**主动消费端**（设备见
//! [`super::device::StreamOutput`]，同步阻塞写），用户经被动生产端半部以
//! [`TrBuffTryWrite`] / [`TrBuffWrite`] 生产。
//!
//! # 无后台任务模型
//!
//! 不 spawn 任何任务。数据搬运发生在**提交路径**上：用户写入的段 drop 时，
//! hook 同步泵把数据写进 QUIC 流（阻塞写保证送达）。`shutdown` 关闭写端
//! （触发泵冲刷剩余数据）后取回流执行 `finish()`（对端读侧由此看到 EOF）。

use std::{
    marker::PhantomData,
    mem::MaybeUninit,
    sync::Arc,
};

use tokio::sync::Mutex;
use iroh::endpoint::SendStream;

use abs_buff::{
    Demand, TrBuffWrite, TrBuffTryWrite,
    gen_may_cancel_future,
};
// `abs_buff` 及其底层依赖（`abs_cancel`）经 `abs_buff_tokio_adapt::x_deps`
// 再导出，无需在 Cargo.toml 重复声明；`buffex` 是直接依赖。
use abs_buff_tokio_adapt::x_deps::abs_buff;
use anylr::SomeOf;
use buffex::{
    circular_buff::{
        CoreAlloc, DevConsumer, Producer,
        builder::CircularBuffBuilder,
    },
    x_deps::{abs_cancel, mm_ptr},
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use mm_ptr::Owned;

use super::{
    device::{StreamOutput, StreamOutputErr},
};

/// 内部缓冲的生产端半部（被动生产端 = 调用者驱动）。
type Inner = Producer<
    DevConsumer<StreamOutput, u8>,
    Owned<[MaybeUninit<u8>], CoreAlloc>,
    u8, CoreAlloc,
>;

/// 写半部（写端适配器）of a buffered iroh stream。
///
/// 实现 [`TrBuffWrite`] / [`TrBuffTryWrite`]，底层是 `circular_buff` 的被动
/// 生产端半部。
pub struct IrohWriter {
    tx: Inner,
    /// 共享的输出流：`shutdown` 时取回执行 `finish()`。
    stream: Arc<Mutex<Option<SendStream>>>,
    /// 最近一次网络写错误（经 [`IrohWriter::take_error`] 取回）。
    err: Arc<Mutex<Option<StreamOutputErr>>>,
}

impl IrohWriter {
    /// 把一个 QUIC 发送流包装成 `cap` 字节的缓冲写端。
    ///
    /// 不 spawn 任何任务；写入的数据在段 drop 时被同步泵搬运到流。
    pub fn try_new(stream: SendStream, cap: usize) -> IrohWriterTryNewAsync<'static> {
        IrohWriterTryNewAsync(&PhantomData, stream, cap)
    }

    /// Report the last network write error, if any.
    pub async fn take_error(&self) -> Option<StreamOutputErr> {
        self.err.lock().await.take()
    }

    /// Flush buffered data, finish the QUIC stream.
    ///
    /// 关闭写端 → 触发消费端 hook → 泵同步冲刷剩余数据（阻塞写）→ 取回流
    /// 执行 `finish()`，对端读侧由此看到 EOF。
    pub fn close_async(&mut self) -> IrohWriterCloseAsync<'_> {
        IrohWriterCloseAsync(self)
    }
}

impl Drop for IrohWriter {
    fn drop(&mut self) {
        let t = self.close_async().into_future();
        abs_art_bridge::Runtime::block_on(async move {
            t.await;
        });
    }
}

#[gen_may_cancel_future(IrohWriterTryNew)]
async fn iroh_writer_try_new_impl_<'f, C>(
    _marker: &'f PhantomData<()>,
    stream: SendStream,
    cap: usize,
    cancel: &'f mut C,
) -> Result<IrohWriter, usize>
where
    C: TrCancellationToken + Clone,
{
    let stream = Arc::new(Mutex::new(Some(stream)));
    let err = Arc::new(Mutex::new(None));
    let output = StreamOutput::new(stream.clone(), err.clone());
    let Result::Ok(builder) = CircularBuffBuilder::with_capacity(cap)
    else {
        return Result::Err(cap);
    };
    let mut ready = builder
        .producer_passive()
        .pipe_into_output(output);
    let tx = ready
        .build_async()
        .may_cancel_with(cancel)
        .await
        .map_err(|_| cap)?;
    Result::Ok(IrohWriter { tx, stream, err })
}

#[gen_may_cancel_future(IrohWriterClose)]
async fn iroh_writer_close_async_<'f, C>(
    writer: &'f mut IrohWriter,
    cancel: &'f mut C,
) -> bool
where
    C: TrCancellationToken + Clone,
{
    // 异步关闭：设置 TX_CLOSED 后持续排空残留数据，直到缓冲清空或被取消。
    writer.tx.close_async().may_cancel_with(cancel).await;
    let stream = writer.stream.lock().await.take();
    if let Some(mut stream) = stream {
        let _ = stream.finish();
        return true;
    }
    false
}

impl TrBuffWrite<u8> for IrohWriter {
    type WriteAsync<'f> = <Inner as TrBuffWrite<u8>>::WriteAsync<'f> where Self: 'f;

    type SegmMut<'f> = <Inner as TrBuffWrite<u8>>::SegmMut<'f> where Self: 'f;

    type Err = <Inner as TrBuffWrite<u8>>::Err;

    fn is_stuffed_closing(&self) -> bool {
        self.tx.is_stuffed_closing()
    }

    fn write_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> Self::WriteAsync<'f> {
        <Inner as TrBuffWrite<u8>>::write_async(&mut self.tx, demand)
    }
}

impl TrBuffTryWrite<u8> for IrohWriter {
    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        <Inner as TrBuffTryWrite<u8>>::try_write(&mut self.tx, demand)
    }
}
