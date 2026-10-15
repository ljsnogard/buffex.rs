//! 主动端设备：把 iroh 的 [`RecvStream`] / [`SendStream`] 包装为
//! `circular_buff` 的主动生产端 / 主动消费端设备（[`TrInput`] / [`TrOutput`]）。
//!
//! 传输层复用 `abs_buff_tokio_adapt` 的 [`ReadAsInput`] / [`WriteAsOutput`]
//! （tokio `AsyncRead` / `AsyncWrite` 适配器）；EOF 与网络错误的跟踪经
//! `gen_may_cancel_future` 宏生成的包装 future 完成——不手写展开形式的
//! `XxxAsync` / `XxxFuture`。

use std::{
    mem::MaybeUninit,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::sync::Mutex;

use iroh::endpoint::{RecvStream, SendStream};

use abs_buff::{
    error::{ReadErrTag, TaggedError, WriteErrTag},
    io::{TrInput, TrOutput},
};
use abs_buff_tokio_adapt::{ReadAsInput, WriteAsOutput};
use abs_cancel::{TrMayCancel, TrCancellationToken};
use anylr::{SomeOf, some_of::SomeLR};
use buffex::x_deps::{abs_buff, abs_cancel, anylr, gen_mcf2};
use gen_mcf2::gen_may_cancel_future;

/// 输入设备的错误类型：tokio 读错误（`ReadErrTag::Propagated`）。
pub type StreamInputErr = TaggedError<std::io::Error, ReadErrTag>;

/// 输出设备的错误类型：tokio 写错误（`WriteErrTag::Propagated`）。
pub type StreamOutputErr = TaggedError<std::io::Error, WriteErrTag>;

/// 主动生产端设备：包装一个 iroh [`RecvStream`]，经 tokio `AsyncRead` 适配。
///
/// 读到 0（tokio EOF 约定）时置共享 `eof` 标志；网络错误记入共享 `err`（泵
/// 把设备错误当作「本轮无数据」，错误经 `Arc` 由调用方 `take_error` 取回）。
pub struct StreamInput {
    stream: RecvStream,
    eof: Arc<AtomicBool>,
    err: Arc<Mutex<Option<StreamInputErr>>>,
}

impl StreamInput {
    pub(super) fn new(
        stream: RecvStream,
        eof: Arc<AtomicBool>,
        err: Arc<Mutex<Option<StreamInputErr>>>,
    ) -> Self {
        StreamInput { stream, eof, err }
    }
}

impl TrInput<u8> for StreamInput {
    type ReadAsync<'f> = StreamInputReadAsync<'f, 'f> where Self: 'f;

    type Err = StreamInputErr;

    fn read_async<'f>(
        &'f mut self,
        target: &'f mut [MaybeUninit<u8>],
    ) -> Self::ReadAsync<'f> {
        StreamInputReadAsync::new(self, target)
    }
}

#[gen_may_cancel_future(StreamInputRead)]
async fn stream_input_read_impl_<'f, K>(
    input: &'f mut StreamInput,
    target: &'f mut [MaybeUninit<u8>],
    cancel: K,
) -> SomeOf<usize, StreamInputErr>
where
    K: TrCancellationToken,
{
    // 传输经 abs_buff_tokio_adapt 的 AsyncRead 适配器（宏生成的 future）。
    let x = ReadAsInput::new(&mut input.stream)
        .read_async(target)
        .may_cancel_with(cancel)
        .await;
    match x.into_inner() {
        SomeLR::Left(n) => {
            // tokio `AsyncRead` 语义：读到 0 = 流结束（EOF）。
            if n == 0 {
                input.eof.store(true, Ordering::Release);
            }
            SomeOf::new_left(n)
        }
        SomeLR::Right(err) => {
            // 泵会把设备错误当作「本轮无数据」——记入共享 Arc 供 take_error。
            let mut guard = input.err.lock().await;
            *guard = Some(err);
            SomeOf::new_left(0)
        }
        SomeLR::Both(_, err) => {
            let mut guard = input.err.lock().await;
            *guard = Some(err);
            SomeOf::new_left(0)
        }
    }
}

/// 主动消费端设备：包装 iroh [`SendStream`]，经 tokio `AsyncWrite` 适配。
///
/// 流经 `Arc<Mutex<Option<…>>>` 与写半部共享——`IrohWriter::shutdown` 需取回
/// 流执行 `finish()`（对端读侧由此看到 EOF）；流被取回后写入视为 0（不再写）。
/// 网络错误记入共享 `err`（泵当作「本轮不能接收」，经 `Arc` 取回）。
pub struct StreamOutput {
    stream: Arc<Mutex<Option<SendStream>>>,
    err: Arc<Mutex<Option<StreamOutputErr>>>,
}

impl StreamOutput {
    pub(super) fn new(
        stream: Arc<Mutex<Option<SendStream>>>,
        err: Arc<Mutex<Option<StreamOutputErr>>>,
    ) -> Self {
        StreamOutput { stream, err }
    }
}

impl TrOutput<u8> for StreamOutput {
    type WriteAsync<'f> = StreamOutputWriteAsync<'f, 'f> where Self: 'f;
    type Err = StreamOutputErr;

    fn write_async<'f>(
        &'f mut self,
        source: &'f [MaybeUninit<u8>],
    ) -> Self::WriteAsync<'f> {
        StreamOutputWriteAsync::new(self, source)
    }
}

#[gen_may_cancel_future(StreamOutputWrite)]
async fn stream_output_write_impl_<'f, K>(
    output: &'f mut StreamOutput,
    source: &'f [MaybeUninit<u8>],
    cancel: K,
) -> SomeOf<usize, StreamOutputErr>
where
    K: TrCancellationToken,
{
    // 从共享槽位取出发送流，避免把 std MutexGuard 持有到 await 之后
    // （MutexGuard 不是 Send，会导致写 future 无法跨线程发送）。
    // shutdown 取回后为 None → 写入 0，不再搬数据。
    let mut guard = output.stream.lock().await;
    let Option::Some(stream) = &mut *guard else {
        let err = std::io::Error::other("Stream missing");
        return SomeOf::new_right(StreamOutputErr::new(err, WriteErrTag::Unknown))
    };
    // 传输经 abs_buff_tokio_adapt 的 AsyncWrite 适配器（宏生成的 future）。
    let x = WriteAsOutput::new(stream)
        .write_async(source)
        .may_cancel_with(cancel)
        .await;
    // 写完后把流放回共享槽位，供下一次 write / shutdown 使用。
    // *output.stream.lock().unwrap() = Some(stream);
    match x.into_inner() {
        SomeLR::Left(n) => SomeOf::new_left(n),
        SomeLR::Right(err) => {
            // 泵会把设备错误当作「本轮不能接收」——记入共享 Arc 供 take_error。
            let mut guard = output.err.lock().await;
            *guard = Some(err);
            SomeOf::new_left(0)
        }
        SomeLR::Both(_, err) => {
            let mut guard = output.err.lock().await;
            *guard = Some(err);
            SomeOf::new_left(0)
        }
    }
}
