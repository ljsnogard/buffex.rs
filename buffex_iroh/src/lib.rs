//! `TrBuffRead` / `TrBuffWrite` adapters over iroh QUIC streams.
//!
//! iroh's [`RecvStream`](iroh::endpoint::RecvStream) and
//! [`SendStream`](iroh::endpoint::SendStream) do not expose their internal buffers
//! through the `abs_buff` segment model, so this crate places a `buffex` ring between
//! the user and the QUIC stream:
//!
//! * [`IrohReader`] wraps a [`RecvStream`](iroh::endpoint::RecvStream) as the ring's
//!   **active producer**; the user consumes the buffered data through `TrBuffRead` /
//!   `TrBuffTryRead`.
//! * [`IrohWriter`] wraps a [`SendStream`](iroh::endpoint::SendStream) as the ring's
//!   **active consumer**; the user produces data through `TrBuffWrite` /
//!   `TrBuffTryWrite`.
//!
//! # 实现形态：薄封装
//!
//! iroh 的 `RecvStream` / `SendStream` 本身就是 tokio 的 `AsyncRead` /
//! `AsyncWrite`（`SendStream` 的 `poll_shutdown` 正是 `finish()`），因此本 crate
//! **不再自己实现「环 ↔ 设备」的搬运泵**，而是把设备直接交给
//! [`buffex_tokio_adapt`] 里已经按 `buffex::ring` 写好的通用适配器
//! [`BuffRead`](buffex_tokio_adapt::BuffRead) /
//! [`BuffWrite`](buffex_tokio_adapt::BuffWrite)，自己只负责：
//!
//! * 把 `abs_buff` 的读 / 写 trait 原样转发给内层；
//! * 提供与 iroh 相称的构造 / 收尾入口（异步形状，可装载取消令牌）。
//!
//! 早先的版本直接依赖已被删除的 `buffex::circular_buff`（`CircularBuffBuilder` /
//! `DevProducer` / `DevConsumer`），那些类型在 `buffex` 改写成 `buffex::ring` 之后
//! 不复存在；迁移后本 crate 不再持有自己的搬运逻辑。
//!
//! # 无后台任务模型（不 spawn）
//!
//! 本 crate **不 spawn 任何任务**：数据搬运由调用方**按需驱动**。语义如下：
//!
//! * **读**：网络数据只在用户操作时被拉取（try-once，非阻塞）——`try_read` 同步推
//!   一轮，`read_async` 在等环里出现数据的过程中持续推进；EOF / 错误被合成读端的
//!   `Closing`，错误详情经 [`IrohReader::take_error`] 取回；
//! * **写**：`try_write` / `write_async` 借段前先冲一轮「环 → 设备」，段 drop 只是把
//!   数据**提交进环**；收尾必须 await [`IrohWriter::close_async`]，它先冲刷残留数据
//!   再 `finish()` 流，对端读侧由此看到 EOF。
//!
//! 构造与操作必须在一个活跃的 tokio 运行时上下文内（QUIC 流的收发包由运行时驱动）。

#![feature(impl_trait_in_assoc_type)]

mod reader_;
mod writer_;

pub use reader_::{
    IrohReader, IrohReaderCloseAsync, IrohReaderReadAsync, IrohReaderSegmRef,
    IrohReaderTryNewAsync, StreamInputErr,
};
pub use writer_::{
    IrohWriter, IrohWriterCloseAsync, IrohWriterSegmMut, IrohWriterTryNewAsync,
    IrohWriterWriteAsync, StreamOutputErr,
};
