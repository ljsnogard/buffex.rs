//! Thin `TrBuffRead` / `TrBuffWrite` adapters over iroh QUIC streams.
//!
//! iroh's [`RecvStream`](iroh::endpoint::RecvStream) and [`SendStream`](iroh::endpoint::SendStream) do not expose their internal
//! buffers through the abs_buff segment model.  This crate places a
//! [`buffex::circular_buff`] buffer between the user and the QUIC stream:
//!
//! * [`IrohReader`] wraps a [`RecvStream`](iroh::endpoint::RecvStream) as the buffer's **active producer**;
//!   the user consumes the buffered data through [`TrBuffRead`](abs_buff::TrBuffRead) /
//!   [`TrBuffTryRead`](abs_buff::TrBuffTryRead).
//! * [`IrohWriter`] wraps a [`SendStream`](iroh::endpoint::SendStream) as the buffer's **active consumer**;
//!   the user produces data through [`TrBuffWrite`](abs_buff::TrBuffWrite) / [`TrBuffTryWrite`](abs_buff::TrBuffTryWrite).
//!
//! # 无后台任务模型（不 spawn）
//!
//! 本 crate **不 spawn 任何任务**：数据搬运由 `circular_buff` 的同步 hook 泵
//! 完成。语义如下：
//!
//! * **写**：用户写入的段 drop 提交时，泵同步（阻塞）把数据写进 QUIC 流——
//!   写入必然送达；`IrohWriter::shutdown` 冲刷剩余数据并 `finish()` 流；
//! * **读**：网络数据只在用户操作时被拉取（try-once，非阻塞）——半部在
//!   对端（生产端）为主动时，`try_read` / `read_async` **自动**先驱动一轮
//!   拉取，调用者无需关心；`read_async` 循环驱动 + yield 等待；
//! * 网络错误经 `take_error` 取回；EOF / 错误被合成读端的 `Closing`。
//!
//! 注意：由于不 spawn，**写路径会阻塞当前线程直到网络写入完成**（无任务模型
//! 的固有语义，见 `buffex::circular_buff::core_` 的文档）；构造与操作必须在
//! 一个活跃的 tokio 运行时上下文内（QUIC 流的收发包由运行时驱动）。

#![feature(impl_trait_in_assoc_type)]

pub mod device;
mod reader;
mod writer;

pub use reader::IrohReader;
pub use writer::IrohWriter;
