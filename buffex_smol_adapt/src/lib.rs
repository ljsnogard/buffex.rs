//! 把**任意** smol [`AsyncRead`](futures_lite::io::AsyncRead) /
//! [`AsyncWrite`](futures_lite::io::AsyncWrite) 设备接上 `buffex` 环，变成
//! [`TrBuffRead`](abs_buff::TrBuffRead) / [`TrBuffWrite`](abs_buff::TrBuffWrite)。
//!
//! # 它提供什么
//!
//! | 类型 | 方向 | 调用方拿到的 trait |
//! | --- | --- | --- |
//! | [`BuffRead`] | 设备 → 环 | `TrBuffRead` / `TrBuffTryRead` |
//! | [`BuffWrite`] | 环 → 设备 | `TrBuffWrite` / `TrBuffTryWrite` |
//! | [`ReadAsInput`] | 设备 → `TrInput` | `abs_buff` 的设备级抽象 |
//! | [`WriteAsOutput`] | `TrOutput` → 设备 | 同上 |
//!
//! 两个环适配器各自持有一个 `buffex` 环（`mm_ptr::Shared<Ring<…>>`，经
//! `Ring::split_unchecked` 拆成读写两半）与一台设备：调用方对着环按段读写，适配器负责
//! 设备与环之间的搬运。收发双工要的是**两个**适配器（各一个环），因为 ring 的安全前提是
//! SPSC。
//!
//! # 设备 trait 为什么是 `futures-lite`
//!
//! smol 自己不做 IO：`smol::io` 正是 [`futures_lite::io`] 的重导出，`async-net` /
//! `async-io` 的 `UnixStream`、`TcpStream` 等实现的也是这一族 trait。因此本 crate 的
//! 设备边界就是 [`futures_lite::io::AsyncRead`] /
//! [`futures_lite::io::AsyncWrite`]，`async_net::unix::UnixStream` 无需任何包装即可直接
//! 传入——本 crate 也因此**不依赖 smol 本体**（与 `buffex_tokio_adapt` 不依赖 tokio 同理）。
//!
//! # 驱动模型：两种，按上层协议栈的形状选
//!
//! | 形态 | 构造入口 | 谁在驱动「环 → 设备」 |
//! | --- | --- | --- |
//! | **按需** | [`BuffWrite::try_new`] | 调用方：`try_write` / `write_async` / `flush_async` / `close_async` 每次顺带推一轮 |
//! | **自驱动** | [`BuffWrite::try_new_local`] | 后台泵：构造时经 `abs_art-smol` 的 `LocalScope` 投到**本线程**队列 |
//!
//! 两者共用的接口语义：
//!
//! * [`BuffRead::try_read`] / [`BuffWrite::try_write`] 是 try-once：同步推一轮搬运，
//!   设备此刻没数据 / 收不下就如实返回 `Drained` / `Stuffed`；
//! * [`BuffRead::read_async`] / [`BuffWrite::write_async`] 在等待条件满足的过程中持续
//!   推进搬运，条件不满足时 park 在环的等待槽上（不忙等）；
//! * [`BuffWrite::flush_async`] / [`BuffWrite::close_async`] 用于收尾：把环内已提交
//!   但还没送达设备的数据冲出去。
//!
//! 按需形态**不 spawn 任何任务**，因此在 `smol::block_on`、`LocalExecutor`、`async_io`
//! 或任何自定义执行体下都能直接用，代价是「谁记得调收尾入口」由调用方负责——上层协议栈
//! 只把适配器当 `TrBuffWrite` 用时，最后一段会留在环里。
//!
//! 自驱动形态补上这个缺口：泵要 await `move_items_into_output_async`，它的 future 由
//! `abs_buff` 经 **GAT 投影**（`TrOutput::WriteAsync<'f>`）取得，而 GAT 投影**不传递
//! auto trait**，因此 `smol::spawn` 要求的 `Send` 证明不出来（即使设备是 socket 这种
//! `Send` 类型，rust-lang/rust#100013）。可行的投递点是**本线程**的 `LocalExecutor`——
//! `abs_art-smol::LocalScope::spawn_local` 不要求 `Send`。smol 没有环境运行时，作用域
//! 在任何线程都能取，但只有**持有它的线程**（`block_on(scope.run_until(..))`）驱动它时
//! 泵才会推进。
//!
//! # 分配点由 [`TrAllocConfig`] 决定
//!
//! 实现里有两个**互相独立**的分配点，各有一个关联类型，由调用方逐点选分配器：
//!
//! | 关联类型 | 用在哪 | 类型形态 |
//! | --- | --- | --- |
//! | `C::RingBodyAlloc` | 环的缓冲本体 | [`RingBufOf<C>`] |
//! | `C::RingSharedAlloc` | 装环的容器 | [`SharedRingOf<C>`] |
//!
//! 默认实现是 [`DefaultAllocConfig`]（两点全 `CoreAlloc`）。两个关联类型都要求
//! `core::alloc::AllocatorClone`。
//!
//! # 零拷贝程度
//!
//! **两个方向都是零拷贝**：段级直接与设备对接，任何一侧都不经过中转缓冲。
//!
//! * **写向**（环 → 设备）：段里的字节由调用方保证已初始化，`poll_write` 要的正是
//!   `&[u8]`，直接借出去即可；
//! * **读向**（设备 → 段）：`poll_read` 要 `&mut [u8]`，而段的写窗口是
//!   `&mut [MaybeUninit<u8>]`。本 crate 选择把窗口**原样**借给设备，代价是一处刻意的
//!   `unsafe`——它的不变式、边界与取舍写在 [`crate::device_`] 的模块文档和
//!   `assume_init_bytes_mut_` 的文档里。
//!
//! # 用法骨架
//!
//! ```no_run
//! #![feature(allocator_ext)]
//! use abs_buff::{Demand, TrBuffRead, TrBuffWrite};
//! use buffex_smol_adapt::{
//!     BuffRead, BuffWrite, DefaultAllocConfig,
//!     x_deps::abs_buff,
//! };
//!
//! # async fn demo(dev_r: futures_lite::io::Cursor<Vec<u8>>,
//! #               dev_w: futures_lite::io::Sink) -> std::io::Result<()> {
//! // 入向：设备 → 环 → 调用方。第三个参数是分配器配置（这里用默认）。
//! let mut rx = BuffRead::<_, DefaultAllocConfig>::try_new(
//!     dev_r,
//!     4096,
//!     DefaultAllocConfig,
//! )
//! .expect("容量须在 ring 允许范围内");
//! let demand = Demand::at_least(1);
//! let some = rx.read_async(&demand).await;
//! // ……消费借到的段（`move_items_to_buff` / `move_items_into_output_async`）……
//! drop(some);
//!
//! // 出向：调用方 → 环 → 设备。
//! let mut tx = BuffWrite::<_, DefaultAllocConfig>::try_new(
//!     dev_w,
//!     4096,
//!     DefaultAllocConfig,
//! )
//! .expect("容量须在 ring 允许范围内");
//! // ……借段写入后收尾，保证环内数据确实送达设备……
//! tx.close_async().await.expect("写方向应正常关闭");
//! # Ok(())
//! # }
//! ```
//!
//! # 消费与收尾语义
//!
//! * `iter_slices()` 是**窥视**：给出 `&[u8]`，既不推进段内已消费量，drop 段时也不提交；
//! * `move_items_to_buff` / `move_items_into_output_async` **搬出多少就消费多少**；
//! * 出向还有一层：**段 drop 只是提交进环**，不等于已经写进设备；收尾必须 await
//!   [`BuffWrite::flush_async`]（等环被泵取空）或 [`BuffWrite::close_async`]
//!   （排空 + `close`）。
//!
//! # 构建前提
//!
//! `buffex` / `abs_buff` 使用 nightly feature（`impl_trait_in_assoc_type` 等），因此本
//! crate 也需要 nightly 工具链。

#![feature(allocator_ext)]
#![feature(impl_trait_in_assoc_type)]

mod alloc_;
mod device_;
mod read_;
mod shared_;
mod write_;

pub use alloc_::{DefaultAllocConfig, TrAllocConfig};
pub use device_::{
    ReadAsInput, SmolInputReadAsync, SmolInputReadFuture, SmolOutputWriteAsync,
    SmolOutputWriteFuture, WriteAsOutput,
};
pub use read_::{BuffRead, BuffReadOpAsync, ReaderOf as ReadReaderOf, WriterOf as ReadWriterOf};
pub use shared_::{InvalidCapacity, RingBufOf, SharedRingOf};
pub use write_::{
    BuffWrite, BuffWriteCloseOpAsync, BuffWriteFlushOpAsync, BuffWriteOpAsync,
    ReaderOf as WriteReaderOf, WriterOf as WriteWriterOf,
};

/// 依赖重导出，便于调用方在不额外声明依赖的情况下对齐版本。
pub mod x_deps {
    pub use buffex::{self, x_deps::abs_buff};
    pub use futures_lite;
    pub use mm_ptr;
}
