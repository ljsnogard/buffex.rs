//! 把**任意** `tokio::io::AsyncRead` / `AsyncWrite` 设备接上 `buffex` 环，变成
//! [`TrBuffRead`](abs_buff::TrBuffRead) / [`TrBuffWrite`](abs_buff::TrBuffWrite)。
//!
//! # 它提供什么
//!
//! | 类型 | 方向 | 调用方拿到的 trait |
//! | --- | --- | --- |
//! | [`BuffRead`] | 设备 → 环 | `TrBuffRead` / `TrBuffTryRead` |
//! | [`BuffWrite`] | 环 → 设备 | `TrBuffWrite` / `TrBuffTryWrite` |
//!
//! 两个适配器各自持有一个 `buffex` 环（`mm_ptr::Shared<Ring<…>>`，经 `Ring::split_unchecked`
//! 拆成读写两半）与一台设备：调用方对着环按段读写，适配器负责设备与环之间的搬运。
//! 收发双工要的是**两个**适配器（各一个环），因为 ring 的安全前提是 SPSC。
//!
//! # 分配点由 [`TrAllocConfig`] 决定
//!
//! 实现里有两个**互相独立**的分配点，每个点在 [`TrAllocConfig`] 里有一个关联类型，
//! 由调用方逐点选分配器：
//!
//! | 关联类型 | 用在哪 | 类型形态 |
//! | --- | --- | --- |
//! | `C::RingBodyAlloc` | 环的缓冲本体 | [`RingBufOf<C>`] = `Box<[MaybeUninit<u8>], _>` |
//! | `C::RingSharedAlloc` | 装环的容器 | [`SharedRingOf<C>`] = `mm_ptr::Shared<Ring<…>, _>` |
//!
//! 两个方向的搬运都是**零拷贝**（段 ↔ `TrInput` / `TrOutput` 直连），所以除环本身之外
//! 没有别的分配点。
//!
//! 用法：构造时把配置实例传进去 —— `BuffRead::try_new(dev, cap, my_config)`。
//! 默认实现是 [`DefaultAllocConfig`]（两点全 `Global`），想要别的就在自定义 config
//! 里把对应关联类型换掉。
//!
//! 两个关联类型都要求 `core::alloc::AllocatorClone`（`mm_ptr::Shared<T, A>: Clone`
//! 要求它，而 `Shared` 拆分要用到克隆；只写 `Allocator + Clone` 推导不出来）。
//!
//! # 驱动模型：两种，按上层协议栈的形状选
//!
//! | 形态 | 构造入口 | 谁在驱动「环 → 设备」 |
//! | --- | --- | --- |
//! | **按需** | [`BuffWrite::try_new`] | 调用方：`try_write` / `write_async` / `flush_async` / `close_async` 每次顺带推一轮 |
//! | **自驱动** | [`BuffWrite::try_new_local`] | 后台泵：构造时经 `abs_art-tokio` 的 `LocalScope` 投到**本线程**队列 |
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
//! 按需形态**不 spawn 任何任务**，因此在 `tokio::spawn` 之外、`join!` 里、甚至同步
//! 上下文里拿 `try_*` 都能用，代价是「谁记得调收尾入口」由调用方负责——上层协议栈
//! 只把适配器当 `TrBuffWrite` 用时，最后一段会留在环里。
//!
//! 自驱动形态补上这个缺口：泵要 await `move_items_into_output_async`，它的 future 由
//! `abs_buff` 经 **GAT 投影**（`TrOutput::WriteAsync<'f>`）取得，而 GAT 投影**不传递
//! auto trait**，因此 `tokio::spawn` 要求的 `Send` 证明不出来（即使设备是 socket 这种
//! `Send` 类型，rust-lang/rust#100013）。可行的投递点是**本线程**的 `LocalSet`——
//! `abs_art-tokio::LocalScope::spawn_local` 不要求 `Send`。代价是构造点必须落在
//! `run_until` 内（见 [`BuffWrite::try_new_local`] 的 `# Panics`）。
//!
//! # 用法骨架
//!
//! ```no_run
//! #![feature(allocator_api)]
//! use abs_buff::TrBuffRead;
//! use buffex_tokio_adapt::{
//!     BuffRead, BuffWrite, DefaultAllocConfig,
//!     x_deps::abs_buff,
//! };
//!
//! # async fn demo(dev_r: tokio::io::DuplexStream, dev_w: tokio::io::DuplexStream)
//! # -> std::io::Result<()> {
//! // 入向：设备 → 环 → 调用方。第三个参数是分配器配置（这里用默认的全 Global）。
//! let mut rx = BuffRead::<_, DefaultAllocConfig>::try_new(
//!     dev_r,
//!     4096,
//!     DefaultAllocConfig,
//! )
//! .expect("容量合法");
//! let demand = abs_buff::Demand::at_least(1);
//! let some = rx.read_async(&demand).await;
//! // ……消费借到的段（`move_items_to_buff` / `move_items_into_output_async`）……
//!
//! // 出向：调用方 → 环 → 设备。
//! let mut tx = BuffWrite::<_, DefaultAllocConfig>::try_new(
//!     dev_w,
//!     4096,
//!     DefaultAllocConfig,
//! )
//! .expect("容量合法");
//! // ……借段写入后收尾，保证环内数据确实送达设备……
//! tx.close_async().await.expect("写方向应正常关闭");
//! # Ok(())
//! # }
//! ```
//!
//! # 消费语义（写给调用方）
//!
//! `buffex` 的段有两种视图，**只有一种会推进消费**：
//!
//! * `iter_slices()` 是**窥视**：给出 `&[u8]`，既不推进段内已消费量，drop 段时也不提交；
//! * `move_items_to_buff` / `move_items_into_output_async` **搬出多少就消费多少**，
//!   drop 段时按该量推进位置。
//!
//! 出向还有一层：**段 drop 只是提交进环**，不等于已经写进设备；收尾必须 await
//! [`BuffWrite::flush_async`] 或 [`BuffWrite::close_async`]。
//!
//! # 构建前提
//!
//! `buffex` 使用 nightly feature（`impl_trait_in_assoc_type` 等），因此本 crate 也需要
//! nightly 工具链。它不在 `buffex` 的 workspace 内，有自己的 `Cargo.lock`。

#![feature(allocator_ext)]
#![feature(core_io)]
#![feature(impl_trait_in_assoc_type)]

mod alloc_;
mod read_;
mod shared_;
mod write_;

pub use alloc_::{TrAllocConfig, DefaultAllocConfig};
pub use read_::{
    BuffRead, BuffReadOpAsync, ReaderOf as ReadReaderOf, WriterOf as ReadWriterOf,
};
pub use shared_::{InvalidCapacity, RingBufOf, SharedRingOf};
pub use write_::{
    BuffWrite, BuffWriteCloseOpAsync, BuffWriteFlushOpAsync, BuffWriteOpAsync,
    ReaderOf as WriteReaderOf, TryNewError, WriterOf as WriteWriterOf,
};

pub mod x_deps {
    pub use abs_buff_tokio_adapt;
    pub use buffex;
    pub use mm_ptr;

    pub use buffex::x_deps::abs_buff;
}
