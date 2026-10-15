//! 内存分配点的配置：把「哪儿需要分配器、约束是什么」集中成一个 trait，
//! 由调用方选具体实现。
//!
//! # 为什么要这个 trait
//!
//! 两侧适配器的内部实现里有**互相独立**的分配点，它们对分配器的要求并不相同
//! （环的缓冲本体是定长一次性分配；`Shared` 的引用计数块要与容器一起存活）。把分配器
//! 写死成 `Global` 会让「用 bump / 池化 / 共享 arena 分配器」的调用方无从下手，而只
//! 暴露**一个**分配器参数又逼着调用方在所有点上用同一个。
//!
//! [`TrAllocConfig`] 为每个分配点给一个**独立的关联类型**，于是调用方可以：
//!
//! * 用默认实现 [`DefaultAllocConfig`]（都是 `Global`）；
//! * 或自定义一个 config，逐点挑不同的分配器（例如环体用 arena、容器用 `Global`）。
//!
//! # 分配点一览
//!
//! | 关联类型 | 用在哪 | 生命周期特征 |
//! | --- | --- | --- |
//! | [`RingBodyAlloc`](TrAllocConfig::RingBodyAlloc) | 环的缓冲本体 `Box<[MaybeUninit<u8>], A>` | 与环同寿，一次性定长分配 |
//! | [`RingSharedAlloc`](TrAllocConfig::RingSharedAlloc) | 装环的容器 `mm_ptr::Shared<Ring<…>, A>` | 与环同寿（引用计数块） |
//!
//! 两个方向的搬运都是**零拷贝**（段 ↔ `TrInput` / `TrOutput` 直连），所以除环本身之外
//! 没有别的分配点：没有中转缓冲，也就没有对应的分配器项。
//!
//! # 约束
//!
//! 两个关联类型都要求 [`AllocatorClone`]（= `Allocator + Clone` 的 marker，见
//! `core::alloc`）。原因：环要经 `Shared` 拆分，`Ring::split_unchecked` 内部会 `clone()`
//! 容器，而 `Arc<T, A>: Clone` 要求的是 `A: AllocatorClone`（标准库**不会**从 `Clone`
//! 自动推导）。
//!
//! # 示例
//!
//! ```no_run
//! #![feature(allocator_api)]
//! use std::alloc::Global;
//!
//! use buffex_tokio_adapt::TrAllocConfig;
//!
//! /// 两个分配点都用 `Global` 的示例配置。
//! #[derive(Clone, Default)]
//! struct MyConfig;
//!
//! impl TrAllocConfig for MyConfig {
//!     type RingBodyAlloc = Global;
//!     type RingSharedAlloc = Global;
//!
//!     fn ring_body_alloc(&self) -> Self::RingBodyAlloc { Global }
//!     fn ring_shared_alloc(&self) -> Self::RingSharedAlloc { Global }
//! }
//! ```
//!
//! # 用默认配置
//!
//! ```no_run
//! #![feature(allocator_api)]
//! use buffex_tokio_adapt::{BuffRead, BuffWrite, DefaultAllocConfig};
//!
//! # async fn demo(dev_r: tokio::io::Empty, dev_w: tokio::io::Sink) {
//! let mut rx =
//!     BuffRead::<_, DefaultAllocConfig>::try_new(dev_r, 4096, DefaultAllocConfig)
//!         .unwrap();
//! let mut tx =
//!     BuffWrite::<_, DefaultAllocConfig>::try_new(dev_w, 4096, DefaultAllocConfig)
//!         .unwrap();
//! # let _ = (&mut rx, &mut tx);
//! # }
//! ```

use core::alloc::{Allocator, AllocatorClone};

use abs_mm::CoreAlloc;
use mm_ptr::x_deps::abs_mm;

/// 集中描述适配器内部各分配点的分配器选择。
///
/// 每个关联类型对应一个独立的分配点（见模块文档的表格），用户可以逐点给不同的
/// 分配器；四个关联类型都必须满足 [`AllocatorClone`]。
///
/// 另外要求 `Clone`：适配器会在需要分配器的地方从配置里取一份克隆，而不必要求取用点
/// 是 `&mut self`（`Shared` 的引用计数块与缓冲本体各要一份）。
pub trait TrAllocConfig: Clone {
    /// 环的缓冲本体（`Box<[MaybeUninit<u8>], _>`）用哪个分配器。
    type RingBodyAlloc: Allocator;

    /// 装环的容器（`mm_ptr::Shared<Ring<…>, _>`，含引用计数块）用哪个分配器。
    type RingSharedAlloc: AllocatorClone;

    /// 取环体分配器（每个环体分配一次）。
    fn ring_body_alloc(&self) -> Self::RingBodyAlloc;

    /// 取共享容器分配器（每个环一次，用于 `Shared` 的分配）。
    fn ring_shared_alloc(&self) -> Self::RingSharedAlloc;

}

/// 默认配置：各分配点都用 [`Global`]。
///
/// 想让某一处换分配器时，实现自己的 [`TrAllocConfig`] 并只在那一处给别的类型即可。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DefaultAllocConfig;

impl TrAllocConfig for DefaultAllocConfig {
    type RingBodyAlloc = CoreAlloc;
    type RingSharedAlloc = CoreAlloc;

    #[inline]
    fn ring_body_alloc(&self) -> Self::RingBodyAlloc {
        CoreAlloc
    }

    #[inline]
    fn ring_shared_alloc(&self) -> Self::RingSharedAlloc {
        CoreAlloc
    }
}
