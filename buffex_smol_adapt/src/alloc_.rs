//! 内存分配点的配置：把「哪儿需要分配器、约束是什么」集中成一个 trait，
//! 由调用方选具体实现。
//!
//! # 分配点一览
//!
//! | 关联类型 | 用在哪 | 生命周期特征 |
//! | --- | --- | --- |
//! | [`RingBodyAlloc`](TrAllocConfig::RingBodyAlloc) | 环的缓冲本体 `Owned<[MaybeUninit<u8>], A>` | 与环同寿，一次性定长分配 |
//! | [`RingSharedAlloc`](TrAllocConfig::RingSharedAlloc) | 装环的容器 `mm_ptr::Shared<Ring<…>, A>` | 与环同寿（引用计数块） |
//!
//! 约束与 `buffex_tokio_adapt` 的同名 trait 逐项相同：只要求 `Clone` 与
//! [`AllocatorClone`]，**没有** `Send` / `'static`——本 crate 不 spawn 任何任务，
//! 适配器与设备始终留在调用方的线程与栈上。
//!
//! # 约束
//!
//! 两个关联类型都要求 [`AllocatorClone`]（= `Allocator + Clone` 的 marker，见
//! `core::alloc`）：环要经 `Shared` 拆分，[`Ring::split_unchecked`](buffex::ring::Ring::split_unchecked)
//! 内部会 `clone()` 容器，而 `Shared<T, A>: Clone` 要求的是 `A: AllocatorClone`
//! （标准库**不会**从 `Clone` 自动推导）。
//!
//! # 示例
//!
//! ```no_run
//! #![feature(allocator_ext)]
//! use std::alloc::Global;
//!
//! use buffex_smol_adapt::TrAllocConfig;
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

use core::alloc::{Allocator, AllocatorClone};

use mm_ptr::x_deps::abs_mm;

/// 集中描述适配器内部各分配点的分配器选择。
///
/// 每个关联类型对应一个独立的分配点（见模块文档的表格），用户可以逐点给不同的
/// 分配器；两个关联类型都必须满足 [`AllocatorClone`]。
///
/// 另外要求 `Clone`：适配器会在需要分配器的地方从配置里取一份克隆，而不必要求取用点
/// 是 `&mut self`（`Shared` 的引用计数块与缓冲本体各要一份）。
pub trait TrAllocConfig: Clone {
    /// 环的缓冲本体（`mm_ptr::Owned<[MaybeUninit<u8>], _>`）用哪个分配器。
    type RingBodyAlloc: Allocator;

    /// 装环的容器（`mm_ptr::Shared<Ring<…>, _>`，含引用计数块）用哪个分配器。
    type RingSharedAlloc: AllocatorClone;

    /// 取环体分配器（每个环体分配一次）。
    fn ring_body_alloc(&self) -> Self::RingBodyAlloc;

    /// 取共享容器分配器（每个环一次，用于 `Shared` 的分配）。
    fn ring_shared_alloc(&self) -> Self::RingSharedAlloc;
}

/// 默认配置：两个分配点都用 [`abs_mm::CoreAlloc`]。
///
/// 想让某一处换分配器时，实现自己的 [`TrAllocConfig`] 并只在那一处给别的类型即可。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DefaultAllocConfig;

impl TrAllocConfig for DefaultAllocConfig {
    type RingBodyAlloc = abs_mm::CoreAlloc;
    type RingSharedAlloc = abs_mm::CoreAlloc;

    #[inline]
    fn ring_body_alloc(&self) -> Self::RingBodyAlloc {
        abs_mm::CoreAlloc
    }

    #[inline]
    fn ring_shared_alloc(&self) -> Self::RingSharedAlloc {
        abs_mm::CoreAlloc
    }
}
