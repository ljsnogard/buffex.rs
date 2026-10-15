//! `abs_buff` 的共享测试支撑（testkit）。
//!
//! 这里集中放「实现/测试段类型时反复需要的东西」，避免每个消费方各抄一份：
//!
//! * [`TestErr`]：只作为 `TrInput` / `TrOutput` 的 `Err` 出现的错误类型；
//! * [`TestInput`] / [`TestOutput`]：把数据与读写位置放在 `Arc` 里，因此
//!   **设备被缓冲（或别的所有者）拿走之后，测试仍能观察进度**；
//! * [`test_move_items_to_segm`] / [`test_move_items_from_segm`]：针对
//!   `TrBuffSegmRef` / `TrBuffSegmMut` 的 `move_items_*` **trait 默认实现**
//!   的泛型断言函数；
//! * [`read_initialized`]：把 `MaybeUninit` 缓冲里已初始化的元素读出来。
//!
//! 本 crate 只用于测试，因此直接依赖 `std`（`Arc` / `Mutex` / `Vec`）。
//!
//! # 装配
//!
//! 本 crate 以 git 依赖引用 `abs_buff`。若要在**本地**同时改动 `abs_buff`
//! 与消费方，请在消费方的 manifest 里把该 git 依赖 patch 到本地路径：
//!
//! ```toml
//! [patch."https://gitee.com/lino_snsalias/abs_buff.git"]
//! abs_buff = { path = "../abs_buff" }
//! ```

use core::{error::Error, fmt, fmt::Debug, mem::MaybeUninit};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    vec::Vec,
};

use abs_buff::{
    ReadySegm,
    buffer::{TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView},
    error::{ReadErrTag, WriteErrTag, TrTaggedError},
    io::{TrInput, TrOutput},
    x_deps::anylr::SomeOf,
};

/// 断言 `segm` 的剩余内容（各物理段按顺序拼接）恰好等于 `expect`。
///
/// 内部设计：`iter_slices` 可能把逻辑上的一段拆成多个物理段（例如跨缓冲区
/// 末端的环绕段），这里按顺序拼接校验，确保视图语义与内容都正确。
fn assert_view_eq<T, S>(segm: &S, expect: &[T])
where
    S: TrBuffSegmView<Item = T>,
    T: Copy + PartialEq + Debug,
{
    assert_eq!(segm.least_count(), expect.len(), "剩余数量必须等于期望数量");
    let mut off = 0usize;
    for piece in segm.iter_slices() {
        for (i, v) in piece.iter().enumerate() {
            assert_eq!(*v, expect[off + i], "第 {} 个元素", off + i);
        }
        off += piece.len();
    }
    assert_eq!(off, expect.len(), "各物理段拼接后必须恰好覆盖期望内容");
}

/// 泛型测试：`TrBuffSegmRef::move_items_to_segm` 的 trait 默认实现。
///
/// 把 `src` 的全部剩余元素搬进 `dst`，校验搬移数量、两侧消费量与源内容；
/// `dst` 的内容由调用方在底层存储上校验。
pub fn test_move_items_to_segm<'seg, T, S, D>(
    src: &mut S,
    dst: &mut D,
    expect: &[T],
) -> usize
where
    S: TrBuffSegmRef<'seg, T>,
    D: TrBuffSegmMut<'seg, T>,
    T: Copy + PartialEq + Debug,
{
    // 先验证源段的剩余内容；
    assert_view_eq(src, expect);
    // 显式调用 trait 默认实现（不经过具体类型可能存在的固有 move 方法）；
    let moved = TrBuffSegmRef::move_items_to_segm(src, dst);
    assert_eq!(moved, expect.len(), "搬移数量必须等于期望数量");
    assert_eq!(src.least_count(), 0, "源段必须被全部消费");
    assert_eq!(
        dst.least_count(),
        0,
        "目标段必须被全部填充（其容量需恰好为 expect.len()）"
    );
    moved
}

/// 泛型测试：`TrBuffSegmMut::move_items_from_segm` 的 trait 默认实现
/// （`move_items_to_segm` 的镜像，从目标段一侧发起）。
pub fn test_move_items_from_segm<'seg, T, S, D>(
    src: &mut S,
    dst: &mut D,
    expect: &[T],
) -> usize
where
    S: TrBuffSegmRef<'seg, T>,
    D: TrBuffSegmMut<'seg, T>,
    T: Copy + PartialEq + Debug,
{
    assert_view_eq(src, expect);
    let moved = TrBuffSegmMut::move_items_from_segm(dst, src);
    assert_eq!(moved, expect.len(), "搬移数量必须等于期望数量");
    assert_eq!(src.least_count(), 0, "源段必须被全部消费");
    assert_eq!(
        dst.least_count(),
        0,
        "目标段必须被全部填充（其容量需恰好为 expect.len()）"
    );
    moved
}

/// 泛型测试：`TrBuffSegmRef::move_items_to_buff` 的 trait 默认实现。
///
/// 把 `src` 的全部剩余元素搬进 `dst` 缓冲，内容直接在缓冲上校验。
///
/// ## Safety
///
/// 与 `move_items_to_buff` 相同：被搬移的元素按位拷贝、不再由段 drop；
/// 调用方需保证 `T` 无需要 drop 的资源。
pub unsafe fn test_move_items_to_buff<'seg, T, S>(
    src: &mut S,
    dst: &mut [MaybeUninit<T>],
    expect: &[T],
) -> usize
where
    S: TrBuffSegmRef<'seg, T>,
    T: Copy + PartialEq + Debug,
{
    assert_view_eq(src, expect);
    assert!(dst.len() >= expect.len(), "目标缓冲必须足以容纳全部元素");
    // SAFETY: 测试数据为无需 drop 的简单类型（调用方保证）；
    let moved = src.move_items_to_buff(dst);
    assert_eq!(moved, expect.len(), "搬移数量必须等于期望数量");
    assert_eq!(src.least_count(), 0, "源段必须被全部消费");
    for (i, m) in dst[..moved].iter().enumerate() {
        assert_eq!(
            unsafe { m.assume_init_read() },
            expect[i],
            "第 {} 个元素",
            i
        );
    }
    moved
}

/// 泛型测试：`TrBuffSegmMut::move_items_from_buff` 的 trait 默认实现。
///
/// 先用 `expect` 填充 `src` 缓冲，再整体搬进 `dst` 段；`dst` 的内容由调用方
/// 在底层存储上校验。
///
/// ## Safety
///
/// 与 `move_items_from_buff` 相同：`src` 中被搬走的元素按位拷贝、不再被 drop；
/// 调用方需保证 `T` 无需要 drop 的资源。
pub unsafe fn test_move_items_from_buff<'seg, T, D>(
    dst: &mut D,
    src: &mut [MaybeUninit<T>],
    expect: &[T],
) -> usize
where
    D: TrBuffSegmMut<'seg, T>,
    T: Copy + PartialEq + Debug,
{
    assert!(src.len() >= expect.len(), "源缓冲必须足以容纳全部元素");
    // 填充源缓冲；
    for (i, m) in src[..expect.len()].iter_mut().enumerate() {
        *m = MaybeUninit::new(expect[i]);
    }
    // SAFETY: 测试数据为无需 drop 的简单类型（调用方保证）；
    let moved = dst.move_items_from_buff(src);
    assert_eq!(moved, expect.len(), "搬移数量必须等于期望数量");
    assert_eq!(
        dst.least_count(),
        0,
        "目标段必须被全部填充（其容量需恰好为 expect.len()）"
    );
    moved
}

/// 读出 `MaybeUninit` 切片里**前 `n` 个已初始化**的元素。
///
/// 段搬移类测试几乎都要做这件事（把搬进 `[MaybeUninit<T>]` 的数据取出来校验），
/// 放在这里避免每个 crate 各写一份。
///
/// # Panics
///
/// `n > src.len()` 时 panic。
pub fn read_initialized<T: Copy>(src: &[MaybeUninit<T>], n: usize) -> Vec<T> {
    assert!(n <= src.len(), "n 不能超过切片长度");
    src[..n]
        .iter()
        .map(|m| unsafe { m.assume_init_read() })
        .collect()
}

/// 测试用错误类型：只作为 `TrInput` / `TrOutput` 的 `Err` 存在。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum TestErr {
    /// 一般性失败。
    Boom,
    /// 目标已满（由 `pipelining` 的测试构造）。
    Stuffed,
}

impl fmt::Display for TestErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TestErr::Boom => write!(f, "boom"),
            TestErr::Stuffed => write!(f, "blocked"),
        }
    }
}

impl Error for TestErr {}

impl TrTaggedError<ReadErrTag> for TestErr {
    fn err_tag(&self) -> ReadErrTag {
        ReadErrTag::Unknown
    }
}

impl TrTaggedError<WriteErrTag> for TestErr {
    fn err_tag(&self) -> WriteErrTag {
        WriteErrTag::Unknown
    }
}

/// 测试输入设备：数据与读取位置放在 `Arc` 里，**设备被缓冲拥有期间**测试仍能
/// 通过自己持有的 `Arc` 观察进度。
pub struct TestInput {
    /// 待读数据。
    pub data: Arc<Mutex<Vec<u8>>>,
    /// 已读位置。
    pub pos: Arc<AtomicUsize>,
}

impl TestInput {
    /// 用给定数据创建输入设备。
    pub fn new(data: Vec<u8>) -> Self {
        TestInput {
            data: Arc::new(Mutex::new(data)),
            pos: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl abs_buff::buffer::TrConsumerState for TestInput {}

impl TrInput<u8> for TestInput {
    type ReadAsync<'f> = ReadySegm<usize, TestErr> where Self: 'f;
    type Err = TestErr;

    fn read_async<'f>(
        &'f mut self,
        target: &'f mut [MaybeUninit<u8>],
    ) -> Self::ReadAsync<'f> {
        let data = self.data.lock().unwrap_or_else(|e| e.into_inner());
        let pos = self.pos.load(Ordering::Relaxed);
        let n = core::cmp::min(target.len(), data.len() - pos);
        for (i, slot) in target[..n].iter_mut().enumerate() {
            *slot = MaybeUninit::new(data[pos + i]);
        }
        self.pos.store(pos + n, Ordering::Relaxed);
        ReadySegm::new(SomeOf::new_left(n))
    }
}

/// 测试输出设备：收下的数据放在 `Arc` 里，测试可随时观察。
pub struct TestOutput {
    /// 已写出的数据。
    pub data: Arc<Mutex<Vec<u8>>>,
}

impl TestOutput {
    /// 创建空的输出设备。
    pub fn new() -> Self {
        TestOutput {
            data: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// 取当前已写出数据的快照。
    pub fn snapshot(&self) -> Vec<u8> {
        self.data.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

impl Default for TestOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl abs_buff::buffer::TrProducerState for TestOutput {}

impl TrOutput<u8> for TestOutput {
    type WriteAsync<'f> = ReadySegm<usize, TestErr> where Self: 'f;

    type Err = TestErr;

    fn write_async<'f>(
        &'f mut self,
        source: &'f [MaybeUninit<u8>],
    ) -> Self::WriteAsync<'f> {
        let n = source.len();
        let mut data = self.data.lock().unwrap_or_else(|e| e.into_inner());
        for m in source {
            // SAFETY: 测试数据为 u8，无 drop 需求。
            data.push(unsafe { m.assume_init_read() });
        }
        ReadySegm::new(SomeOf::new_left(n))
    }
}
