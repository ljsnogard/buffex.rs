//! `abs_buff::buffer::segm` 中四个「与 `TrInput` / `TrOutput` 交换数据」的异步
//! 实现的集成测试。
//!
//! 被测的四个实现（均位于 `abs_buff/src/buffer/segm_.rs`）：
//!
//! 1. `SegmRef::move_items_to_output_async`：`SegmRef` 的关联函数，单步搬移；
//! 2. `SegmMut::move_items_from_input_async`：`SegmMut` 的关联函数，单步搬移；
//! 3. `TrBuffSegmRef::move_items_into_output_async`：trait provided method，循环聚合；
//! 4. `TrBuffSegmMut::move_items_from_input_async`：trait provided method，循环聚合。
//!
//! 所有测试都用 `#[compio::test]` 接入 compio 的真实异步运行时；数据交换的对端是
//! **真实磁盘文件**（`compio::fs::File`，读写经 io_uring 完成事件），既不手写
//! `poll`、也不使用「立即就绪」的假 future。设备上额外挂的两种策略（前若干次读写
//! 返回可重试错误、单次读取限长）只是真实设备的行为参数，用于驱动被测实现内部的
//! 循环分支，不改变「数据来自真实文件」这一事实。
//!
//! # 语义约定：`Demand` 用「起点 + 长度」描述左闭右开区间
//!
//! `Demand` 表示一组被允许的取值（数量），区间一律是左闭右开的 `[起点, 起点 + 长度)`
//! 或单边约束；`min()` / `max()` 给出的是**含端点**的约束边界（`None` 表示该侧无约束）：
//!
//! * `exactly(n)`：恰好 n 个；
//! * `no_more_than(n)`：最多 n 个；
//! * `at_least(n)`：至少 n 个；
//! * `between(a, b)`：a 到 b，两端都含。
//!
//! 上界是含端点的，所以搬移/读写实现可以直接把 `max()` 当作"最多搬多少"使用，不必再做
//! `- 1` 修正；实现侧描述"有 size 个可供搬移"时直接写 `no_more_than(size)` 即可。两个
//! 集合恰好相接（无交集）时 `compromise` 返回 `None` 而不会 panic；取值上限处的需求
//! （如 `exactly(usize::MAX)`）也能精确表示（内部是起始值 + 长度 1）。
//!
//! # 为什么用例里 `no_more_than(n)` 与 `exactly(n)` 都会出现
//!
//! 选择依据不是风格，而是"这条用例要覆盖哪段实现"：
//!
//! * `no_more_than(n)` 用于表达"最多 n 个 / 能取多少取多少"的语义，它与段自身的
//!   `no_more_than(size)` 求交后仍落在 `NoMoreThan` 分支上；
//! * `exactly(n)`（以及 `at_least(n)`）会让 `compromise` 的结果落在 `BaseAndLen`
//!   （起点 + 长度）上，从而覆盖"起点 + 长度"的长度算术与 `max()` 的换算；
//!   只用 `no_more_than` 的用例走不到这些路径（`NoMoreThan` 的 `max()` 只是一次取字段）；
//! * 可用量 < n 时两者**行为不同**：`no_more_than(n)` 会搬走可用的那一部分，而
//!   `exactly(n)` 的交集为空、一个都不搬——这正是"需求不可满足"与"恰好相等"两个
//!   用例存在的理由。

// `gen_may_cancel_future` 生成的工厂 trait 以 `impl Trait` 作为关联类型，与
// `abs_buff_tokio_adapt` 等下游 crate 一样需要显式开启该特性。
#![feature(impl_trait_in_assoc_type)]

// `gen_may_cancel_future` 展开出的代码以 `abs_cancel::…` 的绝对路径引用取消令牌
// 类型，因此需要把 `abs_buff::x_deps` 中的重导出引入本 crate 的作用域，使该路径
// 能够解析（宏按 trait 路径最后一段匹配约束，正是为兼容这种重导出路径）。
use abs_buff::x_deps::abs_cancel;

use core::{mem::MaybeUninit, pin::Pin};
use std::{path::PathBuf, vec, vec::Vec};

use abs_buff::{
    Demand,
    buffer::{SegmMut, SegmReclaim, SegmRef, TrBuffSegmMut, TrBuffSegmRef},
    error::{ReadErrTag, TaggedError, WriteErrTag},
    gen_may_cancel_future,
    io::{TrInput, TrOutput},
    x_deps::anylr::SomeOf,
};
use compio::{
    buf::BufResult,
    fs::File,
    io::{AsyncReadAt, AsyncWriteAt},
};

// ---------------------------------------------------------------------------
// 真实异步设备：compio 文件 → TrInput<u8> / TrOutput<u8>
// ---------------------------------------------------------------------------

/// 以真实 compio 文件为数据源的 [`TrInput<u8>`] 实现。
///
/// * `pos_`：当前读取位置（compio 的文件句柄为定位式读写，自身不维护游标）；
/// * `stalls_left_`：前若干次读取返回可重试错误 [`ReadErrTag::Drained`]，用于驱动
///   上层循环的「重试」分支；
/// * `max_chunk_`：单次底层读取的字节数上限，用于制造短读，驱动单步搬移内部
///   「未读满则继续」的循环；
/// * `calls_`：设备被调用（进入 step 函数）的次数，供测试断言重试与短读次数；
/// * 读到文件末尾时返回 [`ReadErrTag::Closing`]：按本项目约定，EOF 以错误形式
///   上报（而不是返回 0），这样 provided method 才能正常终止而不是空转。
struct FileInput {
    file_: File,
    pos_: u64,
    stalls_left_: usize,
    max_chunk_: usize,
    calls_: usize,
}

impl FileInput {
    /// 用给定的 compio 文件构造输入设备。
    ///
    /// `stalls_left` 为「前几次读取返回可重试错误」的次数，`max_chunk` 为单次读取
    /// 的字节数上限。
    fn new_(file: File, stalls_left: usize, max_chunk: usize) -> Self {
        FileInput {
            file_: file,
            pos_: 0,
            stalls_left_: stalls_left,
            max_chunk_: max_chunk,
            calls_: 0,
        }
    }
}

/// [`FileInput::read_async`] 的 step 函数。
#[gen_may_cancel_future(FileInputRead, pub)]
async fn file_input_read_async_<'f, TyTok>(
    input: &'f mut FileInput,
    target: &'f mut [MaybeUninit<u8>],
    _token: TyTok,
) -> SomeOf<usize, TaggedError<std::io::Error, ReadErrTag>>
where
    TyTok: abs_cancel::TrCancellationToken,
{
    input.calls_ += 1;
    if input.stalls_left_ > 0 {
        input.stalls_left_ -= 1;
        return SomeOf::new_right(TaggedError::new(
            std::io::Error::new(std::io::ErrorKind::WouldBlock, "设备暂时读不到"),
            ReadErrTag::Drained,
        ));
    }
    if target.is_empty() {
        return SomeOf::new_left(0usize);
    }
    let want = core::cmp::min(target.len(), input.max_chunk_);
    let pos = input.pos_;
    // compio 的 `read_at` 要求按值传入 `IoBufMut`，因此先读进 owned 临时缓冲，
    // 再把有效前缀写回调用方的 `MaybeUninit` 目标。
    let BufResult(res, temp) = input.file_.read_at(vec![0u8; want], pos).await;
    match res {
        Result::Ok(0) => SomeOf::new_right(TaggedError::new(
            std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "文件已读完"),
            ReadErrTag::Closing,
        )),
        Result::Ok(n) => {
            for (dst, src) in target[..n].iter_mut().zip(temp[..n].iter()) {
                dst.write(*src);
            }
            input.pos_ += n as u64;
            SomeOf::new_left(n)
        }
        Result::Err(err) => {
            SomeOf::new_right(TaggedError::new(err, ReadErrTag::Propagated))
        }
    }
}

impl TrInput<u8> for FileInput {
    type ReadAsync<'f>
        = FileInputReadAsync<'f, 'f>
    where
        Self: 'f,
        u8: 'f;

    type Err = TaggedError<std::io::Error, ReadErrTag>;

    fn read_async<'f>(
        &'f mut self,
        target: &'f mut [MaybeUninit<u8>],
    ) -> Self::ReadAsync<'f> {
        FileInputReadAsync::new(self, target)
    }
}

/// 以真实 compio 文件为落点的 [`TrOutput<u8>`] 实现。
///
/// * `pos_`：当前写入位置；
/// * `stalls_left_`：前若干次写入返回可重试错误 [`WriteErrTag::Stuffed`]，用于驱动
///   上层循环的「重试」分支；
/// * `max_chunk_`：单次底层写入的字节数上限，用于制造部分写入，驱动单步搬移内部
///   「未写完则继续」的循环；
/// * `calls_`：设备被调用（进入 step 函数）的次数，供测试断言重试次数。
struct FileOutput {
    file_: File,
    pos_: u64,
    stalls_left_: usize,
    max_chunk_: usize,
    calls_: usize,
}

impl FileOutput {
    /// 用给定的 compio 文件构造输出设备。
    fn new_(file: File, stalls_left: usize, max_chunk: usize) -> Self {
        FileOutput {
            file_: file,
            pos_: 0,
            stalls_left_: stalls_left,
            max_chunk_: max_chunk,
            calls_: 0,
        }
    }
}

/// [`FileOutput::write_async`] 的 step 函数。
#[gen_may_cancel_future(FileOutputWrite, pub)]
async fn file_output_write_async_<'f, TyTok>(
    output: &'f mut FileOutput,
    source: &'f [MaybeUninit<u8>],
    _token: TyTok,
) -> SomeOf<usize, TaggedError<std::io::Error, WriteErrTag>>
where
    TyTok: abs_cancel::TrCancellationToken,
{
    output.calls_ += 1;
    if output.stalls_left_ > 0 {
        output.stalls_left_ -= 1;
        return SomeOf::new_right(TaggedError::new(
            std::io::Error::new(std::io::ErrorKind::WouldBlock, "设备暂时写不进"),
            WriteErrTag::Stuffed,
        ));
    }
    if source.is_empty() {
        return SomeOf::new_left(0usize);
    }
    let want = core::cmp::min(source.len(), output.max_chunk_);
    // SAFETY: 调用方保证 `source` 的前 `want` 个字节已初始化；`MaybeUninit<u8>` 与
    // `u8` 布局、对齐完全相同，故按已初始化字节解读是健全的。
    let owned: Vec<u8> = unsafe {
        core::slice::from_raw_parts(source.as_ptr() as *const u8, want)
    }
    .to_vec();
    let pos = output.pos_;
    let BufResult(res, _) = output.file_.write_at(owned, pos).await;
    match res {
        Result::Ok(n) => {
            output.pos_ += n as u64;
            SomeOf::new_left(n)
        }
        Result::Err(err) => {
            SomeOf::new_right(TaggedError::new(err, WriteErrTag::Propagated))
        }
    }
}

impl TrOutput<u8> for FileOutput {
    type WriteAsync<'f>
        = FileOutputWriteAsync<'f, 'f>
    where
        Self: 'f,
        u8: 'f;

    type Err = TaggedError<std::io::Error, WriteErrTag>;

    fn write_async<'f>(
        &'f mut self,
        source: &'f [MaybeUninit<u8>],
    ) -> Self::WriteAsync<'f> {
        FileOutputWriteAsync::new(self, source)
    }
}

// ---------------------------------------------------------------------------
// 测试辅助
// ---------------------------------------------------------------------------

/// 生成互不冲突的临时文件路径（测试并行执行时也不会互相覆盖）。
fn tmp_path_(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "abs_buff_segm_async_{}_{tag}.bin",
        std::process::id()
    ))
}

/// 取出 [`SomeOf`] 的左值（成功搬移的字节数）；若结果是右侧错误，则连带错误内容
/// panic，便于定位失败原因。
fn left_of_<TyErr>(v: SomeOf<usize, TyErr>, ctx: &str) -> usize
where
    TyErr: std::fmt::Debug,
{
    let detail = v.as_ref().pick_right().map(|e| format!("{e:?}"));
    match v.pick_left() {
        Option::Some(n) => n,
        Option::None => panic!("{ctx}：期望成功结果，实际得到错误 {detail:?}"),
    }
}

/// 读出 `MaybeUninit` 缓冲中前 `n` 个**已初始化**的元素。
fn read_init_<T: Copy>(src: &[MaybeUninit<T>], n: usize) -> Vec<T> {
    // SAFETY: 调用方保证前 `n` 个元素已由被测代码写入；`T: Copy`，按位读出即可。
    src[..n]
        .iter()
        .map(|m| unsafe { m.assume_init_read() })
        .collect()
}

// ---------------------------------------------------------------------------
// 1. SegmRef::move_items_to_output_async（关联函数，单步）
// ---------------------------------------------------------------------------

/// 验证 `SegmRef::move_items_to_output_async` 单步搬移把段内数据按序写入真实文件。
/// - 测试目标：搬移量取「需求允许的最大数量（含端点）」与「段内剩余量」的较小者，并按
///   实际写入量推进段的已消费量；`exactly(n)` 必须恰好 n 个；需求与可搬量无交集时不搬移；
///   设备一次写不完时内部继续写。
/// - 测试手段：在 compio 运行时中创建真实输出文件（单次写入上限 2 字节以制造部分写入），
///   对 5 字节段 `[1, 2, 3, 4, 5]` 依次调用：`exactly(3)`、`exactly(3)`（此时只剩 2 个）、
///   `no_more_than(3)`、`no_more_than(3)`（此时段已空）。
/// - 判定标准：四次返回依次为 3、0、2、0；段剩余量依次为 2、2、0、0；设备调用次数依次为
///   2、2、3、3（第 1 轮由 2 + 1 两次写入凑满；无交集与空段都不触碰设备）；设备写入
///   位置为 5；读回的文件恰好为 `[1, 2, 3, 4, 5]`；段 drop 后回收量为 5。
#[compio::test]
async fn segm_ref_move_items_to_output_async_writes_prefix_to_file() {
    let path = tmp_path_("ref_step_out");
    let file = File::create(&path).await.expect("创建输出文件");
    let mut output = FileOutput::new_(file, 0, 2);

    let data: Vec<u8> = vec![1, 2, 3, 4, 5];
    let mut consumed = 0usize;
    let mut segm = SegmRef::new(
        &data[..],
        SegmReclaim::new(Pin::new(&mut consumed)),
    );

    // 第 1 轮：`exactly(3)` 要求恰好 3 个（限长 2 → 由 2 + 1 两次写入完成）。
    let demand = Demand::exactly(3);
    let res = segm.move_items_to_output_async(&mut output, &demand).await;
    assert_eq!(left_of_(res, "第 1 轮搬移"), 3, "exactly(3) 必须恰好 3 个");
    assert_eq!(segm.least_count(), 2);
    assert_eq!(output.calls_, 2, "限长 2 时应由 2 + 1 两次写入凑满 3 字节");

    // 第 2 轮：段内只剩 2 个，`exactly(3)` 与可搬量无交集 → 0 且不触碰设备。
    let demand = Demand::exactly(3);
    let res = segm.move_items_to_output_async(&mut output, &demand).await;
    assert_eq!(left_of_(res, "第 2 轮搬移"), 0, "需求下界 3 > 可搬量 2");
    assert_eq!(segm.least_count(), 2);
    assert_eq!(output.calls_, 2, "无交集时不应触碰设备");

    // 第 3 轮：`no_more_than(3)` 允许最多 3 个 → 把剩下的 2 个搬完。
    let demand = Demand::no_more_than(3);
    let res = segm.move_items_to_output_async(&mut output, &demand).await;
    assert_eq!(left_of_(res, "第 3 轮搬移"), 2);
    assert_eq!(segm.least_count(), 0);
    assert_eq!(output.calls_, 3);

    // 第 4 轮：段已空 → 搬 0 个，且不应触碰设备。
    let demand = Demand::no_more_than(3);
    let res = segm.move_items_to_output_async(&mut output, &demand).await;
    assert_eq!(left_of_(res, "第 4 轮搬移"), 0, "空段不应搬移任何数据");
    assert_eq!(output.pos_, 5, "前三轮共写入 5 字节");
    assert_eq!(output.calls_, 3, "第 4 轮不应触碰设备");
    drop(segm);

    assert_eq!(std::fs::read(&path).expect("读回输出文件"), data);
    assert_eq!(consumed, 5);
    std::fs::remove_file(&path).ok();
}

/// 验证 `SegmRef::move_items_to_output_async` 在需求与段内可搬移量无交集时不搬移。
/// - 测试目标：需求与「段内可搬移量」取交集失败（`Demand::compromise` 返回
///   `None`）时，方法立即返回 0，且不改动段状态、不触碰输出设备。
/// - 测试手段：3 字节段，先配 `Demand::exactly(5)`（下界 5 大于可搬量 3），再配
///   `Demand::at_least(4)`（下界 = 可搬量 + 1，两集合恰好相接），各对真实文件输出调用
///   一次。
/// - 判定标准：两次返回值都为 0；段剩余量始终为 3；设备写入位置始终为 0，读回的文件为
///   空；段 drop 后回收量为 0（段未被消费）。
#[compio::test]
async fn segm_ref_move_items_to_output_async_moves_nothing_when_unmeetable() {
    let path = tmp_path_("ref_step_unmeet");
    let file = File::create(&path).await.expect("创建输出文件");
    let mut output = FileOutput::new_(file, 0, usize::MAX);

    let data: Vec<u8> = vec![7, 8, 9];
    let mut consumed = 0usize;
    let mut segm = SegmRef::new(
        &data[..],
        SegmReclaim::new(Pin::new(&mut consumed)),
    );

    let demand = Demand::exactly(5);
    let res = segm.move_items_to_output_async(&mut output, &demand).await;
    assert_eq!(left_of_(res, "需求不可满足时"), 0);
    assert_eq!(segm.least_count(), 3, "需求不可满足时不应消费段");
    assert_eq!(output.pos_, 0, "设备不应收到任何写入");

    // 需求下界 = 可搬量 + 1：两集合恰好相接（交集为空），必须返回 0 而不是 panic。
    let demand = Demand::at_least(4);
    let res = segm.move_items_to_output_async(&mut output, &demand).await;
    assert_eq!(left_of_(res, "恰好相接时"), 0);
    assert_eq!(segm.least_count(), 3, "恰好相接时也不应消费段");
    assert_eq!(output.pos_, 0);
    drop(segm);

    assert!(
        std::fs::read(&path).expect("读回输出文件").is_empty(),
        "文件应保持为空"
    );
    assert_eq!(consumed, 0);
    std::fs::remove_file(&path).ok();
}

// ---------------------------------------------------------------------------
// 2. SegmMut::move_items_from_input_async（关联函数，单步）
// ---------------------------------------------------------------------------

/// 验证 `SegmMut::move_items_from_input_async` 单步搬移把真实文件数据读进段的空位。
/// - 测试目标：按需求上限读取、短读后继续读、EOF 以可终止错误上报，并按实际读入量
///   推进段的已写入量。
/// - 测试手段：输入是内容为 `b"HELLO"`（5 字节）的真实文件，设备的单次读取上限设为
///   2 字节以制造短读；段有 6 个空位；依次以 `exactly(3)`、`at_least(0)`（无上界）、
///   `at_least(0)` 调用该关联函数（方法调用语法优先选中固有实现）。
/// - 判定标准：第 1 轮搬入 3 字节（内部由 2 + 1 两次短读完成，设备被调用 2 次）且段
///   剩余 3；第 2 轮搬入 2 字节并附带 `ReadErrTag::Closing` 错误（设备再被调用 2 次：
///   一次读到 2 字节、一次探到 EOF）、段剩余 1；第 3 轮搬入 0 且同样附带 `Closing`
///   （设备第 5 次被调用）、段剩余量不变；段 drop 后回收量为 5，底层缓冲前 5 个槽位恰为
///   `b"HELLO"`。
#[compio::test]
async fn segm_mut_move_items_from_input_async_fills_slots_from_file() {
    let path = tmp_path_("mut_step_in");
    std::fs::write(&path, b"HELLO").expect("写入测试夹具");
    let file = File::open(&path).await.expect("打开输入文件");
    let mut input = FileInput::new_(file, 0, 2);

    let mut storage = [MaybeUninit::<u8>::uninit(); 6];
    let mut consumed = 0usize;
    let mut segm = SegmMut::new(
        &mut storage[..],
        SegmReclaim::new(Pin::new(&mut consumed)),
    );

    // 第 1 轮：`exactly(3)` → 由两次短读（2 + 1）凑满。
    let demand = Demand::exactly(3);
    let res = segm.move_items_from_input_async(&mut input, &demand).await;
    assert_eq!(left_of_(res, "第 1 轮读取"), 3);
    assert_eq!(segm.least_count(), 3);
    assert_eq!(input.calls_, 2, "限长 2 时应由 2 + 1 两次短读凑满 3 字节");

    // 第 2 轮：段内还剩 3 个空位、文件只剩 2 字节 → 搬入 2 个后遇 EOF 报 Closing。
    let demand = Demand::at_least(0);
    let res = segm.move_items_from_input_async(&mut input, &demand).await;
    assert_eq!(res.as_ref().pick_left().copied(), Option::Some(2));
    assert_eq!(
        res.as_ref().pick_right().map(|e| e.tag()),
        Option::Some(ReadErrTag::Closing),
        "文件读完后必须以 EOF（Closing）上报"
    );
    assert_eq!(segm.least_count(), 1);
    assert_eq!(input.calls_, 4, "第 2 轮：一次读到 2 字节，一次探到 EOF");

    // 第 3 轮：文件已到末尾 → 搬入 0 个，段剩余空位数不变。
    let demand = Demand::at_least(0);
    let res = segm.move_items_from_input_async(&mut input, &demand).await;
    assert_eq!(left_of_(res, "第 3 轮读取"), 0);
    assert_eq!(segm.least_count(), 1, "未搬入任何数据，剩余空位不变");
    assert_eq!(input.calls_, 5, "第 3 轮只应探一次 EOF");
    drop(segm);

    assert_eq!(read_init_(&storage, 5), b"HELLO".to_vec());
    assert_eq!(consumed, 5);
    std::fs::remove_file(&path).ok();
}

/// 验证 `SegmMut::move_items_from_input_async` 在段内没有空位时既不搬移也不读取。
/// - 测试目标：段内可写入量为 0 时，需求取交集得到上限 0，方法立即返回 0，不触碰
///   输入设备。
/// - 测试手段：以 0 长度的堆上数组作为段缓冲，输入设备是内容为 `b"XY"` 的真实文件；
///   以 `no_more_than(8)` 调用一次。
/// - 判定标准：返回值为 0；段剩余量为 0；设备读取位置仍为 0、设备调用次数为 0
///   （完全未被读取）；段 drop 后回收量为 0。
#[compio::test]
async fn segm_mut_move_items_from_input_async_moves_nothing_into_full_target() {
    let path = tmp_path_("mut_step_full");
    std::fs::write(&path, b"XY").expect("写入测试夹具");
    let file = File::open(&path).await.expect("打开输入文件");
    let mut input = FileInput::new_(file, 0, usize::MAX);

    let mut empty: [MaybeUninit<u8>; 0] = [];
    let mut consumed = 0usize;
    let mut segm = SegmMut::new(
        &mut empty[..],
        SegmReclaim::new(Pin::new(&mut consumed)),
    );

    let demand = Demand::no_more_than(8);
    let res = segm.move_items_from_input_async(&mut input, &demand).await;
    assert_eq!(left_of_(res, "空段读取"), 0);
    assert_eq!(segm.least_count(), 0);
    assert_eq!(input.pos_, 0, "不应触碰输入设备");
    assert_eq!(input.calls_, 0, "设备一次也不应被调用");
    drop(segm);

    assert_eq!(consumed, 0);
    std::fs::remove_file(&path).ok();
}

// ---------------------------------------------------------------------------
// 3. TrBuffSegmRef::move_items_into_output_async（provided method，循环）
// ---------------------------------------------------------------------------

/// 验证 `TrBuffSegmRef::move_items_into_output_async` 的循环重试与搬移量聚合。
/// - 测试目标：provided method 循环调用单步搬移，直到段被消费空或达到需求允许的最大
///   数量（含端点）；把各轮搬移量累加后返回；遇到「暂时写不进去」的可重试错误
///   （`WriteErrTag::Stuffed`）继续重试，而不是把错误抛给调用方。
/// - 测试手段：5 字节段 `[1, 2, 3, 4, 5]` 配真实文件输出设备，设备前 3 次写入返回
///   `Stuffed`（不落盘），此后正常写入；第 1 次调用需求为 `exactly(3)`（恰好 3 个），
///   第 2 次为 `at_least(1)`（把剩余搬完），第 3 次在已空的段上再调用一次。
/// - 判定标准：三次返回的搬移量依次为 3、2、0；段剩余量依次为 2、0、0；第 1 次调用后
///   设备的 3 次可重试错误被循环全部消化（`stalls_left_ == 0`）且随后写入成功；三次
///   调用共触碰设备 5 次（4 次属于第 1 次调用、1 次属于第 2 次调用，第 3 次调用不触碰
///   设备）；读回的文件恰好为 `[1, 2, 3, 4, 5]`；段 drop 后回收量为 5。
#[compio::test]
async fn tr_buff_segm_ref_move_items_into_output_async_retries_and_aggregates() {
    let path = tmp_path_("ref_provided_out");
    let file = File::create(&path).await.expect("创建输出文件");
    // 前 3 次写入返回可重试的「写不进去」，用于驱动 provided method 的重试分支。
    let mut output = FileOutput::new_(file, 3, usize::MAX);

    let data: Vec<u8> = vec![1, 2, 3, 4, 5];
    let mut consumed = 0usize;
    let mut segm = SegmRef::new(
        &data[..],
        SegmReclaim::new(Pin::new(&mut consumed)),
    );

    // 第 1 次调用（显式走 trait provided method）：需求恰好 3 个，3 轮重试后写入 3 个。
    let demand = Demand::exactly(3);
    let res = TrBuffSegmRef::move_items_into_output_async(
        &mut segm, &mut output, &demand,
    )
    .await;
    assert_eq!(left_of_(res, "第 1 次调用"), 3);
    assert_eq!(segm.least_count(), 2);
    assert_eq!(output.stalls_left_, 0, "3 次可重试错误应被循环全部消化");
    assert_eq!(
        output.calls_, 4,
        "第 1 次调用：3 次返回可重试错误 + 1 次真正写入"
    );

    // 第 2 次调用：`at_least(1)` 没有上限 → 循环把段内剩余 2 个搬完。
    let demand = Demand::at_least(1);
    let res = TrBuffSegmRef::move_items_into_output_async(
        &mut segm, &mut output, &demand,
    )
    .await;
    assert_eq!(left_of_(res, "第 2 次调用"), 2);
    assert_eq!(segm.least_count(), 0);

    // 第 3 次调用：段已空 → 循环入口即退出，返回 0。
    let demand = Demand::at_least(1);
    let res = TrBuffSegmRef::move_items_into_output_async(
        &mut segm, &mut output, &demand,
    )
    .await;
    assert_eq!(left_of_(res, "第 3 次调用"), 0, "空段应立即返回 0");
    assert_eq!(
        output.calls_, 5,
        "第 2 次调用再写 1 次；第 3 次调用不触碰设备"
    );
    drop(segm);

    assert_eq!(std::fs::read(&path).expect("读回输出文件"), data);
    assert_eq!(consumed, 5);
    std::fs::remove_file(&path).ok();
}

// ---------------------------------------------------------------------------
// 4. TrBuffSegmMut::move_items_from_input_async（provided method，循环）
// ---------------------------------------------------------------------------

/// 验证 `TrBuffSegmMut::move_items_from_input_async` 的循环重试与搬移量聚合。
/// - 测试目标：provided method 循环调用单步读取，直到段被填满、输入耗尽或达到需求
///   上限；把各轮搬入量累加后返回；遇到「暂时读不到」的可重试错误
///   （`ReadErrTag::Drained`）继续重试，遇到可终止的 EOF（`ReadErrTag::Closing`）则在
///   满足需求下界的前提下正常结束。
/// - 测试手段：8 个空位的段配内容为 `b"ABCDEF"`（6 字节）的真实文件输入设备，设备前
///   2 次读取返回 `Drained`；第 1 次调用需求为 `exactly(3)`（恰好 3 个），第 2 次为
///   `at_least(1)`。
/// - 判定标准：两次返回的搬移量依次为 3、3；段剩余量依次为 5、2；第 1 次调用后设备的
///   2 次可重试错误被循环全部消化（`stalls_left_ == 0`）；设备共被调用 5 次（2 次重试
///   + 1 次真读 + 1 次读到 3 字节 + 1 次探到 EOF），读取位置最终为 6；段 drop 后回收量
///   为 6；底层 8 个槽位的前 6 个恰为 `b"ABCDEF"`。
#[compio::test]
async fn tr_buff_segm_mut_move_items_from_input_async_retries_and_aggregates() {
    let path = tmp_path_("mut_provided_in");
    std::fs::write(&path, b"ABCDEF").expect("写入测试夹具");
    let file = File::open(&path).await.expect("打开输入文件");
    // 前 2 次读取返回可重试的「读不到」，用于驱动 provided method 的重试分支。
    let mut input = FileInput::new_(file, 2, usize::MAX);

    let mut storage = [MaybeUninit::<u8>::uninit(); 8];
    let mut consumed = 0usize;
    let mut segm = SegmMut::new(
        &mut storage[..],
        SegmReclaim::new(Pin::new(&mut consumed)),
    );

    // 第 1 次调用（显式走 trait provided method）：需求恰好 3 个，2 轮重试后搬入 3 个。
    let demand = Demand::exactly(3);
    let res = TrBuffSegmMut::move_items_from_input_async(
        &mut segm, &mut input, &demand,
    )
    .await;
    assert_eq!(left_of_(res, "第 1 次调用"), 3);
    assert_eq!(segm.least_count(), 5);
    assert_eq!(input.stalls_left_, 0, "2 次可重试错误应被循环全部消化");
    assert_eq!(input.calls_, 3, "第 1 次调用：2 次返回可重试错误 + 1 次真读");

    // 第 2 次调用：`at_least(1)` 无上限 → 搬入剩余 3 字节；随后底层读到 EOF
    // （Closing，可终止），因已满足下界 1 而正常结束。
    let demand = Demand::at_least(1);
    let res = TrBuffSegmMut::move_items_from_input_async(
        &mut segm, &mut input, &demand,
    )
    .await;
    assert_eq!(left_of_(res, "第 2 次调用"), 3);
    assert_eq!(segm.least_count(), 2);
    assert_eq!(input.pos_, 6, "文件已被读完");
    assert_eq!(
        input.calls_, 5,
        "第 2 次调用：1 次读到 3 字节 + 1 次探到 EOF"
    );
    drop(segm);

    assert_eq!(read_init_(&storage, 6), b"ABCDEF".to_vec());
    assert_eq!(consumed, 6);
    std::fs::remove_file(&path).ok();
}

// ---------------------------------------------------------------------------
// 边界：需求与可搬量恰好相等
// ---------------------------------------------------------------------------

/// 验证「需求与可搬量恰好相等」这一边界：交集是单点集合，必须正好搬 n 个。
/// - 测试目标：`exactly(n)` 与「恰好 n 个可供搬移」求交得到 `{n}`（内部是起点 n、长度 1），
///   搬移量恰为 n，且两侧都不需要为凑数多调一次设备。
/// - 测试手段：输出方向用 3 字节段 + `exactly(3)` 写真实文件；输入方向用恰好 3 个空位的
///   段 + 内容 3 字节的真实文件 + `exactly(3)`。
/// - 判定标准：两次搬移量都为 3；段剩余量都为 0；设备都只被调用 1 次；两侧回收量都为 3；
///   文件内容各自正确。
#[compio::test]
async fn exactly_n_meets_exactly_n_available_items() {
    // -- 输出方向：段内恰好 3 个，需求 exactly(3) --
    let out_path = tmp_path_("exact_fit_out");
    let file = File::create(&out_path).await.expect("创建输出文件");
    let mut output = FileOutput::new_(file, 0, usize::MAX);
    let data: Vec<u8> = vec![7, 8, 9];
    let mut out_consumed = 0usize;
    let mut segm = SegmRef::new(
        &data[..],
        SegmReclaim::new(Pin::new(&mut out_consumed)),
    );
    let demand = Demand::exactly(3);
    let res = segm.move_items_to_output_async(&mut output, &demand).await;
    assert_eq!(left_of_(res, "恰好 3 个可用时"), 3);
    assert_eq!(segm.least_count(), 0);
    assert_eq!(output.calls_, 1, "一次写满即可");
    drop(segm);
    assert_eq!(std::fs::read(&out_path).expect("读回输出文件"), data);
    assert_eq!(out_consumed, 3);
    std::fs::remove_file(&out_path).ok();

    // -- 输入方向：恰好 3 个空位，文件恰好 3 字节，需求 exactly(3) --
    let in_path = tmp_path_("exact_fit_in");
    std::fs::write(&in_path, b"XYZ").expect("写入测试夹具");
    let file = File::open(&in_path).await.expect("打开输入文件");
    let mut input = FileInput::new_(file, 0, usize::MAX);
    let mut storage = [MaybeUninit::<u8>::uninit(); 3];
    let mut in_consumed = 0usize;
    let mut segm = SegmMut::new(
        &mut storage[..],
        SegmReclaim::new(Pin::new(&mut in_consumed)),
    );
    let demand = Demand::exactly(3);
    let res = segm.move_items_from_input_async(&mut input, &demand).await;
    assert_eq!(left_of_(res, "恰好 3 个空位时"), 3);
    assert_eq!(segm.least_count(), 0);
    assert_eq!(input.calls_, 1, "读满需求即止，不会多探一次 EOF");
    assert_eq!(input.pos_, 3);
    drop(segm);
    assert_eq!(read_init_(&storage, 3), b"XYZ".to_vec());
    assert_eq!(in_consumed, 3);
    std::fs::remove_file(&in_path).ok();
}

// ---------------------------------------------------------------------------
// 边界：空需求（取值集合为空集）
// ---------------------------------------------------------------------------

/// 验证空需求（`less_than(0)`）在两个 provided method 上都"一个都不搬"。
/// - 测试目标：`Demand::less_than(0)` 的取值集合是空集，其 `min()` / `max()` 都是
///   `None`；聚合循环不能把这一对 `None` 按"至少 1 个、无上界"的缺省解释处理，否则会
///   无视需求把段内数据全部搬走。
/// - 测试手段：输出方向用 3 字节段配真实文件输出设备、输入方向用 3 个空位的段配内容
///   `b"XYZ"` 的真实文件输入设备，两边都以 `less_than(0)` 调用对应的 provided method。
/// - 判定标准：两次返回的搬移量都是 0；段剩余量不变（3 / 3）；两个设备一次都没被调用
///   （`calls_ == 0`、`pos_ == 0`）；输出文件为空、输入文件未被读取；两侧回收量都为 0。
#[compio::test]
async fn empty_demand_moves_nothing() {
    let demand = Demand::less_than(0);
    assert!(demand.is_empty(), "less_than(0) 应当是空需求");
    assert_eq!(
        (demand.min(), demand.max()),
        (Option::None, Option::None),
        "空集的 min/max 都是 None，这正是必须显式判空的原因"
    );

    // -- 输出方向：TrBuffSegmRef::move_items_into_output_async --
    let out_path = tmp_path_("empty_out");
    let file = File::create(&out_path).await.expect("创建输出文件");
    let mut output = FileOutput::new_(file, 0, usize::MAX);
    let data: Vec<u8> = vec![1, 2, 3];
    let mut out_consumed = 0usize;
    let mut out_segm = SegmRef::new(
        &data[..],
        SegmReclaim::new(Pin::new(&mut out_consumed)),
    );

    let res = TrBuffSegmRef::move_items_into_output_async(
        &mut out_segm, &mut output, &demand,
    )
    .await;
    assert_eq!(left_of_(res, "空需求（输出方向）"), 0);
    assert_eq!(out_segm.least_count(), 3, "空需求不应消费段");
    assert_eq!(output.calls_, 0, "空需求不应触碰输出设备");
    assert_eq!(output.pos_, 0);
    drop(out_segm);

    assert!(
        std::fs::read(&out_path).expect("读回输出文件").is_empty(),
        "文件应保持为空"
    );
    assert_eq!(out_consumed, 0);
    std::fs::remove_file(&out_path).ok();

    // -- 输入方向：TrBuffSegmMut::move_items_from_input_async --
    let in_path = tmp_path_("empty_in");
    std::fs::write(&in_path, b"XYZ").expect("写入测试夹具");
    let file = File::open(&in_path).await.expect("打开输入文件");
    let mut input = FileInput::new_(file, 0, usize::MAX);
    let mut storage = [MaybeUninit::<u8>::uninit(); 3];
    let mut in_consumed = 0usize;
    let mut in_segm = SegmMut::new(
        &mut storage[..],
        SegmReclaim::new(Pin::new(&mut in_consumed)),
    );

    let res = TrBuffSegmMut::move_items_from_input_async(
        &mut in_segm, &mut input, &demand,
    )
    .await;
    assert_eq!(left_of_(res, "空需求（输入方向）"), 0);
    assert_eq!(in_segm.least_count(), 3, "空需求不应写入段");
    assert_eq!(input.calls_, 0, "空需求不应触碰输入设备");
    assert_eq!(input.pos_, 0);
    drop(in_segm);

    assert_eq!(in_consumed, 0);
    std::fs::remove_file(&in_path).ok();
}
