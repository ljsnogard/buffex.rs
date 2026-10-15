//! `ring::Ring` 的冒烟测试。
//!
//! # 运行时与驱动方式
//!
//! 需要异步的用例一律写成普通 `async fn`，紧随其后用
//! [`dual_runtime_test_`](crate::test_support_::dual_runtime_test_) 生成 **tokio** 与
//! **compio** 两个**真实运行时**下的测试方法；不在用例里 `block_on`、也不手动构造
//! waker 轮询——那样测到的是「假想的世界」，而不是真实运行时里由 waker 驱动的行为。
//!
//! # 覆盖范围
//!
//! 覆盖环形缓冲的常见冒烟场景：读写往返、空 / 满边界（含 REVERSION 约定下「满环」
//! 与「空环」的区分）、`Demand` 的下限 / 上限语义、跨末端的两段式段、多轮 FIFO 顺序，
//! 以及异步入口在条件不满足时确实 park、在取消令牌已就绪时立即返回 `Cancelled`。
//!
//! 自 `Ring::split` 起，读写两端可以拆成 [`RingWriter`] / [`RingReader`] 分别持有，
//! 因而能像 `circular_buff` 那样写「一端 park、另一端提交后唤醒」的 `join!` 并发用例
//! （见「分拆后的并发读写」一节）。
//!
//! # park 复用与「写者只在真正腾出空间后被唤醒」（曾为缺陷，现为回归用例）
//!
//! park future 在挂起状态下被 drop（取消 / `select` 落败）时，`RingHalf_::release_` 会在
//! `Drop` 里清空 `wake_slot_` 并释放 `opt_demand_`；写端的唤醒判定按 `free_size()`（而非
//! `data_size()`）与等待中的 `Demand` 下限比较。任一项回退，`ring_*_async` 的循环都会在
//! **一次 poll 内同步空转**（不让出也不返回，表现为任务挂死）；本节末尾的两个回归用例以
//! 「查询预算令牌」把空转截断成**有界失败**。
//!
//! # 关闭（EOF）
//!
//! `Ring::close_producer` / `close_consumer`（以及两个半部的 `close`）把状态字里的
//! `PRODUCER_CLOSED` / `CONSUMER_CLOSED` 置起来并唤醒对端，于是「设备结束 / 收尾」不再
//! 需要调用方自己造通知：环里还有数据就照常交付，取空之后读侧拿 `Closing`、写侧一律
//! `Closing`。副作用是 `try_read_internal_` 的「EOF 例外」必须兑现——写端已关闭时即使
//! 不足 `Demand` 下限也要交付现有部分，否则「已关闭 ⇒ `can_consume_` 为真」会让 park
//! 循环在一次 poll 内同步空转；本条以查询预算令牌把空转截断成**有界失败**。

use core::{
    future::{self, IntoFuture},
    mem::MaybeUninit,
};

use std::{
    boxed::Box,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
    vec,
    vec::Vec,
};

use abs_buff::{
    Demand,
    buffer::{TrBuffSegmMut, TrConsumerState, TrProducerState, TrReclaim},
    x_deps::abs_cancel::{CancelledToken, TrMayCancel, TrCancellationToken},
};

use crate::{
    ring::{reclaim::ReclSliceRef, *},
    test_support_::dual_runtime_test_,
};

// ---------------------------------------------------------------------------
// 测试辅助
// ---------------------------------------------------------------------------

/// 测试用缓冲类型：`Box<[MaybeUninit<u8>]>`（元素 `u8`）。
type TestBuff = Box<[MaybeUninit<u8>]>;

/// 测试用 `Ring` 具体类型。
type TestRing = Ring<TestBuff, u8>;

/// `Ring::split` 出的写端（借用 `Ring`）。
type TestWriter<'f> =
    RingWriter<&'f TestRing, TestBuff, u8>;

/// `Ring::split` 出的读端（借用 `Ring`）。
type TestReader<'f> =
    RingReader<&'f TestRing, TestBuff, u8>;

/// 构造一个指定容量的测试环（容量须落在 `Ring` 允许的
/// `[MIN_CAPACITY, MAX_CAPACITY]` 内，见 [`Ring::check_buffer_size`]）。
fn new_ring_(capacity: usize) -> TestRing {
    let buff = Box::<[u8]>::new_uninit_slice(capacity);
    Ring::new_unchecked(buff)
}

/// 借出写段、把 `data` 位拷贝进环，drop 提交后返回实际写入的元素数。
async fn fill_bytes_(ring: &mut TestRing, demand: &Demand<usize>, data: &[u8]) -> usize {
    let some = ring.write_async(demand).await;
    let mut segm = some.pick_left().expect("应能借出写段");
    let moved = segm.move_items_from_as_buff(data);
    drop(segm); // 提交：按已写入量推进写位置
    moved
}

/// 借出读段、取出 `len` 个元素，drop 提交后返回取出的字节序列。
async fn take_bytes_(ring: &mut TestRing, demand: &Demand<usize>, len: usize) -> Vec<u8> {
    let some = ring.read_async(demand).await;
    let mut segm = some.pick_left().expect("应能借出读段");
    let got = take_segm_bytes_(&mut segm, len).await;
    drop(segm); // 提交：按已消费量推进读位置
    got
}

/// 从读段取出 `len` 个元素（段 drop 时按已消费量推进读位置）。
async fn take_segm_bytes_<R>(segm: &mut ReclSliceRef<'_, u8, R>, len: usize) -> Vec<u8>
where
    R: TrReclaim,
{
    assert!(len <= segm.least_count(), "取出量超过段内可读量");

    let mut staging = Vec::<MaybeUninit<u8>>::with_capacity(len);
    staging.resize(len, MaybeUninit::uninit());
    let moved = unsafe { segm.move_items_to_buff(&mut staging) };
    assert_eq!(moved, len, "应恰好取出 len 个元素");

    // SAFETY: 上面已把前 `len` 个 `MaybeUninit<u8>` 初始化；`u8` 为位拷贝且无 drop 资源。
    staging
        .into_iter()
        .map(|m| unsafe { m.assume_init() })
        .collect()
}

/// 通过写端借出写段并写入 `data`，drop 提交后返回实际写入的元素数。
async fn tx_write_(tx: &mut TestWriter<'_>, demand: &Demand<usize>, data: &[u8]) -> usize {
    let some = tx.write_async(demand).await;
    let mut segm = some.pick_left().expect("应能借出写段");
    let moved = segm.move_items_from_as_buff(data);
    drop(segm); // 提交：推进写位置并触发消费端事件
    moved
}

/// 通过读端借出读段并取出 `len` 个元素，drop 提交后返回取出的字节序列。
async fn rx_read_(rx: &mut TestReader<'_>, demand: &Demand<usize>, len: usize) -> Vec<u8> {
    let some = rx.read_async(demand).await;
    let mut segm = some.pick_left().expect("应能借出读段");
    let got = take_segm_bytes_(&mut segm, len).await;
    drop(segm); // 提交：推进读位置并触发生产端事件
    got
}

/// 测试用取消令牌：`is_cancelled()` 被查询满 `budget_` 次之后才「取消」。
///
/// # 用途
///
/// 若 park 的唤醒槽位 / demand 绑定没有在 drop 时释放，或写端的空间判定回退成
/// `data_size()`，读 / 写循环就会在**一次 poll 内同步空转**、不让出也不返回——直接跑会
/// 挂死。把预算调到很小的值，空转就会在有限次查询后以 `Cancelled` 结束，于是回归表现为
/// **有界失败**而不是挂死。
///
/// `cancellation()` 返回永不就绪的 future：正常实现里首次 park 必须继续挂起，不能被这个
/// 令牌「提前放行」，否则用例无法通过。
#[derive(Clone)]
struct BudgetToken {
    checks_: Arc<AtomicUsize>,
    budget_: usize,
}

impl BudgetToken {
    /// 构造一个预算为 `budget` 次的令牌（父令牌与子令牌共享同一计数）。
    fn new(budget: usize) -> Self {
        BudgetToken {
            checks_: Arc::new(AtomicUsize::new(0)),
            budget_: budget,
        }
    }

    /// 已发生的取消状态查询次数（含父令牌与子令牌）。
    fn checks(&self) -> usize {
        self.checks_.load(Ordering::Acquire)
    }
}

impl TrCancellationToken for BudgetToken {
    type Cancellation = future::Pending<()>;
    type ChildToken = BudgetToken;

    fn is_cancelled(&self) -> bool {
        self.checks_.fetch_add(1, Ordering::AcqRel) + 1 >= self.budget_
    }

    fn can_be_cancelled(&self) -> bool {
        true
    }

    fn child_token(&self) -> Self::ChildToken {
        self.clone()
    }

    fn cancellation(self) -> Self::Cancellation {
        future::pending()
    }
}

// ---------------------------------------------------------------------------
// 基础读写（保留的起步用例）
// ---------------------------------------------------------------------------

/// 同步读写入口在真实运行时下的最简往返。
/// - 测试目标：`Ring::try_write` / `try_read` 借出的读写段能把数据原样搬进、搬出。
/// - 测试手段：容量 8 上借写段写入 `[1, 2, 4, 16]` 并提交，再借读段取回。
/// - 判定标准：取回的字节序列与写入序列完全一致。
async fn smoke_test_sync_() {
    const BUFF_SIZE: usize = 8;
    let buff = Box::<[u8]>::new_uninit_slice(BUFF_SIZE);
    let mut ring = Ring::new_unchecked(buff);
    {
        let w_demand = Demand::no_more_than(BUFF_SIZE);
        let w_x = ring.try_write(&w_demand);
        let mut w_segm = w_x.pick_left().unwrap();

        let msg = [1u8, 2u8, 4u8, 16u8];
        w_segm.move_items_from_as_buff(&msg);
    }
    {
        let r_demand = Demand::no_more_than(BUFF_SIZE);
        let r_x = ring.try_read(&r_demand);
        let mut r_segm = r_x.pick_left().unwrap();

        let mut msg = [MaybeUninit::<u8>::uninit(); BUFF_SIZE];
        let size = unsafe { r_segm.move_items_to_buff(&mut msg) };

        let vec: Vec<u8> = msg
            .iter()
            .map(|m| unsafe { m.assume_init_read() })
            .collect();
        assert_eq!(
            &vec.as_slice()[..size],
            [1u8, 2u8, 4u8, 16u8].as_slice(),
        )
    }
}

dual_runtime_test_!(smoke_test_sync_);

/// 异步读写入口在真实运行时下的最简往返。
/// - 测试目标：`Ring::write_async` / `read_async` 在数据 / 空间就绪时就地完成。
/// - 测试手段：容量 8 上 `write_async` 写入 `[1, 2, 4, 16]`，再 `read_async` 取回。
/// - 判定标准：取回的字节序列与写入序列完全一致。
async fn smoke_test_async_() {
    const BUFF_SIZE: usize = 8;
    let buff = Box::<[u8]>::new_uninit_slice(BUFF_SIZE);
    let mut ring = Ring::new_unchecked(buff);
    {
        let w_demand = Demand::no_more_than(BUFF_SIZE);
        let w_x = ring.write_async(&w_demand).await;
        let mut w_segm = w_x.pick_left().unwrap();

        let msg = [1u8, 2u8, 4u8, 16u8];
        w_segm.move_items_from_as_buff(&msg);
    }
    {
        let r_demand = Demand::no_more_than(BUFF_SIZE);
        let r_x = ring.read_async(&r_demand).await;
        let mut r_segm = r_x.pick_left().unwrap();

        let mut msg = [MaybeUninit::<u8>::uninit(); BUFF_SIZE];
        let size = unsafe { r_segm.move_items_to_buff(&mut msg) };

        let vec: Vec<u8> = msg
            .iter()
            .map(|m| unsafe { m.assume_init_read() })
            .collect();
        assert_eq!(
            &vec.as_slice()[..size],
            [1u8, 2u8, 4u8, 16u8].as_slice(),
        )
    }
}

dual_runtime_test_!(smoke_test_async_);

// ---------------------------------------------------------------------------
// 边界与 Demand 语义
// ---------------------------------------------------------------------------

/// 空环上的 `try_read` 必须返回 `Drained`，而不是空段或挂起。
/// - 测试目标：无数据时读入口不给出空段（否则调用方会拿到 0 长度却以为成功）。
/// - 测试手段：新建容量 8 的空环，直接以 `at_least(1)` 调用 `try_read`。
/// - 判定标准：返回 `ConsumerError::Drained`；`data_size == 0`、`free_size == 8`。
async fn try_read_empty_returns_drained_() {
    let mut ring = new_ring_(8);

    let demand = Demand::at_least(1);
    let some = ring.try_read(&demand);
    assert!(
        matches!(some.pick_right(), Some(ConsumerError::Drained(_))),
        "空环读应返回 Drained"
    );
    assert_eq!(ring.data_size(), 0, "空环数据量应为 0");
    assert_eq!(ring.free_size(), 8, "空环可写空间应为整个容量");
}

dual_runtime_test_!(try_read_empty_returns_drained_);

/// 满环上的 `try_write` 必须返回 `Stuffed`；且「满」不能被误判成「空」。
/// - 测试目标：REVERSION 约定下满环（`wp == rp && rv`）与空环（`wp == rp && !rv`）可区分。
/// - 测试手段：容量 4 上写满 4 个元素，断言状态并再试写一次；随后读空、再次写满。
/// - 判定标准：满环 `data_size == 4`、`free_size == 0`、再写返回 `Stuffed`；读空后
///   `data_size == 0`、`free_size == 4`，并且还能再次写满（位置可复用）。
async fn try_write_full_returns_stuffed_() {
    let mut ring = new_ring_(4);

    let written = fill_bytes_(&mut ring, &Demand::at_least(4), &[1, 2, 3, 4]).await;
    assert_eq!(written, 4, "空环应能一次写满");
    assert_eq!(ring.data_size(), 4, "满环应报告 capacity 个可读元素");
    assert_eq!(ring.free_size(), 0, "满环应无剩余可写空间");

    let demand = Demand::at_least(1);
    let some = ring.try_write(&demand);
    assert!(
        matches!(some.pick_right(), Some(ProducerError::Stuffed(_))),
        "满环写应返回 Stuffed"
    );

    let got = take_bytes_(&mut ring, &Demand::at_least(4), 4).await;
    assert_eq!(got, vec![1, 2, 3, 4], "满环读出应完整且有序");
    assert_eq!(ring.data_size(), 0, "读空后数据量应为 0");
    assert_eq!(ring.free_size(), 4, "读空后应恢复全部可写空间");

    let written = fill_bytes_(&mut ring, &Demand::at_least(4), &[5, 6, 7, 8]).await;
    assert_eq!(written, 4, "读空后应能再次写满");
}

dual_runtime_test_!(try_write_full_returns_stuffed_);

/// 数据量不足 `Demand` 下限时，`try_read` 必须返回 `Drained` 而不是部分段。
/// - 测试目标：读侧的 `at_least` 下限门控。
/// - 测试手段：容量 8 上写 2 个元素，先以 `at_least(4)` 读，再以 `at_least(2)` 读。
/// - 判定标准：不足下限返回 `Drained` 且数据仍在（`data_size == 2`）；下限满足时取回两个元素。
async fn try_read_honours_at_least_() {
    let mut ring = new_ring_(8);
    assert_eq!(fill_bytes_(&mut ring, &Demand::at_least(2), &[1, 2]).await, 2);

    let demand = Demand::at_least(4);
    let some = ring.try_read(&demand);
    assert!(
        matches!(some.pick_right(), Some(ConsumerError::Drained(_))),
        "数据不足下限时必须返回 Drained"
    );
    assert_eq!(ring.data_size(), 2, "被拒绝的读不应消费数据");

    let got = take_bytes_(&mut ring, &Demand::at_least(2), 2).await;
    assert_eq!(got, vec![1, 2], "满足下限后应取回全部数据");
}

dual_runtime_test_!(try_read_honours_at_least_);

/// 可写空间不足 `Demand` 下限时，`try_write` 必须返回 `Stuffed` 而不是部分段。
/// - 测试目标：写侧的 `at_least` 下限门控。
/// - 测试手段：容量 8 上写 5 个元素（剩 3 格），先以 `at_least(4)` 借写段，
///   再以 `at_least(3)` 借写段。
/// - 判定标准：不足下限返回 `Stuffed`；满足下限时能借出写段。
async fn try_write_honours_at_least_() {
    let mut ring = new_ring_(8);
    assert_eq!(fill_bytes_(&mut ring, &Demand::at_least(5), &[0u8; 5]).await, 5);
    assert_eq!(ring.free_size(), 3, "写 5 后应剩 3 格可写空间");

    let demand = Demand::at_least(4);
    let some = ring.try_write(&demand);
    assert!(
        matches!(some.pick_right(), Some(ProducerError::Stuffed(_))),
        "可写空间不足下限时必须返回 Stuffed"
    );

    let demand = Demand::at_least(3);
    let some = ring.try_write(&demand);
    assert!(some.pick_left().is_some(), "满足下限时应能借出写段");
}

dual_runtime_test_!(try_write_honours_at_least_);

/// `Demand` 上界限制单次借出的元素数，且未消费的段 drop 后不推进位置。
/// - 测试目标：`exactly`（下界 = 上界）限制单次借出量，以及「空借出（offset 为 0）不消费」。
/// - 测试手段：容量 8 上以 `at_least(5)` 写 5 个元素；以 `exactly(2)` 借读段，断言段长后
///   直接 drop；再以 `exactly(2)` 取走 2 个。
/// - 判定标准：单次段长恰为 2；drop 未消费的段后 `data_size` 仍为 5；取走后剩 3。
async fn try_read_limits_count_by_max_() {
    let mut ring = new_ring_(8);
    assert_eq!(fill_bytes_(&mut ring, &Demand::at_least(5), &[1, 2, 3, 4, 5]).await, 5);
    assert_eq!(ring.data_size(), 5);

    let demand = Demand::exactly(2);
    let some = ring.try_read(&demand);
    let segm = some.pick_left().expect("应能借出读段");
    assert_eq!(segm.least_count(), 2, "上界为 2 时应只借出 2 个元素");
    drop(segm); // offset 仍为 0：不得消费任何元素
    assert_eq!(ring.data_size(), 5, "未消费的读段 drop 后数据量应不变");

    let got = take_bytes_(&mut ring, &Demand::exactly(2), 2).await;
    assert_eq!(got, vec![1, 2]);
    assert_eq!(ring.data_size(), 3, "取走 2 个后应剩 3 个");
}

dual_runtime_test_!(try_read_limits_count_by_max_);

/// 构造入口的容量边界：**接受最小容量 1**，只拒绝不可用的容量 0。
/// - 测试目标：`Ring::try_new` 的容量校验（`MIN_CAPACITY == 1`）。
/// - 测试手段：分别以容量 0、1、2 的 `Box<[MaybeUninit<u8>]>` 调用 `try_new`。
/// - 判定标准：容量 0 返回 `Err(0)`（零长缓冲上任何读写都不可满足）；容量 1 构造成功
///   且 `capacity() == 1`（位置编码用 `REVERSION` 位区分空 / 满，不需要空出一格）；
///   容量 2 照旧被接受。
async fn try_new_accepts_min_capacity_one_() {
    let buff = Box::<[u8]>::new_uninit_slice(0);
    let x: Result<TestRing, usize> = Ring::try_new(buff);
    assert_eq!(x.err(), Some(0usize), "容量 0 应被拒绝");

    let buff = Box::<[u8]>::new_uninit_slice(1);
    let x: Result<TestRing, usize> = Ring::try_new(buff);
    let ring = x.expect("容量 1 应被接受");
    assert_eq!(ring.capacity(), 1, "最小容量的环容量就是 1");

    let buff = Box::<[u8]>::new_uninit_slice(2);
    let x: Result<TestRing, usize> = Ring::try_new(buff);
    let ring = x.expect("容量 2 应被接受");
    assert_eq!(ring.capacity(), 2);
}

dual_runtime_test_!(try_new_accepts_min_capacity_one_);

// ---------------------------------------------------------------------------
// 最小容量（1 格）的完整验收
//
// 容量 1 是位置编码（REVERSION 位区分空 / 满）允许的下限：`rp == wp == 0` 恒成立，
// 一切由 `rv` 承担；且**不存在实际绕行**——每次读写都走「跨末端」分支。下面四条把
// 这条边界钉住：往返、重新可写、关闭语义、park / 唤醒。
// ---------------------------------------------------------------------------

/// 容量 1 的环上写满 → 读空 → 再写，状态视图与数据都正确。
/// - 测试手段：容量 1 的环依次写入 42、读走 1 个、再写入 7、再读走 1 个；每步查
///   `consumer_state` / `producer_state`，并核对取回的字节。
/// - 判定标准：写满后读端看到 `(1, false)`、写端看到 `(0, false)`；读空后回到
///   `(0, false)`；**再写入必须仍然成功**（这正是 REVERSION 位在容量 1 下翻转的证据）；
///   两次取回的字节分别为 42 与 7。
async fn capacity_one_roundtrip_() {
    let mut ring = new_ring_(1usize);
    assert_eq!(ring.capacity(), 1usize);

    let demand = Demand::at_least(1);
    assert_eq!(fill_bytes_(&mut ring, &demand, &[42u8]).await, 1usize);
    assert_eq!(
        ring.consumer_state(),
        Option::Some((1usize, false)),
        "写满 1 格后读端应看到 1"
    );
    assert_eq!(
        ring.producer_state(),
        Option::Some((0usize, false)),
        "写满 1 格后写端应看到 0 格可写"
    );

    assert_eq!(take_bytes_(&mut ring, &demand, 1usize).await, vec![42u8]);
    assert_eq!(
        ring.consumer_state(),
        Option::Some((0usize, false)),
        "读空后读端应看到 0"
    );

    // 关键一步：容量 1 下写位置没有别的取值可用，只能靠 REVERSION 位区分「空 → 满」。
    assert_eq!(fill_bytes_(&mut ring, &demand, &[7u8]).await, 1usize);
    assert_eq!(
        ring.consumer_state(),
        Option::Some((1usize, false)),
        "再次写入后应当又能读到 1 个"
    );
    assert_eq!(take_bytes_(&mut ring, &demand, 1usize).await, vec![7u8]);
}

dual_runtime_test_!(capacity_one_roundtrip_);

/// 容量 1 下 `Demand` 的上下界仍被正确夹取（可用空间 1 格）。
/// - 测试手段：容量 1 的环上分别用 `at_least(1)`、`no_more_than(2)`、`exactly(1)`
///   借写段，检查借出的段长；写满后再用 `at_least(2)` 借写段。
/// - 判定标准：前三种都能借出长度 1 的段；写满后 `at_least(2)` 得不到写段
///   （`Unsatisfiable`——下限超过容量，属于调用方错误而非「暂时写不下」）。
async fn capacity_one_respects_demand_bounds_() {
    let mut ring = new_ring_(1usize);

    for demand in [
        Demand::at_least(1),
        Demand::no_more_than(2),
        Demand::exactly(1),
    ] {
        {
            let mut segm = ring
                .try_write(&demand)
                .pick_left()
                .expect("空环上这些 demand 都应能借出写段");
            assert_eq!(segm.least_count(), 1usize, "容量 1 的环一次只借得出 1 格");
            assert_eq!(segm.move_items_from_as_buff(&[1u8]), 1usize);
        }
        // 每次写完都读空，保证下一次借写段时环是空的。
        let take = Demand::exactly(1);
        assert_eq!(take_bytes_(&mut ring, &take, 1usize).await, vec![1u8]);
    }

    // 写满后：下限 2 超过容量 ⇒ `Unsatisfiable`，而不是「暂时写不下」的 `Stuffed`。
    let _ = fill_bytes_(&mut ring, &Demand::at_least(1), &[2u8]).await;
    let demand = Demand::at_least(2);
    let mut outcome = ring.try_write(&demand);
    assert!(
        outcome.as_mut().pick_left().is_none(),
        "环已满且 demand 下限为 2（超过容量）时不应借出写段"
    );
    let err = outcome
        .pick_right()
        .expect("借不出写段时必须给出错误");
    assert_eq!(
        err,
        ProducerError::Unsatisfiable,
        "下限超过容量应报 Unsatisfiable"
    );
}

dual_runtime_test_!(capacity_one_respects_demand_bounds_);

/// 容量 1 下「关闭生产端」的读侧语义：先交出缓存中的那 1 格，再报 EOF。
/// - 测试手段：容量 1 的环写入 1 格后 `close_producer()`；随后用同步与异步两个入口
///   各读一次（异步读到旧数据的那次用查询预算令牌截断潜在空转）。
/// - 判定标准：关闭后**仍能读走缓存中的 1 格**；读空后再读得到 `Closing`（EOF），
///   即「关闭 ⇒ 交出剩余」的 EOF 例外在容量 1 下同样成立。
async fn capacity_one_close_producer_yields_eof_() {
    let mut ring = new_ring_(1usize);
    let demand = Demand::at_least(1);
    assert_eq!(fill_bytes_(&mut ring, &demand, &[9u8]).await, 1usize);

    ring.close_producer();
    assert_eq!(
        take_bytes_(&mut ring, &demand, 1usize).await,
        vec![9u8],
        "关闭生产端后应仍能读走缓存中的那一格"
    );
    assert!(
        ring.try_read(&demand).pick_left().is_none(),
        "读空且生产端已关闭 ⇒ 读侧应报 EOF"
    );

    // 异步入口同样立刻收尾（用查询预算令牌把潜在空转截断成有界失败）。
    let token = BudgetToken::new(64);
    let mut outcome = ring.read_async(&demand).may_cancel_with(token).await;
    assert!(outcome.as_mut().pick_left().is_none());
    assert_eq!(
        outcome.pick_right().expect("必须给出错误"),
        ConsumerError::Closing
    );
}

dual_runtime_test_!(capacity_one_close_producer_yields_eof_);

/// 容量 1 下「读端 park、写端提交」的唤醒路径（跨 `split` 出来的两个半部）。
/// - 测试手段：容量 1 的环 `split` 成写端 / 读端；读端先 park 在 `read_async` 上，
///   让出一次执行权后写端写入 1 格并提交。
/// - 判定标准：读端被唤醒并拿到长度 1 的段、取到的字节正是写入值。
async fn capacity_one_park_then_write_() {
    let mut ring = new_ring_(1usize);
    let (mut tx, mut rx) = Ring::split(&mut ring);

    let reader = async {
        let demand = Demand::at_least(1);
        let mut outcome = rx.read_async(&demand).await;
        let segm = outcome
            .as_mut()
            .pick_left()
            .expect("写端提交后读端应被唤醒并拿到段");
        assert_eq!(segm.least_count(), 1usize, "容量 1 的环只可能有 1 格可读");
        assert_eq!(take_segm_bytes_(segm, 1usize).await, vec![5u8]);
    };
    let writer = async {
        // 先让读端 park，再写入：这样这条用例验的才是「唤醒」而不是「同步拿到」。
        futures_lite::future::yield_now().await;
        let demand = Demand::at_least(1);
        assert_eq!(tx_write_(&mut tx, &demand, &[5u8]).await, 1usize);
    };

    futures_util::join!(reader, writer);
}

dual_runtime_test_!(capacity_one_park_then_write_);

// ---------------------------------------------------------------------------
// 跨末端环绕
// ---------------------------------------------------------------------------

/// 写 / 读区域跨过缓冲区物理末端时，段被拆成两段物理空间，逻辑上仍是一段。
/// - 测试目标：跨末端环绕时段的物理拆分与数据顺序。
/// - 测试手段：容量 5 上写 3、读 2（此时 `wp == 3`、`rp == 2`），再以 `at_least(4)`
///   借写段（应为 `[3,5)` 与 `[0,2)` 两段）写入 3 个元素；随后以 `at_least(4)` 借读段
///   （应为 `[2,5)` 与 `[0,1)` 两段）读空。
/// - 判定标准：写段物理长度为 `[2, 2]`、读段为 `[3, 1]`；读回序列为 `[3, 10, 11, 12]`；
///   读空后 `data_size == 0`。
async fn wrap_around_two_pieces_() {
    let mut ring = new_ring_(5);

    // 写 [1, 2, 3]：wp = 3。
    assert_eq!(fill_bytes_(&mut ring, &Demand::at_least(3), &[1, 2, 3]).await, 3);
    // 读 [1, 2]：rp = 2，剩 1 个可读、4 格可写。
    assert_eq!(take_bytes_(&mut ring, &Demand::at_least(2), 2).await, vec![1, 2]);

    // 再借 4 格：物理上跨末端，应为 [3,5) 两格 + [0,2) 两格。
    let demand = Demand::at_least(4);
    let some = ring.write_async(&demand).await;
    let mut segm = some.pick_left().expect("跨末端也应一次借出全部 4 格");
    let lens: Vec<usize> = segm.iter_slices_mut().map(|s| s.len()).collect();
    assert_eq!(lens, vec![2, 2], "跨末端写段应拆成 [3,5) 与 [0,2)");
    assert_eq!(segm.move_items_from_as_buff(&[10u8, 11, 12]), 3);
    drop(segm);
    assert_eq!(ring.data_size(), 4, "写入 3 个后应共有 4 个可读元素");

    // 可读区同样跨末端：应为 [2,5) 三格 + [0,1) 一格。
    let demand = Demand::at_least(4);
    let some = ring.read_async(&demand).await;
    let segm = some.pick_left().expect("应有 4 个元素可读");
    let lens: Vec<usize> = segm.iter_slices().map(|s| s.len()).collect();
    assert_eq!(lens, vec![3, 1], "跨末端读段应拆成 [2,5) 与 [0,1)");
    drop(segm); // offset 为 0：不消费

    let got = take_bytes_(&mut ring, &Demand::at_least(4), 4).await;
    assert_eq!(got, vec![3, 10, 11, 12], "跨末端读出应保持写入顺序");
    assert_eq!(ring.data_size(), 0, "读空后数据量应为 0");
}

dual_runtime_test_!(wrap_around_two_pieces_);

/// 多轮写入 / 读出后仍保持严格 FIFO，位置在环绕与跨末端之间正确推进。
/// - 测试目标：反复「写入 3 个、读空」时 REVERSION 置位 / 清除与环绕位置不串数据。
/// - 测试手段：容量 4 上循环 40 轮，每轮写入 3 个由轮次派生的字节并立即读空。
/// - 判定标准：每轮写满 3、读回顺序与写入一致、读空后 `data_size == 0`。
async fn wrap_around_fifo_cycles_() {
    const CAP: usize = 4;
    let mut ring = new_ring_(CAP);

    for i in 0..40u32 {
        let base = (i * 3) as u8;
        let payload = [base, base.wrapping_add(1), base.wrapping_add(2)];

        let written = fill_bytes_(&mut ring, &Demand::at_least(3), &payload).await;
        assert_eq!(written, 3, "第 {i} 轮应写入 3 个元素");
        assert_eq!(ring.data_size(), 3, "第 {i} 轮写入后应有 3 个可读元素");

        let got = take_bytes_(&mut ring, &Demand::at_least(3), 3).await;
        assert_eq!(got, payload.to_vec(), "第 {i} 轮 FIFO 顺序应保持");
        assert_eq!(ring.data_size(), 0, "第 {i} 轮读空后数据量应为 0");
    }
}

dual_runtime_test_!(wrap_around_fifo_cycles_);

// ---------------------------------------------------------------------------
// 异步入口
// ---------------------------------------------------------------------------

/// 数据 / 空间都就绪时，异步读写入口在真实运行时里直接完成往返。
/// - 测试目标：`write_async` / `read_async` 的即时完成路径与 `try_*` 数据一致。
/// - 测试手段：容量 8 上 `write_async(&at_least(3))` 写入 `[1, 2, 3]`，
///   再 `read_async(&at_least(3))` 取回。
/// - 判定标准：写后 `data_size == 3`；读出序列为 `[1, 2, 3]`；读后 `data_size == 0`。
async fn async_write_read_roundtrip_() {
    let mut ring = new_ring_(8);

    {
        let demand = Demand::at_least(3);
        let some = ring.write_async(&demand).await;
        let mut segm = some.pick_left().expect("空环应可写");
        assert_eq!(segm.move_items_from_as_buff(&[1u8, 2, 3]), 3);
    }
    assert_eq!(ring.data_size(), 3, "异步写提交后应有 3 个可读元素");

    {
        let demand = Demand::at_least(3);
        let some = ring.read_async(&demand).await;
        let mut segm = some.pick_left().expect("应有 3 个元素可读");
        let got = take_segm_bytes_(&mut segm, 3).await;
        assert_eq!(got, vec![1, 2, 3], "异步读应取回写入序列");
    }
    assert_eq!(ring.data_size(), 0, "异步读提交后应已读空");
}

dual_runtime_test_!(async_write_read_roundtrip_);

/// 数据量低于 `Demand` 下限时，`read_async` 必须 park 而不是就绪。
/// - 测试目标：读侧异步等待在「不足下限」时不虚假就绪（并验证 park 不消费数据）。
/// - 测试手段：写 2 个元素后以 `at_least(5)` 发起 `read_async`，与「让出一次」的探测
///   并发：探测放在 `select` 左侧，因此第二次轮询只会看到探测就绪并短路，被 park 的
///   读等待不会被再次轮询（`ring` 的唤醒槽位当前只设不清，二次轮询会空转）。
/// - 判定标准：`select` 返回左侧（探测先完成）证明读等待仍挂起；`data_size` 仍为 2。
async fn read_async_parks_when_insufficient_() {
    let mut ring = new_ring_(8);
    assert_eq!(fill_bytes_(&mut ring, &Demand::at_least(2), &[7, 8]).await, 2);

    let demand = Demand::at_least(5); // 只有 2 个，低于下限
    {
        let read = ring.read_async(&demand).into_future();
        let probe = async { futures_lite::future::yield_now().await };
        futures_util::pin_mut!(probe, read);
        match futures_util::future::select(probe, read).await {
            futures_util::future::Either::Left(_) => {}
            futures_util::future::Either::Right(_) => {
                panic!("数据不足下限时读等待不得就绪（Demand 门控失效）")
            }
        }
    } // 读等待在此 drop，归还对 `ring` 的可变借用

    assert_eq!(ring.data_size(), 2, "park 不应消费数据");
}

dual_runtime_test_!(read_async_parks_when_insufficient_);

/// 可写空间为 0 时，`write_async` 必须 park 而不是就绪。
/// - 测试目标：写侧异步等待在满环时不虚假就绪（并验证 park 不释放空间）。
/// - 测试手段：容量 4 写满后以 `at_least(1)` 发起 `write_async`，与「让出一次」的探测
///   并发（探测在 `select` 左侧，保证被 park 的写等待只被轮询一次）。
/// - 判定标准：`select` 返回左侧证明写等待仍挂起；`free_size` 仍为 0。
async fn write_async_parks_when_full_() {
    let mut ring = new_ring_(4);
    assert_eq!(fill_bytes_(&mut ring, &Demand::at_least(4), &[1, 2, 3, 4]).await, 4);

    let demand = Demand::at_least(1);
    {
        let write = ring.write_async(&demand).into_future();
        let probe = async { futures_lite::future::yield_now().await };
        futures_util::pin_mut!(probe, write);
        match futures_util::future::select(probe, write).await {
            futures_util::future::Either::Left(_) => {}
            futures_util::future::Either::Right(_) => {
                panic!("满环时写等待不得就绪")
            }
        }
    } // 写等待在此 drop，归还对 `ring` 的可变借用

    assert_eq!(ring.free_size(), 0, "park 不应释放可写空间");
}

dual_runtime_test_!(write_async_parks_when_full_);

/// 取消令牌已就绪时，异步读写入口立即返回 `Cancelled`，不进入 park。
/// - 测试目标：`gen_may_cancel_future` 生成的 future 的主动取消路径。
/// - 测试手段：空环上以 `CancelledToken` 驱动 `read_async`；写满后以同样的令牌驱动
///   `write_async`。
/// - 判定标准：读侧返回 `ConsumerError::Cancelled`；写侧返回 `ProducerError::Cancelled`。
async fn async_ops_honour_cancellation_() {
    let mut ring = new_ring_(8);

    // 读侧：空环 + 已取消令牌 → 立即 Cancelled（未注册 waker）。
    let demand = Demand::at_least(1);
    let some = ring
        .read_async(&demand)
        .may_cancel_with(CancelledToken::new())
        .await;
    assert!(
        matches!(some.pick_right(), Some(ConsumerError::Cancelled)),
        "读等待应被取消令牌中止"
    );

    // 写侧：写满 + 已取消令牌 → 立即 Cancelled。
    assert_eq!(fill_bytes_(&mut ring, &Demand::at_least(8), &[0u8; 8]).await, 8);
    let demand = Demand::at_least(1);
    let some = ring
        .write_async(&demand)
        .may_cancel_with(CancelledToken::new())
        .await;
    assert!(
        matches!(some.pick_right(), Some(ProducerError::Cancelled)),
        "满环上的写等待应被取消令牌中止"
    );
}

dual_runtime_test_!(async_ops_honour_cancellation_);

// ---------------------------------------------------------------------------
// 分拆后的并发读写（调用者日常用法）
// ---------------------------------------------------------------------------

/// 分拆出的写端 / 读端可分别持有，同步读写往返与两端状态查询都正常。
/// - 测试目标：`Ring::split` 之后 `RingWriter::try_write` / `RingReader::try_read`
///   的日常往返，以及两端的 `producer_state` / `consumer_state` 视图。
/// - 测试手段：容量 8 上用写端写 3 个元素并查状态；再用读端读回 3 个并查状态。
/// - 判定标准：写后读端看到 `data == 3`、写端看到 `free == 5`；读回序列为
///   `[1, 2, 3]`；读空后读端看到 `data == 0`。
async fn split_tx_rx_roundtrip_() {
    let mut ring = new_ring_(8);
    let (mut tx, mut rx) = Ring::split(&mut ring);

    {
        let demand = Demand::at_least(3);
        let mut segm = tx.try_write(&demand).pick_left().expect("应能借出写段");
        assert_eq!(segm.move_items_from_as_buff(&[1u8, 2, 3]), 3);
    }
    assert_eq!(rx.consumer_state(), Some((3, false)), "读端应看到 3 个可读元素");
    assert_eq!(tx.producer_state(), Some((5, false)), "写端应看到 5 格可写空间");

    {
        let demand = Demand::at_least(3);
        let mut segm = rx.try_read(&demand).pick_left().expect("应能借出读段");
        assert_eq!(take_segm_bytes_(&mut segm, 3).await, vec![1, 2, 3]);
    }
    assert_eq!(rx.consumer_state(), Some((0, false)), "读空后读端应看到 0");
}

dual_runtime_test_!(split_tx_rx_roundtrip_);

/// 读端先 park，写端提交后在同一任务里把它唤醒。
/// - 测试目标：分拆后「一端 park、另一端提交」的读侧唤醒路径（`read_async`）。
/// - 测试手段：`join!` 并发「读端等 3 个元素」与「写端立即提交 `[7, 8, 9]`」。把 park
///   的读端放在 `join!` 第一位，保证它先挂起；写端不额外 `yield`，避免给读等待制造
///   一次「条件尚未满足」的轮询（这类提前唤醒在 park 复用时曾同步空转，见文件末尾的回归
///   用例 `reader_can_park_again_after_cancelled_wait_`）。
/// - 判定标准：读等待被唤醒并取回 `[7, 8, 9]`。
async fn read_async_wakes_on_write_() {
    let mut ring = new_ring_(8);
    let (mut tx, mut rx) = Ring::split(&mut ring);

    let read = async {
        let demand = Demand::at_least(3);
        assert_eq!(rx_read_(&mut rx, &demand, 3).await, vec![7, 8, 9]);
    };
    let write = async {
        let demand = Demand::at_least(3);
        let mut segm = tx.try_write(&demand).pick_left().expect("应能借出写段");
        assert_eq!(segm.move_items_from_as_buff(&[7u8, 8, 9]), 3);
    };
    futures_util::join!(read, write);
}

dual_runtime_test_!(read_async_wakes_on_write_);

/// 写端先 park（满环），读端取走后把它唤醒。
/// - 测试目标：分拆后「一端 park、另一端提交」的写侧唤醒路径（`write_async`）。
/// - 测试手段：容量 4 写满；`join!` 并发「写端等 1 格空间」与「读端取走 1 个元素」，
///   把 park 的写端放在第一位、读端不 `yield`。
/// - 判定标准：写等待被唤醒、拿到恰好 1 格空间并写回 1 个元素。
async fn write_async_wakes_on_read_() {
    let mut ring = new_ring_(4);
    let (mut tx, mut rx) = Ring::split(&mut ring);
    {
        let demand = Demand::at_least(4);
        let mut segm = tx.try_write(&demand).pick_left().expect("应能借出写段");
        assert_eq!(segm.move_items_from_as_buff(&[1u8, 2, 3, 4]), 4);
    }

    let write = async {
        let demand = Demand::at_least(1);
        let some = tx.write_async(&demand).await;
        let mut segm = some.pick_left().expect("读走 1 格后写等待应被唤醒");
        assert_eq!(segm.least_count(), 1, "读走 1 格后应恰好可写 1 格");
        assert_eq!(segm.move_items_from_as_buff(&[9u8]), 1);
    };
    let read = async {
        let demand = Demand::no_more_than(1);
        assert_eq!(rx_read_(&mut rx, &demand, 1).await, vec![1]);
    };
    futures_util::join!(write, read);
}

dual_runtime_test_!(write_async_wakes_on_read_);

/// 取消一个挂起的读等待后，已经就绪的数据仍可正常取走。
/// - 测试目标：读等待被取消（future drop）不会破坏环的读写状态。
/// - 测试手段：空环上发起 `read_async`，用 `select` + 让出一次证明它挂起后取消；
///   写端写入 3 个元素；读端以**同步** `try_read` 取回。
/// - 判定标准：取回的序列为 `[1, 2, 3]`；写后读端状态显示 3 个可读元素。
///
/// 说明：这里刻意**不**再次 park（第二次 park 见文件末尾的回归用例
/// `reader_can_park_again_after_cancelled_wait_`）；`demand` 仍需活过借用它的 future。
async fn cancel_pending_read_then_read_ready_() {
    let mut ring = new_ring_(8);
    let (mut tx, mut rx) = Ring::split(&mut ring);

    let demand = Demand::at_least(3);
    {
        let read = rx.read_async(&demand).into_future();
        let probe = async { futures_lite::future::yield_now().await };
        futures_util::pin_mut!(probe, read);
        assert!(
            matches!(
                futures_util::future::select(probe, read).await,
                futures_util::future::Either::Left(_)
            ),
            "空环上读等待应先 park（探测先完成）"
        );
    } // read drop = 取消

    assert_eq!(tx_write_(&mut tx, &Demand::at_least(3), &[1, 2, 3]).await, 3);
    assert_eq!(rx.consumer_state(), Some((3, false)));

    let demand = Demand::at_least(3);
    let mut segm = rx.try_read(&demand).pick_left().expect("应能借出读段");
    assert_eq!(take_segm_bytes_(&mut segm, 3).await, vec![1, 2, 3]);
}

dual_runtime_test_!(cancel_pending_read_then_read_ready_);

// ---------------------------------------------------------------------------
// park 复用与「写者只在真正腾出空间后被唤醒」
// ---------------------------------------------------------------------------
//
// 本节两个用例都来自曾经的缺陷，现在作为**回归用例**保留：
//
// 1. 同一个半部的第二次 park：park future 首次 poll 注册 waker 后，若在挂起状态下被
//    drop（取消 / `select` 落败），`RingHalf_::release_` 会在 `Drop` 里清空
//    `wake_slot_` 并释放 `opt_demand_`。否则残留槽位会让下一次 park 的首次 poll 误判
//    成「已经注册过」而立即 `Ready(None)`，`ring_*_async` 的循环在**一次 poll 内**同步
//    空转，不让出也不返回；残留的 demand 则让 `init_demand_` 静默失败，对端 `wake()`
//    按过时下限判定，甚至解引用已经释放的 `Demand`。
// 2. 等待空间的写者被提前唤醒：`Producer::wake` 必须按 `free_size()`（而非
//    `data_size()`）与等待中的 `Demand` 下限比较，只有真正腾出足够空间才唤醒。
//
// 空转一旦真的发生，任务会挂死，故用例用 [`BudgetToken`] 把空转截断成**有界失败**：
// 在有限次查询后以 `Cancelled` 暴露；修复后两个用例都应当通过。

/// 回归用例（读侧）：挂起的读等待被取消后，同一个读端可以再次挂起并被正常唤醒。
/// - 测试目标：第一次 park 在挂起状态被丢弃后，第二次 park 必须重新注册 waker 与
///   demand，而不是被残留的唤醒槽位「假唤醒」。
/// - 测试手段：空环上读等待 park 一次后取消（drop 外层 future，park future 随之 drop）；
///   随后在 `join!` 中让读端**再次**等待 1 个元素、写端提交 1 个元素；用查询预算令牌把
///   可能出现的空转截断。
/// - 判定标准：第二次读等待被写端唤醒，取回 `[1]`。
///   历史缺陷：`wake_slot_` 只设不清、`opt_demand_` 在 park 中途 drop 时不 reset，
///   第二次 park 立即 `Ready(None)`，读循环在**一次 poll 内**反复查询取消状态直到预算
///   耗尽，最终以 `Cancelled` 失败。
async fn reader_can_park_again_after_cancelled_wait_() {
    let mut ring = new_ring_(8);
    let (mut tx, mut rx) = Ring::split(&mut ring);

    // 第一次等待：park 后取消（drop）。唤醒槽位与 demand 绑定由 park future 的 `Drop`
    // 释放，这里只需保证 demand 活过借用它的外层 future。
    let first_demand = Demand::at_least(1);
    {
        let read = rx.read_async(&first_demand).into_future();
        let probe = async { futures_lite::future::yield_now().await };
        futures_util::pin_mut!(probe, read);
        assert!(
            matches!(
                futures_util::future::select(probe, read).await,
                futures_util::future::Either::Left(_)
            ),
            "空环上首次读等待应先 park"
        );
    }

    let token = BudgetToken::new(64);
    let read = async {
        let demand = Demand::at_least(1);
        let some = rx
            .read_async(&demand)
            .may_cancel_with(token.clone())
            .await;
        let mut segm = some.pick_left().unwrap_or_else(|| {
            panic!(
                "第二次读等待未挂起：在一次 poll 内查询取消状态 {} 次后中止。\
                 回归点：park future 在挂起中被 drop 时应释放唤醒槽位与 demand 绑定，\
                 否则第二次 park 会被残留槽位假唤醒并同步空转。",
                token.checks()
            )
        });
        assert_eq!(take_segm_bytes_(&mut segm, 1).await, vec![1]);
    };
    let write = async {
        assert_eq!(tx_write_(&mut tx, &Demand::at_least(1), &[1]).await, 1);
    };
    futures_util::join!(read, write);
}

dual_runtime_test_!(reader_can_park_again_after_cancelled_wait_);

/// 回归用例（写侧）：等待「更多空间」的写者只在真正腾出足够空间后被唤醒。
/// - 测试目标：写者需要 3 格空间时，只腾出 1 格不应唤醒它，腾出 3 格才唤醒。
/// - 测试手段：容量 4 写满；写端以 `at_least(3)` 等待空间；读端先取走 1 格（只腾出
///   1 格，不应唤醒写者），让出一次后再取走 2 格（此时才腾出 3 格）；用查询预算令牌把
///   可能出现的空转截断。
/// - 判定标准：写者最终拿到 3 格空间并写回 3 个元素。
///   历史缺陷：`Producer::wake` 曾按 `data_size() >= 3` 误判为「空间足够」而提前唤醒
///   写者；写者重试仍失败，park 又因 `wake_slot_` 未清立即就绪，循环同步空转。
///
/// 同一判定错误还有**反向**表现：若读端一次读走 3 格（`data_size` 降为 1），
/// `data_size() < 3` 会让对端直接返回——真正腾出的 3 格空间反而**不唤醒**写者，写者永久
/// 挂起。该表现是「漏唤醒」，park 之后不再查询取消状态，预算令牌无法截断，故本用例只
/// 钉住「空间不足时不得唤醒」这一半。
async fn writer_woken_only_when_enough_free_space_() {
    let mut ring = new_ring_(4);
    let (mut tx, mut rx) = Ring::split(&mut ring);
    {
        let demand = Demand::at_least(4);
        let mut segm = tx.try_write(&demand).pick_left().expect("应能借出写段");
        assert_eq!(segm.move_items_from_as_buff(&[1u8, 2, 3, 4]), 4);
    }

    let token = BudgetToken::new(64);
    let write = async {
        let demand = Demand::at_least(3);
        let some = tx
            .write_async(&demand)
            .may_cancel_with(token.clone())
            .await;
        let mut segm = some.pick_left().unwrap_or_else(|| {
            panic!(
                "等待 3 格空间的写者被提前唤醒：在一次 poll 内查询取消状态 {} 次后中止。\
                 回归点：Producer::wake 必须按 free_size 判定空间，而不是 data_size；\
                 提前唤醒叠加残留的唤醒槽位会同步空转。",
                token.checks()
            )
        });
        assert_eq!(segm.move_items_from_as_buff(&[9u8, 9, 9]), 3);
    };
    let read = async {
        // 只取走 1 格：不是写者需要的 3 格。
        let demand = Demand::no_more_than(1);
        assert_eq!(rx_read_(&mut rx, &demand, 1).await, vec![1]);
        // 让出一次：给写者一次重新轮询的机会（若它被错误唤醒，会在此暴露）。
        futures_lite::future::yield_now().await;
        // 再取走 2 格：此时才真正腾出 3 格。
        let demand = Demand::no_more_than(2);
        assert_eq!(rx_read_(&mut rx, &demand, 2).await, vec![2, 3]);
    };
    futures_util::join!(write, read);
}

dual_runtime_test_!(writer_woken_only_when_enough_free_space_);


// ---------------------------------------------------------------------------
// 关闭（EOF）：close_producer / close_consumer 与对端唤醒
// ---------------------------------------------------------------------------

/// 空环上 park 的消费者，在对端 `close` 之后必须立刻醒并拿到 `Closing`。
/// - 测试目标：`RingWriter::close`（置 `PRODUCER_CLOSED`）能唤醒**已登记**的消费者等待，
///   而不是让它在「不会再有任何数据」的环上永久挂起。
/// - 测试手段：容量 8 的空环拆成两半；`join!` 里先 poll「以 `at_least(1)` 读」的 future
///   （保证它先 park），再 poll 关闭写端的那一侧。
/// - 判定标准：读等待返回 `ConsumerError::Closing`（既不是 `Drained`，也不挂起）；
///   写端状态为「生产端已关闭」。
async fn close_producer_wakes_parked_consumer_() {
    let mut ring = new_ring_(8);
    let (mut tx, mut rx) = Ring::split(&mut ring);

    let read = async {
        let demand = Demand::at_least(1);
        let some = rx.read_async(&demand).await;
        assert!(
            matches!(some.pick_right(), Some(ConsumerError::Closing)),
            "生产端关闭后，空环上的读等待应报 Closing"
        );
    };
    let close = async {
        tx.close(); // 读半部 park 在同一个环上，close 必须把它唤醒
    };
    futures_util::join!(read, close);

    assert!(
        tx.ring_state().is_producer_closed(),
        "close 之后状态字应带 PRODUCER_CLOSED"
    );
}

dual_runtime_test_!(close_producer_wakes_parked_consumer_);

/// 关闭生产端不会吞掉环里已提交的数据：先交付完，取空之后才是 `Closing`。
/// - 测试目标：EOF 的交付顺序（数据优先、`Closing` 最后）。
/// - 测试手段：容量 8 写入 3 个元素 → `close` → 用读端取回 3 个 → 再读一次。
/// - 判定标准：第一次取回 `[5, 6, 7]`；第二次返回 `ConsumerError::Closing`。
async fn close_producer_delivers_remaining_then_closing_() {
    let mut ring = new_ring_(8);
    let (mut tx, mut rx) = Ring::split(&mut ring);
    {
        let demand = Demand::at_least(3);
        let mut segm = tx.try_write(&demand).pick_left().expect("应能借出写段");
        assert_eq!(segm.move_items_from_as_buff(&[5u8, 6, 7]), 3);
    }
    tx.close();

    let demand = Demand::at_least(1);
    assert_eq!(
        rx_read_(&mut rx, &demand, 3).await,
        vec![5u8, 6, 7],
        "关闭前已提交的数据必须照常交付"
    );

    let demand = Demand::at_least(1);
    let some = rx.read_async(&demand).await;
    assert!(
        matches!(some.pick_right(), Some(ConsumerError::Closing)),
        "取空之后才应报 Closing"
    );
}

dual_runtime_test_!(close_producer_delivers_remaining_then_closing_);

/// 满环上 park 的生产者，在消费端 `close` 之后必须立刻醒并拿到 `Closing`。
/// - 测试目标：`RingReader::close`（置 `CONSUMER_CLOSED`）能唤醒**已登记**的生产者等待。
/// - 测试手段：容量 4 写满 → `join!` 里先 poll「以 `at_least(1)` 写」的 future（保证它
///   先 park）→ 再 poll 关闭读端的那一侧。
/// - 判定标准：写等待返回 `ProducerError::Closing`（不是 `Stuffed`、也不挂起）。
async fn close_consumer_wakes_parked_producer_() {
    let mut ring = new_ring_(4);
    let (mut tx, mut rx) = Ring::split(&mut ring);
    {
        let demand = Demand::at_least(4);
        let mut segm = tx.try_write(&demand).pick_left().expect("应能借出写段");
        assert_eq!(segm.move_items_from_as_buff(&[1u8, 2, 3, 4]), 4);
    }

    let write = async {
        let demand = Demand::at_least(1);
        let some = tx.write_async(&demand).await;
        assert!(
            matches!(some.pick_right(), Some(ProducerError::Closing)),
            "消费端关闭后，满环上的写等待应报 Closing"
        );
    };
    let close = async {
        rx.close();
    };
    futures_util::join!(write, close);

    assert!(
        rx.ring_state().is_consumer_closed(),
        "close 之后状态字应带 CONSUMER_CLOSED"
    );
}

dual_runtime_test_!(close_consumer_wakes_parked_producer_);

/// `close` 幂等，且关闭之后的读写入口一致地报 `Closing`。
/// - 测试目标：重复关闭不 panic、不重复唤醒；关闭之后的语义与 `try_*_internal_` 的分支一致。
/// - 测试手段：空环上连续 `close` 写端两次，再连续 `close` 读端两次；期间用 `try_write`
///   与 `read_async` 各探一次。
/// - 判定标准：`is_producer_closed` / `is_consumer_closed` 均为真；`try_write` 返回
///   `ProducerError::Closing`（不是 `Stuffed`）；读端在空环上返回 `ConsumerError::Closing`。
async fn close_is_idempotent_() {
    let mut ring = new_ring_(8);
    let (mut tx, mut rx) = Ring::split(&mut ring);

    tx.close();
    tx.close();
    assert!(tx.ring_state().is_producer_closed(), "重复 close 不应改变状态");

    let demand = Demand::at_least(1);
    assert!(
        matches!(
            tx.try_write(&demand).pick_right(),
            Some(ProducerError::Closing)
        ),
        "生产端关闭后写入口应报 Closing，而不是 Stuffed"
    );

    let demand = Demand::at_least(1);
    assert!(
        matches!(
            rx.read_async(&demand).await.pick_right(),
            Some(ConsumerError::Closing)
        ),
        "生产端关闭且环为空时读入口应报 Closing"
    );

    rx.close();
    rx.close();
    assert!(rx.ring_state().is_consumer_closed(), "重复 close 不应改变状态");
}

dual_runtime_test_!(close_is_idempotent_);

/// 生产端关闭后，`Demand` 下限不足时也必须交付现有部分（EOF 例外）。
/// - 测试目标：钉住 `try_read_internal_` 的「EOF 例外」——写端已关闭时不再按 `Demand`
///   下限卡住，而是返回现有数据；并保证异步入口在「已关闭 + 环空」时立刻给 `Closing`，
///   而不是在「`can_consume_` 为真」的唤醒上同步空转。
/// - 测试手段：容量 8 写入 2 个元素；未关闭时以 `at_least(5)` 调 `try_read`；关闭生产端
///   后再以 `at_least(5)` 调 `try_read` 并取走这 2 个；最后以 `at_least(5)` 调
///   `read_async`，用预算 64 次的查询令牌把潜在空转截断成有界失败。
/// - 判定标准：未关闭时 `Drained`；关闭后 `try_read` 交出恰好 2 个元素、内容为 `[1, 2]`；
///   取空后的 `read_async` 返回 `Closing`（不是 `Cancelled`——那说明它在空转）。
async fn eof_exception_returns_partial_under_min_demand_() {
    let mut ring = new_ring_(8);
    let (mut tx, mut rx) = Ring::split(&mut ring);
    {
        let demand = Demand::at_least(2);
        let mut segm = tx.try_write(&demand).pick_left().expect("应能借出写段");
        assert_eq!(segm.move_items_from_as_buff(&[1u8, 2]), 2);
    }

    // 未关闭：不足下限 ⇒ Drained，不交出这 2 个元素。
    let demand = Demand::at_least(5);
    assert!(
        matches!(
            rx.try_read(&demand).pick_right(),
            Some(ConsumerError::Drained(_))
        ),
        "未关闭时不足下限应返回 Drained"
    );

    tx.close();

    // 已关闭：交付现有部分（不足下限）。
    let demand = Demand::at_least(5);
    let mut segm = rx
        .try_read(&demand)
        .pick_left()
        .expect("EOF 例外应交付现有部分");
    assert_eq!(segm.least_count(), 2, "应交付现有的 2 个元素");
    assert_eq!(take_segm_bytes_(&mut segm, 2).await, vec![1u8, 2]);
    drop(segm); // 提交：环被取空

    // 已关闭 + 环空 ⇒ 立刻 Closing；预算令牌保证「万一空转」表现为有界失败。
    let demand = Demand::at_least(5);
    let some = rx
        .read_async(&demand)
        .may_cancel_with(BudgetToken::new(64))
        .await;
    assert!(
        matches!(some.pick_right(), Some(ConsumerError::Closing)),
        "取空 + 已关闭应报 Closing（返回 Cancelled 说明在同步空转）"
    );
}

dual_runtime_test_!(eof_exception_returns_partial_under_min_demand_);

// ---------------------------------------------------------------------------
// 多线程压力用例：SPSC 唤醒协议在**真并行**下的回归验收
// ---------------------------------------------------------------------------
//
// 历史缺陷（详见 dev-notes/ring-20261001-0255.md）：`wake_slot_` 是无保护的
// `Option<Waker>`，兴趣位（STNDBY）的发布发生在等待者注册 waker **之前**，并且在整个
// `ring_*_async` 期间不会清除。于是存在两个窗口：
//
// * 条件检查之后、兴趣位发布之前，对端提交 → 对端看不到兴趣位 → 不唤醒；
// * 兴趣位发布之后、waker 注册之前，对端提交 → 对端看到兴趣位、但槽位还是空的 → 不唤醒。
//
// 两者都会让等待者睡死。现在协议已改成「登记 → 发布 → 复检」+ 由自旋锁保护的等待槽
// （见 dev-notes/ring-20261001-0342.md），本节的用例就是这套协议的回归验收：单线程
// 运行时（`join!` 逐个 poll 两个 future）在窗口内不会切换任务、抓不到问题，因此把两端
// 放到**两个线程**上真并行跑，并用容量 2 的环让两端严格轮流「等满一轮 → 搬一轮」，把
// 每一次唤醒都变成一次性的：丢掉任何一次，两端会同时停在 park 上（死锁），由兜底超时
// 暴露。

/// 压测环的容量：取 `Ring` 允许的最小值（2），让两端每一轮都必然走到等待 / 唤醒。
const STRESS_CAPACITY: usize = 2;

/// 每轮搬移的元素数：等于容量，于是写端每轮写满、读端每轮取空，严格交替。
const STRESS_CHUNK: usize = STRESS_CAPACITY;

/// 每次尝试里每个方向的轮数。
const STRESS_ROUNDS: usize = 2_000;

/// 独立尝试的次数：每次都换一个新环、重新起两端。
///
/// 实测缺陷的命中点既有「第一次 park 与对端首次提交相撞」的启动窗口，也有稳定态里
/// 千分之一量级的偶发相撞；多次尝试同时覆盖两者。
const STRESS_ATTEMPTS: usize = 8;

/// 单次尝试的兜底超时：正确实现下 `STRESS_ROUNDS` 轮远快于此；触发即「丢了唤醒」。
const STRESS_DEADLINE: Duration = Duration::from_secs(10);

/// 压测用的两半：`Ring` 由 `Arc` 共享，`split_unchecked` 之后只有两半各持一个。
type StressWriter = RingWriter<Arc<TestRing>, TestBuff, u8>;
type StressReader = RingReader<Arc<TestRing>, TestBuff, u8>;

/// 用 `Arc<Ring>` 拆出一对可跨线程 / 跨任务移动的读写半部。
fn split_shared_ring_(capacity: usize) -> (StressWriter, StressReader) {
    let ring = Arc::new(new_ring_(capacity));
    // SAFETY: `split_unchecked` 要求环被智能指针独占持有、且不存在 weak 升级。
    // `ring` 被 move 进来后由内部 clone 一份给写端；返回之后测试内不再保留其它
    // `Arc`，也没有 `Weak`，两个半部各持唯一的一份。
    unsafe { Ring::split_unchecked(ring) }
}

/// 压测写端：`rounds` 轮「借满 `STRESS_CHUNK` 个写段 → 填充 → 提交（drop）」。
async fn stress_write_loop_(mut tx: StressWriter, rounds: usize, progress: Arc<AtomicUsize>) {
    let payload = [0xA5u8; STRESS_CHUNK];
    for _ in 0..rounds {
        let demand = Demand::at_least(STRESS_CHUNK);
        let some = tx.write_async(&demand).await;
        let mut segm = some.pick_left().expect("写端应能借出写段");
        assert_eq!(
            segm.move_items_from_as_buff(&payload),
            STRESS_CHUNK,
            "写端应恰好搬入一轮的量"
        );
        drop(segm); // 提交：推进 wp，并在读端待机时唤醒它
        progress.fetch_add(1, Ordering::Relaxed);
    }
}

/// 压测读端：`rounds` 轮「借满 `STRESS_CHUNK` 个读段 → 取走 → 提交（drop）」。
async fn stress_read_loop_(mut rx: StressReader, rounds: usize, progress: Arc<AtomicUsize>) {
    for _ in 0..rounds {
        let demand = Demand::at_least(STRESS_CHUNK);
        let some = rx.read_async(&demand).await;
        let mut segm = some.pick_left().expect("读端应能借出读段");
        let got = take_segm_bytes_(&mut segm, STRESS_CHUNK).await;
        assert_eq!(got.len(), STRESS_CHUNK, "读端应恰好取走一轮的量");
        drop(segm); // 提交：推进 rp，并在写端待机时唤醒它
        progress.fetch_add(1, Ordering::Relaxed);
    }
}

/// 把两个方向分别放到两个 OS 线程上跑，并用 `recv_timeout` 兜底等待。
///
/// 两个闭包各自负责「建运行时 + `block_on` 本方向的循环」；结束信号由 `Drop` 守卫发出，
/// 因此某一端 panic（断言失败）时主线程也能立刻失败，而不必等满超时。
fn run_stress_on_two_threads_(
    runtime: &'static str,
    attempt: usize,
    write: impl FnOnce() + Send + 'static,
    read: impl FnOnce() + Send + 'static,
    done_w: Arc<AtomicUsize>,
    done_r: Arc<AtomicUsize>,
) {
    /// 离开作用域（含 unwind）时向主线程报告「本端已结束」。
    struct DoneGuard_(mpsc::Sender<()>);

    impl Drop for DoneGuard_ {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    let (sig_tx, sig_rx) = mpsc::channel::<()>();

    let w_sig = DoneGuard_(sig_tx.clone());
    let w = thread::spawn(move || {
        let _guard = w_sig;
        write();
    });

    let r_sig = DoneGuard_(sig_tx);
    let r = thread::spawn(move || {
        let _guard = r_sig;
        read();
    });

    let mut finished = 0usize;
    while finished < 2 {
        if sig_rx.recv_timeout(STRESS_DEADLINE).is_err() {
            panic!(
                "{runtime} 多线程 ping-pong 第 {attempt}/{STRESS_ATTEMPTS} 次尝试在 \
                 {STRESS_DEADLINE:?} 内未完成：写端 {}/{} 轮、读端 {}/{} 轮。\
                 两端同时停在 park 上说明丢了唤醒\
                 （见 dev-notes/ring-20261001-0225.md）。",
                done_w.load(Ordering::Relaxed),
                STRESS_ROUNDS,
                done_r.load(Ordering::Relaxed),
                STRESS_ROUNDS,
            );
        }
        finished += 1;
    }

    w.join().expect("写端线程不应 panic");
    r.join().expect("读端线程不应 panic");
}

/// 多线程压力用例（tokio）：SPSC ping-pong 在真并行下不得丢唤醒。
/// - 测试目标：读 / 写两端在**两个线程**上并行推进 `STRESS_ROUNDS` 轮「等满一轮再搬
///   一轮」。任何一次「本应发生的唤醒」被丢掉，两端都会同时停在 park 上，且没有后续事件
///   能再唤醒它们（死锁）。
/// - 测试手段：容量 2 的环经 `Arc` + `split_unchecked` 拆成两半，各交给一个 OS 线程，
///   线程内用 tokio 的 `current_thread` 运行时 `block_on` 驱动；主线程用
///   `mpsc::recv_timeout` 兜底。
///   这里刻意**不用** `#[tokio::test(flavor = "multi_thread")]` + `tokio::spawn`：spawn
///   要求 future 满足 `'static`，而 `RingReader::read_async` 的返回类型是 GAT 投影，
///   rustc 目前证不出这一点（issue #100013），换成 `BoxFuture<'static, _>` 同样如此；
///   单任务里的 `join!` 更不行——它逐个 poll 两个 future，制造不出真并行。
/// - 判定标准：每次尝试的 `STRESS_ROUNDS` 轮全部完成，`STRESS_ATTEMPTS` 次都通过；超时
///   失败时打印两端各自完成的轮数，用来区分是「写端等空间」还是「读端等数据」被卡住。
#[test]
fn wakeup_stress_tokio_() {
    for attempt in 1..=STRESS_ATTEMPTS {
        let (tx, rx) = split_shared_ring_(STRESS_CAPACITY);
        let done_w = Arc::new(AtomicUsize::new(0));
        let done_r = Arc::new(AtomicUsize::new(0));

        let dw = Arc::clone(&done_w);
        let write = move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("创建 tokio current_thread 运行时");
            rt.block_on(stress_write_loop_(tx, STRESS_ROUNDS, dw));
        };

        let dr = Arc::clone(&done_r);
        let read = move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("创建 tokio current_thread 运行时");
            rt.block_on(stress_read_loop_(rx, STRESS_ROUNDS, dr));
        };

        run_stress_on_two_threads_("tokio", attempt, write, read, done_w, done_r);
    }
}

/// 多线程压力用例（compio）：同 [`wakeup_stress_tokio_`]，只是换成 compio 运行时。
/// - 测试手段：compio 是 thread-per-core、没有多线程 worker 池，所以「多线程」就是一个
///   线程一个 `compio::runtime::Runtime`，各自 `block_on` 一个方向（compio 文档里的用法）；
///   兜底方式与判定标准同 tokio 版本。
#[test]
fn wakeup_stress_compio_() {
    for attempt in 1..=STRESS_ATTEMPTS {
        let (tx, rx) = split_shared_ring_(STRESS_CAPACITY);
        let done_w = Arc::new(AtomicUsize::new(0));
        let done_r = Arc::new(AtomicUsize::new(0));

        let dw = Arc::clone(&done_w);
        let write = move || {
            let rt = compio::runtime::Runtime::new().expect("创建 compio 运行时");
            rt.block_on(stress_write_loop_(tx, STRESS_ROUNDS, dw));
        };

        let dr = Arc::clone(&done_r);
        let read = move || {
            let rt = compio::runtime::Runtime::new().expect("创建 compio 运行时");
            rt.block_on(stress_read_loop_(rx, STRESS_ROUNDS, dr));
        };

        run_stress_on_two_threads_("compio", attempt, write, read, done_w, done_r);
    }
}
