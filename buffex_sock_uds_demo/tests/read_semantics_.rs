//! **读语义特征测试**：钉住两个适配器的 `read_async` 在「设备上只有少量数据、
//! 且未 EOF」时到底什么时候交付数据。
//!
//! # 当前语义：有数据就交付
//!
//! 两个适配器的搬运泵都以「**设备此刻能给的**」为一次搬运的终点：设备给出多少就
//! 提交多少，不等环段填满。因此 `ring_cap` 可以远大于一条消息的长度，`read_async`
//! 仍会及时把它交出来——这是 `smux_v1_sock_demo` 的握手帧（几十字节）能在
//! `ring-cap = 256 KiB` 下跑通的前提。
//!
//! # 历史：曾经是「填满环段才交付」
//!
//! 早先两侧都直接用 `move_items_from_input_async` 填段（tokio 侧按需驱动、compio 侧
//! 后台泵，行为一致），于是设备上只有 `chunk` 字节时会一直 park 到「环段填满」或
//! 「设备结束」——请求 / 应答式上层因此必然静止（对端在等应答，而本端在等段填满）。
//! 当时 RTT 用例把 `ring-cap` 钉成 `chunk` 正是为了绕开它；现在这条约束不再必要。

use std::{
    io::Write as _,
    time::{Duration, Instant},
};

use buffex_compio_adapt::{
    BuffRead as CompioBuffRead, DefaultAllocConfig as CompioAllocConfig,
};
use buffex_tokio_adapt::{
    BuffRead as TokioBuffRead, DefaultAllocConfig as TokioAllocConfig,
};
use buffex_sock_uds_demo::pump::{self, Staging};
use buffex::x_deps::abs_buff;

/// 写端延迟：留出一点时间让「读端已经 park 在搬运泵里」这件事确定发生。
const WRITE_DELAY: Duration = Duration::from_millis(50);
/// 观察窗口：超过它还没交付，就判定为「不会交付」。
const WINDOW: Duration = Duration::from_millis(600);

/// 起一个 std 线程，延迟 `delay` 后往 std 侧 socket 写 `n` 字节，且**不关闭**
/// （避免 EOF 触发「设备结束」这条交付路径）。
fn spawn_std_writer_(sock: std::os::unix::net::UnixStream, n: usize, delay: Duration) {
    std::thread::spawn(move || {
        let mut s = sock;
        std::thread::sleep(delay);
        let _ = s.write_all(&vec![7u8; n]);
        let _ = s.flush();
        // 保持连接打开：睡眠期间不让 EOF 干扰观察。
        std::thread::sleep(Duration::from_secs(10));
    });
}

/// 造一对已连通的 UDS（std 侧给写线程，运行时侧交给适配器）。
fn pair_for_tokio_() -> (std::os::unix::net::UnixStream, tokio::net::UnixStream) {
    let (a, b) = std::os::unix::net::UnixStream::pair().expect("应能建立 socket 对");
    b.set_nonblocking(true).expect("tokio 需要非阻塞 fd");
    let b = tokio::net::UnixStream::from_std(b).expect("应能转为 tokio UnixStream");
    (a, b)
}

/// 造一对已连通的 UDS（std 侧给写线程，运行时侧交给适配器）。
fn pair_for_compio_() -> (std::os::unix::net::UnixStream, compio::net::UnixStream) {
    let (a, b) = std::os::unix::net::UnixStream::pair().expect("应能建立 socket 对");
    let b = compio::net::UnixStream::from_std(b).expect("应能转为 compio UnixStream");
    (a, b)
}

/// 环容量等于消息大小时，tokio 适配器的 `read_async` 能及时交付。
/// - 测试目标：确认「一条消息恰好填满一个环段」是 tokio 侧可靠交付的条件。
/// - 测试手段：环容量与写入量都取 4 KiB，对端延迟 50 ms 后写入且保持连接，
///   在 600 ms 窗口内 await `read_async` 直到凑满 4 KiB。
/// - 判断：在规定窗口内读满 4 KiB。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_read_delivers_when_ring_equals_message_() {
    let (a, b) = pair_for_tokio_();
    spawn_std_writer_(a, 4096, WRITE_DELAY);
    let mut rx = TokioBuffRead::try_new(b, 4096, TokioAllocConfig).expect("容量合法");
    let mut staging = Staging::new();
    let mut dst = Vec::new();
    let r = tokio::time::timeout(
        WINDOW,
        pump::read_exact_into(&mut rx, &mut staging, 4096, &mut dst),
    )
    .await;
    assert!(
        matches!(r, Ok(Ok(4096))),
        "环容量等于消息大小时应交付 4 KiB，实际 {r:?}"
    );
    assert_eq!(dst.len(), 4096, "交付的字节数应与写入量一致");
}

/// 环容量大于消息大小时，tokio 适配器的 `read_async` 也**必须及时交付**。
/// - 测试目标：钉住「有数据就交付」语义——设备给出不足一段的数据时不得等到段填满。
/// - 测试手段：环容量 64 KiB、只写入 4 KiB 且保持连接（避免 EOF 这条交付路径），
///   在 600 ms 窗口内 await `read_async` 直到凑满 4 KiB。
/// - 判断：窗口内读满 4 KiB，且耗时明显短于窗口（`WRITE_DELAY` 之后即交付）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_read_delivers_partial_segment_() {
    let (a, b) = pair_for_tokio_();
    spawn_std_writer_(a, 4096, WRITE_DELAY);
    let mut rx = TokioBuffRead::try_new(b, 64 * 1024, TokioAllocConfig).expect("容量合法");
    let mut staging = Staging::new();
    let mut dst = Vec::new();
    let t0 = Instant::now();
    let r = tokio::time::timeout(
        WINDOW,
        pump::read_exact_into(&mut rx, &mut staging, 4096, &mut dst),
    )
    .await;
    assert!(
        matches!(r, Ok(Ok(4096))),
        "环容量大于消息大小时也应及时交付 4 KiB，实际 {r:?}（耗时 {:?}）",
        t0.elapsed()
    );
    assert_eq!(dst.len(), 4096, "交付的字节数应与写入量一致");
}

/// `try_read`（poll-once）能把部分数据交出来。
/// - 测试目标：确认「同步推一轮」与「异步等环段填满」的差别——这是调用方在
///   需要低延迟小消息时的可用出路。
/// - 测试手段：环容量 64 KiB，对端写入 4 KiB 且保持连接；等 200 ms 后调用
///   `try_read` 并用 `move_items_to_buff` 搬出。
/// - 判断：搬出的字节数恰为 4096。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_try_read_delivers_partial_segment_() {
    let (a, b) = pair_for_tokio_();
    spawn_std_writer_(a, 4096, WRITE_DELAY);
    let mut rx = TokioBuffRead::try_new(b, 64 * 1024, TokioAllocConfig).expect("容量合法");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let demand = abs_buff::Demand::at_least(1);
    let mut got = 0usize;
    for _ in 0..4 {
        let some = rx.try_read(&demand);
        let Some(mut segm) = some.pick_left() else {
            break;
        };
        let now = segm.least_count();
        let mut buf = vec![core::mem::MaybeUninit::uninit(); now];
        // SAFETY: `T = u8` 无析构资源，`buf` 与环段无别名。
        let moved = unsafe { segm.move_items_to_buff(&mut buf) };
        got += moved;
    }
    assert_eq!(got, 4096, "try_read 应当能把已搬入环的部分数据交出来");
}

/// 环容量等于消息大小时，compio 适配器的 `read_async` 能及时交付。
/// - 测试目标：与 tokio 侧同一条结论在 compio 侧也成立（行为一致）。
/// - 测试手段：环容量与写入量都取 4 KiB，对端延迟 50 ms 后写入且保持连接。
/// - 判断：600 ms 窗口内读满 4 KiB。
#[compio::test]
async fn compio_read_delivers_when_ring_equals_message_() {
    let (a, b) = pair_for_compio_();
    spawn_std_writer_(a, 4096, WRITE_DELAY);
    let mut rx = CompioBuffRead::try_new(b, 4096, CompioAllocConfig).expect("容量合法");
    let mut staging = Staging::new();
    let mut dst = Vec::new();
    let r = compio::time::timeout(
        WINDOW,
        pump::read_exact_into(&mut rx, &mut staging, 4096, &mut dst),
    )
    .await;
    assert!(
        matches!(r, Ok(Ok(4096))),
        "环容量等于消息大小时应交付 4 KiB，实际 {r:?}"
    );
}

/// 环容量大于消息大小时，compio 适配器的 `read_async` 也必须及时交付。
/// - 测试目标：与 tokio 侧的同类用例一起钉住「有数据就交付」在两个后端上一致。
/// - 测试手段：环容量 64 KiB、只写入 4 KiB 且保持连接，600 ms 窗口内 await。
/// - 判断：窗口内读满 4 KiB（`WRITE_DELAY` 之后即交付，而不是等到超时）。
#[compio::test]
async fn compio_read_delivers_partial_segment_() {
    let (a, b) = pair_for_compio_();
    spawn_std_writer_(a, 4096, WRITE_DELAY);
    let mut rx = CompioBuffRead::try_new(b, 64 * 1024, CompioAllocConfig).expect("容量合法");
    let mut staging = Staging::new();
    let mut dst = Vec::new();
    let t0 = Instant::now();
    let r = compio::time::timeout(
        WINDOW,
        pump::read_exact_into(&mut rx, &mut staging, 4096, &mut dst),
    )
    .await;
    assert!(
        matches!(r, Ok(Ok(4096))),
        "环容量大于消息大小时也应及时交付 4 KiB，实际 {r:?}（耗时 {:?}）",
        t0.elapsed()
    );
    assert_eq!(dst.len(), 4096, "交付的字节数应与写入量一致");
}
