//! 集成测试：**compio** 侧写端收尾契约（[`TrBuffTryFlush`] / [`TrBuffFlush`]）的验收。
//!
//! 与 tokio / smol 侧的两点结构性差异，本文件正是要钉住它们：
//!
//! 1. compio 的适配器**只有一种驱动形态**（`try_new` 构造时就 `spawn` 后台泵）；
//! 2. 因此 `try_flush` / `close` **不推进也不等待**——环已空即 `Ok`、否则 `Stuffed`，
//!    封口只置位并唤醒泵；真正的排空与设备 `shutdown` 由 `flush_async` / `close_async`
//!    负责。
//!
//! 设备一律用本文件内的**确定性内存设备**（借用形态），不依赖真实 socket 时序。

use core::cell::RefCell;

use std::{
    io,
    rc::Rc,
};

use abs_buff::{
    Demand, TrBuffFlush, TrBuffTryFlush, TrBuffWrite,
    buffer::TrBuffSegmMut,
};
use buffex::x_deps::abs_buff;
use buffex_compio_adapt::{BuffWrite, DefaultAllocConfig};
use compio::{
    buf::{BufResult, IoBuf},
    io::AsyncWrite,
};

/// 共享的已收字节（测试侧检查用）。
type SharedBytes_ = Rc<RefCell<Vec<u8>>>;

/// 共享的 `shutdown` 记账位（测试侧检查用）。
type SharedFlag_ = Rc<RefCell<bool>>;

/// 借用形态的收集设备：把每次写入追加进共享缓冲，记账 `shutdown`。
struct CollectDevice {
    /// 已收下的字节（与测试共享，便于收尾后检查）。
    got_: Rc<RefCell<Vec<u8>>>,
    /// 是否收到过 `shutdown`。
    closed_: Rc<RefCell<bool>>,
}

impl CollectDevice {
    fn new_() -> (Self, SharedBytes_, SharedFlag_) {
        let got = Rc::new(RefCell::new(Vec::new()));
        let closed = Rc::new(RefCell::new(false));
        (
            CollectDevice {
                got_: got.clone(),
                closed_: closed.clone(),
            },
            got,
            closed,
        )
    }
}

impl AsyncWrite for CollectDevice {
    async fn write<T: IoBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        let n = buf.buf_len();
        self.got_.borrow_mut().extend_from_slice(&buf.as_init()[..n]);
        BufResult(Result::Ok(n), buf)
    }

    async fn flush(&mut self) -> io::Result<()> {
        Result::Ok(())
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        *self.closed_.borrow_mut() = true;
        Result::Ok(())
    }
}

/// 确定性负载：第 `i` 个字节为 `(i % 251) as u8`。
fn payload_(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// 把 `payload` 全部提交进写端（借段 → 填数据 → drop 提交），随后不主动收尾。
async fn submit_all_<T>(tx: &mut T, payload: &[u8])
where
    T: TrBuffWrite<u8> + Unpin,
    for<'s> T::SegmMut<'s>: TrBuffSegmMut<'s, u8>,
    T::Err: core::fmt::Debug,
{
    let mut off = 0usize;
    while off < payload.len() {
        let demand = Demand::at_least(1);
        let some = tx.write_async(&demand).await;
        let mut segm = some.pick_left().expect("应当能借出写段");
        let moved = segm.move_items_from_as_buff(&payload[off..]);
        assert!(moved > 0usize, "借出的写段不应为空");
        off += moved;
        drop(segm); // 提交进环
    }
}

/// **泛型收尾**：只要求 `TrBuffFlush`，用 trait 上的 `flush_async` / `close_async`。
///
/// 与 `smux_v1` 将来的用法一致：协议栈不知道具体传输类型、也不需要知道它是不是自驱动。
async fn flush_then_close_<T>(tx: &mut T) -> Result<(), T::Err>
where
    T: TrBuffFlush<u8> + Unpin,
    T::Err: core::fmt::Debug,
{
    tx.flush_async().await?;
    tx.close_async().await
}

/// compio 形态：提交完不调收尾，靠**泛型层**的 `TrBuffFlush` 全部送达并 `shutdown`。
/// - 测试目标：`BuffWrite::try_new`（构造即 spawn 后台泵）实现 `TrBuffFlush`，
///   协议栈只需在收尾时调到 trait 方法，即可保证「已提交字节送达 + 设备被封口」。
/// - 测试手段：容量 64 的环上提交 200 字节（必然走「环满 → 泵边冲边写」），设备是可
///   借用、可检查的内存设备；随后用 `flush_then_close_` 收尾。
/// - 判断：设备收到的字节与负载逐字节一致；`shutdown` 已被调用；收尾返回 `Ok`。
#[compio::test]
async fn flush_trait_delivers_and_shuts_down_() {
    let payload = payload_(200usize);
    let (dev, got, closed) = CollectDevice::new_();
    let mut tx = BuffWrite::<_, DefaultAllocConfig>::try_new(dev, 64, DefaultAllocConfig)
        .expect("在 compio runtime 内且容量合法");

    submit_all_(&mut tx, &payload).await;
    flush_then_close_(&mut tx).await.expect("泛型收尾应当成功");

    assert!(
        *closed.borrow(),
        "close_async 必须把设备 shutdown（对端据此看到 EOF）"
    );
    assert_eq!(
        &got.borrow()[..],
        &payload[..],
        "送达设备的字节应与提交的负载一致"
    );
}

/// `try_flush` 的 try-once 语义：**泵还没搬走就如实报 `Stuffed`，排空后才 `Ok`**。
/// - 测试目标：compio 侧同步入口不推进、不等待，只如实回答「现在是否已全部交出」。
/// - 测试手段：提交一帧后**立刻**（第一个 await 点之前）调 `try_flush`；随后用
///   `flush_async` 真正排空，再调一次 `try_flush`。
/// - 判断：排空前 `Err(Stuffed)`、排空后 `Ok`；`close` 可重复调用（幂等）。
#[compio::test]
async fn try_flush_reports_pending_until_drained_() {
    const K_FRAME: [u8; 16] = *b"INVITE-handshake";
    let (dev, got, _closed) = CollectDevice::new_();
    let mut tx = BuffWrite::<_, DefaultAllocConfig>::try_new(dev, 64, DefaultAllocConfig)
        .expect("在 compio runtime 内且容量合法");

    submit_all_(&mut tx, &K_FRAME).await;
    assert!(
        tx.try_flush().is_err(),
        "后台泵还没来得及搬运时，try_flush 必须如实报未排空"
    );

    tx.flush_async().await.expect("异步排空应当成功");
    tx.try_flush().expect("排空之后 try_flush 应当成功");

    tx.close().expect("首次 close 应当成功");
    tx.close().expect("close 必须幂等");
    tx.close_async().await.expect("close_async 应当幂等成功");

    assert_eq!(&got.borrow()[..], &K_FRAME[..], "送出的应是那一帧");
}

/// `close`（同步封口）唤醒 park 在环空上的后台泵，泵随后排空并 `shutdown`。
/// - 测试目标：钉住同步封口在**自驱动**形态下的实际效果——不必 await 任何 future，
///   只调一次 `close()`，泵就会把已提交数据送完并收尾设备。
/// - 测试手段：提交一帧后只调 `close()`（不 await），然后 `close_async` 等泵退出；
///   设备侧检查字节与 `shutdown`。
/// - 判断：设备收到那一帧、且已被 `shutdown`。
#[compio::test]
async fn close_wakes_the_pump_parked_on_the_ring_() {
    const K_FRAME: [u8; 16] = *b"INVITE-handshake";
    let (dev, got, closed) = CollectDevice::new_();
    let mut tx = BuffWrite::<_, DefaultAllocConfig>::try_new(dev, 64, DefaultAllocConfig)
        .expect("在 compio runtime 内且容量合法");

    submit_all_(&mut tx, &K_FRAME).await;
    tx.close().expect("close 应当成功");
    tx.close_async().await.expect("等泵收尾应当成功");

    assert_eq!(&got.borrow()[..], &K_FRAME[..], "泵应当把那一帧交给设备");
    assert!(*closed.borrow(), "泵正常收尾时必须 shutdown 设备");
}
