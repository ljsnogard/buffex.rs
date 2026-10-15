//! 集成测试：写侧**收尾契约**（[`TrBuffTryFlush`] / [`TrBuffFlush`]）的验收。
//!
//! 背景：`smux_v1` 的复用循环只把传输当 [`TrBuffWrite`] 用、从不替适配器调收尾入口，
//! 于是按需驱动形态（`try_new`）的「环 → 设备」永远不被推进——握手帧留在环里、对端
//! 永久等待。上游因此补了这两条 flush trait：协议栈只要在收尾时用它们，就不必再要求
//! 调用方「必须选自驱动形态」。
//!
//! 本文件把契约钉在**泛型边界**上（与 `smux_v1` 将来使用的方式一致），逐个形态验收。

use std::time::Duration;

use buffex_tokio_adapt::{
    BuffWrite, DefaultAllocConfig,
    x_deps::abs_buff::{
        self, Demand, TrBuffFlush, TrBuffTryFlush, TrBuffWrite,
        buffer::TrBuffSegmMut,
    },
};
use tokio::io::AsyncReadExt;

/// 确定性负载：第 `i` 个字节为 `(i % 251) as u8`。
fn payload_(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// 把 `payload` 全部提交进写端（借段 → 填数据 → drop 提交），随后**不**主动收尾。
///
/// 这是 `smux_v1` 复用循环的形状：提交完最后一帧就放手。
async fn submit_all_<T>(tx: &mut T, payload: &[u8])
where
    T: TrBuffWrite<u8> + Unpin,
    for<'s> T::SegmMut<'s>: abs_buff::buffer::TrBuffSegmMut<'s, u8>,
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

/// **泛型收尾函数**：只要求 `TrBuffFlush`，用 trait 上的 `flush_async` / `close_async`。
///
/// 它与 `smux_v1` 将来在泛型层调用的形态一致——证明「协议栈可以在不知道具体传输类型、
/// 也不知道它是不是自驱动」的前提下完成收尾。
async fn flush_then_close_<T>(tx: &mut T) -> Result<(), T::Err>
where
    T: TrBuffFlush<u8> + Unpin,
    T::Err: core::fmt::Debug,
{
    tx.flush_async().await?;
    tx.close_async().await
}

/// 按需形态：提交完不调 `flush`，靠**泛型层**的 `TrBuffFlush` 收尾即可全部送达。
/// - 测试目标：`BuffWrite::try_new`（调用方驱动）实现 `TrBuffFlush` 后，
///   「提交进环」+「泛型 `flush_async`」= 数据送达设备，协议栈不需要知道形态。
/// - 测试手段：容量 64 的环上提交 200 字节（必然多次走「环满 → 边冲边写」），
///   然后用 `flush_then_close_` 收尾；设备侧 `read_exact` 收满 200 字节。
/// - 判断：设备侧收到的字节与负载逐字节一致，收尾返回 `Ok`。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_form_flushes_through_the_trait_() {
    let payload = payload_(200usize);
    let (dev, mut peer) = tokio::io::duplex(1024);
    let mut tx = BuffWrite::<_, DefaultAllocConfig>::try_new(dev, 64, DefaultAllocConfig)
        .expect("容量合法");

    submit_all_(&mut tx, &payload).await;
    flush_then_close_(&mut tx)
        .await
        .expect("泛型收尾应当成功");

    let mut got = vec![0u8; payload.len()];
    tokio::time::timeout(Duration::from_millis(300), peer.read_exact(&mut got))
        .await
        .expect("收尾之后设备应当已经收到全部字节")
        .expect("对端读不应出错");
    assert_eq!(got, payload, "送达的字节应与提交的负载一致");
}

/// 自驱动形态：同一段泛型收尾代码在 `try_new_local` 上也成立。
/// - 测试目标：`TrBuffFlush` 的实现对两种驱动形态**语义一致**——调用方无需换代码。
/// - 测试手段：与上一条逐字相同，只把构造入口换成 `try_new_local`，并把
///   「构造 + 提交 + 收尾 + 对端读」放进本线程那条本地队列的驱动栈。
/// - 判断：设备侧字节与负载一致，收尾返回 `Ok`。
#[test]
fn local_form_flushes_through_the_trait_() {
    use abs_art::{FULL, TrLocalScope};
    use abs_art_tokio::Runtime as TokioRuntime;

    let payload = payload_(200usize);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("建运行时");
    rt.block_on(async {
        let scope = TokioRuntime::<FULL>::current().local_scope();
        scope
            .run_until(async {
                let (dev, mut peer) = tokio::io::duplex(1024);
                let mut tx =
                    BuffWrite::<_, DefaultAllocConfig>::try_new_local(dev, 64, DefaultAllocConfig)
                        .expect("构造自驱动写端");

                submit_all_(&mut tx, &payload).await;
                flush_then_close_(&mut tx)
                    .await
                    .expect("泛型收尾应当成功");

                let mut got = vec![0u8; payload.len()];
                tokio::time::timeout(Duration::from_millis(300), peer.read_exact(&mut got))
                    .await
                    .expect("收尾之后设备应当已经收到全部字节")
                    .expect("对端读不应出错");
                assert_eq!(got, payload, "送达的字节应与提交的负载一致");
            })
            .await;
    });
}

/// `try_flush` 的 try-once 语义：**还没被搬走就如实报 `Stuffed`，排空后才 `Ok`**。
/// - 测试目标：同步入口给协议栈一个「现在能不能收尾」的非阻塞判据，而不是假装成功。
/// - 测试手段：`try_new_local` 形态下，后台泵的第一次 poll 需要一个 await 点才会发生；
///   而 `try_flush` 是**同步**的、不经过任何 await，因此在「提交完那一帧、还没 await」
///   的时刻环里必然仍有数据——此时它必须报 `Err`。随后用 `flush_async` 真正排空，再断言
///   变成 `Ok`。
/// - 判断：排空前 `Err`、排空后 `Ok`，且 `close` / `close_async` 可重复调用（幂等）。
#[test]
fn try_flush_reports_pending_until_drained_() {
    use abs_art::{FULL, TrLocalScope};
    use abs_art_tokio::Runtime as TokioRuntime;

    let payload = payload_(64usize);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("建运行时");
    rt.block_on(async {
        let scope = TokioRuntime::<FULL>::current().local_scope();
        scope
            .run_until(async {
                let (dev, mut peer) = tokio::io::duplex(4096);
                let mut tx =
                    BuffWrite::<_, DefaultAllocConfig>::try_new_local(dev, 64, DefaultAllocConfig)
                        .expect("构造自驱动写端");

                submit_all_(&mut tx, &payload).await;
                assert!(
                    tx.try_flush().is_err(),
                    "同步入口在泵还没跑之前就必须如实报未排空"
                );

                tx.flush_async().await.expect("异步排空应当成功");
                tx.try_flush().expect("排空之后 try_flush 应当成功");

                // 幂等：`close` 与 `close_async` 都可以重复调用。
                tx.close().expect("首次 close 应当成功");
                tx.close().expect("close 必须幂等");
                tx.close_async().await.expect("close_async 应当幂等成功");

                let mut got = vec![0u8; payload.len()];
                tokio::time::timeout(Duration::from_millis(300), peer.read_exact(&mut got))
                    .await
                    .expect("收尾之后设备应当已经收到全部字节")
                    .expect("对端读不应出错");
                assert_eq!(got, payload);
            })
            .await;
    });
}

/// `close`（同步封口）必须**唤醒 park 在环读端上的搬运者**。
/// - 测试目标：钉住「全被动环那一半」的缺口修复——`TrBuffTryFlush::close` 让协议栈
///   退出时能主动置 `PRODUCER_CLOSED`（并唤醒等待方），而不是只靠 drop 隐式表达。
/// - 测试手段：`try_new_local` 形态下后台泵正 park 在「环空」上；主任务只提交一帧，
///   然后调**同步** `close()`（不 await 任何 future）。泵被唤醒后应把这一帧交给设备、
///   `shutdown` 设备并退出——**空设备的对端因此读到 EOF**（在此之前它一直 pending）。
/// - 判断：对端先收到那一帧、再读到 EOF（`read_exact` 收满 16 字节后下一次读返回 0），
///   且全程不需要调用方替适配器推任何一次搬运。
#[test]
fn close_wakes_the_pump_parked_on_the_ring_() {
    use abs_art::{FULL, TrLocalScope};
    use abs_art_tokio::Runtime as TokioRuntime;

    const K_FRAME: [u8; 16] = *b"INVITE-handshake";
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("建运行时");

    rt.block_on(async {
        let scope = TokioRuntime::<FULL>::current().local_scope();
        scope
            .run_until(async {
                let (dev, mut peer) = tokio::io::duplex(1024);
                let mut tx =
                    BuffWrite::<_, DefaultAllocConfig>::try_new_local(dev, 64, DefaultAllocConfig)
                        .expect("构造自驱动写端");

                submit_all_(&mut tx, &K_FRAME).await;
                // 同步封口：唤醒 park 中的泵；泵负责把这一帧交给设备再 shutdown。
                tx.close().expect("close 应当成功");

                let mut got = [0u8; 16];
                tokio::time::timeout(Duration::from_millis(300), peer.read_exact(&mut got))
                    .await
                    .expect("close 之后泵应当被唤醒并及时把这一帧交给设备")
                    .expect("对端读不应出错");
                assert_eq!(got, K_FRAME);

                // 泵收尾时会 `shutdown` 设备，因此对端随后读到 EOF。
                let mut tail = [0u8; 1];
                let eof = tokio::time::timeout(Duration::from_millis(300), peer.read(&mut tail))
                    .await
                    .expect("泵应当已经 shutdown 设备");
                assert_eq!(eof.expect("对端读不应出错"), 0usize, "对端应当读到 EOF");
            })
            .await;
    });
}
