
//! 集成测试：写侧两种驱动形态在「**提交完最后一段后就不再调用写入口**」的协议栈下的差别。
//!
//! 本文件是 [`buffex_tokio_adapt::BuffWrite`] 的「按需 vs 自驱动」对照实验，动机见
//! `smux_v1_uds_bench` 的踩坑记录：`smux_v1` 的复用循环把帧提交进环之后就走，
//! 从不替适配器调 `flush_async`。于是按需形态的「环 → 设备」永远不会被推进。

use std::time::Duration;

use buffex_tokio_adapt::{
    BuffWrite, DefaultAllocConfig,
    x_deps::abs_buff::{self, Demand, TrBuffWrite, buffer::TrBuffSegmMut},
};
use tokio::io::AsyncReadExt;

/// 一帧「握手」字节（16 字节，内容无所谓，只要非空）。
const K_FRAME: [u8; 16] = *b"INVITE-handshake";

/// 把一帧提交进写端（借段 → 填数据 → drop 提交），之后不再调用任何写入口。
///
/// 这就是 `smux_v1` 复用循环的形状：**提交即放手**，不回头调 `flush_async`。
async fn submit_once_<T>(tx: &mut T)
where
    T: TrBuffWrite<u8> + Unpin,
    for<'s> T::SegmMut<'s>: abs_buff::buffer::TrBuffSegmMut<'s, u8>,
    T::Err: core::fmt::Debug,
{
    let demand = Demand::at_least(1);
    let mut segm = tx
        .write_async(&demand)
        .await
        .pick_left()
        .expect("应当能借出写段");
    let moved = segm.move_items_from_as_buff(&K_FRAME);
    assert_eq!(moved, K_FRAME.len(), "一帧应当一次填完");
    drop(segm); // 提交：进入环，等「环 → 设备」的搬运者
}

/// 按需形态（`try_new`）：提交完一帧就放手，对端**收不到**。
/// - 测试目标：钉住「按需驱动形态要求调用方自己调收尾入口」这一语义——
///   上层只借段、不调 `flush_async` 时，环里的字节永远留在环里。
/// - 测试手段：`tokio::io::duplex` 当设备；容量 64 的环上提交一帧后**什么都不做**；
///   对端用 `timeout(300ms, read_exact(16))` 等这帧。
/// - 判断：对端**超时**（`Elapsed`）。这正是 `smux_v1` 在 UDS 上观察到的静默挂起。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_drive_leaves_committed_frame_in_the_ring_() {
    let (dev, mut peer) = tokio::io::duplex(1024);
    let mut tx = BuffWrite::<_, DefaultAllocConfig>::try_new(dev, 64, DefaultAllocConfig)
        .expect("容量合法");

    submit_once_(&mut tx).await;
    // 这里刻意**不**调 `tx.flush_async()` / `tx.close_async()`，模拟 `smux_v1` 写完就走。

    let mut got = [0u8; 16];
    let waited = tokio::time::timeout(Duration::from_millis(300), peer.read_exact(&mut got)).await;
    assert!(
        waited.is_err(),
        "按需形态下没人推搬运，对端本应收不到；实际收到 {got:?}"
    );
    assert_eq!(tx.data_size(), 16usize, "帧仍在环里：已提交、但没被搬给设备");
}

/// 自驱动形态（`try_new_local`）：同样的「提交完就放手」，对端**收到**。
/// - 测试目标：证明 `try_new_local` 把「环 → 设备」的搬运交给后台泵之后，
///   「提交进环」就等于「迟早送达设备」，与调用方会不会调 flush 无关。
/// - 测试手段：与上一条逐字同形，只把构造入口换成 `try_new_local`，并把整段
///   （建环 + 提交 + 对端读）放进本线程**那条**本地队列的驱动栈
///   （`Runtime::current().local_scope().run_until(..)`）——泵是被投到那条队列上的任务，
///   只有驱动它才会推进。这里刻意**不**自己 `LocalSet::new()`：`try_new_local` 用的是
///   本线程 `thread_local!` 里那条 `LocalSet`（见 `abs_art-tokio` 的 `local_scope`），
///   另建一条等于把泵投到一条没人驱动的队列上。
/// - 判断：对端在超时内收满 16 字节，且与 [`K_FRAME`] 一致。
#[test]
fn local_drive_delivers_after_submit_without_flush_() {
    use abs_art::{FULL, TrLocalScope};
    use abs_art_tokio::Runtime as TokioRuntime;

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

                submit_once_(&mut tx).await; // 同样不调 flush

                let mut got = [0u8; 16];
                tokio::time::timeout(Duration::from_millis(300), peer.read_exact(&mut got))
                    .await
                    .expect("自驱动形态下后台泵应当把这一帧搬出去")
                    .expect("对端读不应出错");
                assert_eq!(got, K_FRAME, "对端收到的应当是那一帧");
            })
            .await;
    });
}

/// 收尾对照组：按需形态只要调用方**记得**调 `flush_async`，同样送得到。
/// - 测试目标：说明上一条的差别是「谁负责驱动」，而不是「按需形态坏了」。
/// - 测试手段：与第一条同形，但在对端读之前补一次 `tx.flush_async().await`。
/// - 判断：对端收满 16 字节，且 `flush_async` 正常返回。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_drive_delivers_once_flush_is_called_() {
    let (dev, mut peer) = tokio::io::duplex(1024);
    let mut tx = BuffWrite::<_, DefaultAllocConfig>::try_new(dev, 64, DefaultAllocConfig)
        .expect("容量合法");

    submit_once_(&mut tx).await;
    tx.flush_async().await.expect("按需形态的收尾应当成功");

    let mut got = [0u8; 16];
    tokio::time::timeout(Duration::from_millis(300), peer.read_exact(&mut got))
        .await
        .expect("调过 flush 之后这一帧应当已经上网")
        .expect("对端读不应出错");
    assert_eq!(got, K_FRAME);
}
