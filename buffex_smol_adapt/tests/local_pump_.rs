//! 自驱动形态（`BuffWrite::try_new_local`）的回归用例。
//!
//! 这里验的是**本 crate 曾经缺失的那件事**：适配器自己把「环 → 设备」驱动到底，
//! 而不是等调用方下一次借段时顺带推一轮。协议栈若把适配器当纯 `TrBuffWrite` 用
//! （提交完就挂到别处等），按需驱动形态会把最后一段永久留在环里。
//!
//! 驱动方式：smol 的本地队列是本线程的 `thread_local!`（`abs_art-smol` 的
//! `LocalScope` 是它的别名），因此测试用 `block_on(scope.run_until(..))` 同时驱动
//! 「队列（后台泵）」与「场景主体」。

use abs_art::{SPAWN_LOCAL, TrLocalScope};
use abs_art_smol::Runtime;
use abs_buff::{Demand, TrBuffWrite, buffer::TrBuffSegmMut};
use buffex_smol_adapt::{BuffWrite, DefaultAllocConfig, x_deps::abs_buff};
use futures_lite::{future::block_on, io::AsyncReadExt};

/// 把 `payload` 全部写进写端（借段 → 搬 → drop 提交）。
async fn write_all_<T>(tx: &mut T, payload: &[u8])
where
    T: TrBuffWrite<u8> + Unpin,
    for<'s> T::SegmMut<'s>: TrBuffSegmMut<'s, u8>,
{
    let mut off = 0usize;
    while off < payload.len() {
        let demand = Demand::at_least(1);
        let some = tx.write_async(&demand).await;
        let mut segm = some.pick_left().expect("写入：环应当可借段");
        let moved = segm.move_items_from_as_buff(&payload[off..]);
        assert!(moved > 0, "借出的写段不应为空");
        off += moved;
        drop(segm); // 提交进环：此后由后台泵搬给设备
    }
}

/// 测试目标：`try_new_local` 的后台泵**独立于调用方**把已提交的字节送达设备。
///
/// 手段：`Runtime::<{ SPAWN_LOCAL }>` 交出本线程的本地队列；用一对真实 UNIX domain
/// socket 做设备，写端走自驱动形态；提交一段负载之后调用方**不再碰适配器**（既不
/// `flush_async` 也不 `write_async`），只是 await「对端读满」。
///
/// 判定：对端读到与写入逐字节相同的负载，且适配器的 `data_size()` 归零。若后台泵
/// 不存在（按需驱动的语义泄漏到本入口），对端永远等不到数据，测试会挂起而不是通过。
#[test]
fn local_pump_delivers_without_caller_driving_() {
    let scope = Runtime::<{ SPAWN_LOCAL }>::current().local_scope();
    block_on(scope.run_until(async {
        let payload: Vec<u8> = (0..4096usize).map(|i| (i % 251) as u8).collect();
        let (dev, mut peer) = async_net::unix::UnixStream::pair().expect("建 socket 对应成功");
        let mut tx =
            BuffWrite::<_, DefaultAllocConfig>::try_new_local(dev, 4096, DefaultAllocConfig)
                .expect("自驱动形态应当构造成功");

        write_all_(&mut tx, &payload).await;

        let mut got = vec![0u8; payload.len()];
        peer.read_exact(&mut got).await.expect("对端应当读满");
        assert_eq!(got, payload, "后台泵搬过去的字节必须与写入完全一致");
        assert_eq!(tx.data_size(), 0usize, "送达之后环应当被取空");
    }));
}

/// 测试目标：**丢适配器**不会丢掉「已提交进环、还没上网」的字节。
///
/// 手段：同上的装置，提交一段负载后直接 `drop(tx)`（不调任何收尾入口），随后继续在
/// 同一条本地队列上驱动并等对端读满。
///
/// 判定：对端仍读到完整负载——`drop` 走的是「关生产端 + detach 泵」，泵把环取空、
/// `close` 设备之后才退出。若 `drop` 改成取消泵（smol 的 `Task` 在 drop 时默认取消），
/// 这段负载就永远出不去。
#[test]
fn dropping_adapter_still_flushes_committed_bytes_() {
    let scope = Runtime::<{ SPAWN_LOCAL }>::current().local_scope();
    block_on(scope.run_until(async {
        let payload: Vec<u8> = (0..4096usize).map(|i| (i % 241) as u8).collect();
        let (dev, mut peer) = async_net::unix::UnixStream::pair().expect("建 socket 对应成功");
        let mut tx =
            BuffWrite::<_, DefaultAllocConfig>::try_new_local(dev, 4096, DefaultAllocConfig)
                .expect("自驱动形态应当构造成功");

        write_all_(&mut tx, &payload).await;
        drop(tx);

        let mut got = vec![0u8; payload.len()];
        peer.read_exact(&mut got).await.expect("对端应当读满");
        assert_eq!(got, payload, "丢适配器之后泵仍须把已提交的字节搬完");
    }));
}
