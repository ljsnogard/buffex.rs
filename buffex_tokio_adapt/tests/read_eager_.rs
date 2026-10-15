//! 「有数据就交付」的回归用例。
//!
//! 上游 `move_items_from_input_async` 的语义是**填满**段（上限即段长），设备给出少量
//! 字节后转为空闲时它只是 `Pending`——数据被扣在段里、一个字节也不提交。对「请求 /
//! 应答」式上层（例如 `smux_v1` 的握手帧）来说那等于永久静止，因此本 crate 的搬运
//! 每轮只 poll 一次搬移、拿到多少提交多少。这里把它钉住。

use std::time::Duration;

use abs_buff::Demand;
use buffex::x_deps::abs_buff;
use buffex_tokio_adapt::{BuffRead, DefaultAllocConfig};
use tokio::io::{AsyncWriteExt, duplex};

/// 测试目标：设备只给出 25 字节（远小于环容量）时，`read_async` 也必须及时交付。
///
/// 手段：环容量 64 KiB 的 `BuffRead` 包住 `tokio::io::duplex` 的一端；另一端写入
/// 25 字节并**保持连接**（避免 EOF 这条交付路径），随后用 500 ms 超时包住
/// `read_async`。
///
/// 判定：超时之前借到一个可读段，段内恰有 25 个字节、内容为写入的 `7`。若实现退回
/// 「填满段才提交」，本用例会以超时失败。
#[tokio::test(flavor = "current_thread")]
async fn short_message_is_delivered_promptly_() {
    let (mut peer, dev) = duplex(64 * 1024);
    let mut rx =
        BuffRead::<_, DefaultAllocConfig>::try_new(dev, 64 * 1024, DefaultAllocConfig)
            .expect("容量合法");
    peer.write_all(&[7u8; 25]).await.expect("对端应能写入");

    let demand = Demand::at_least(1);
    let mut got = tokio::time::timeout(Duration::from_millis(500), rx.read_async(&demand))
        .await
        .expect("短消息应当及时交付（不得等环段填满）");
    let segm = got.as_mut().pick_left().expect("应当借到读段");
    assert_eq!(segm.least_count(), 25, "交付的字节数应与写入量一致");
}
