//! 「有数据就交付」的回归用例。
//!
//! 上游 `move_items_from_input_async` 的语义是**填满**段（上限即段长），设备给出少量
//! 字节后转为空闲时它只是 `Pending`——数据被扣在段里、一个字节也不提交。对「请求 /
//! 应答」式上层（例如 `smux_v1` 的握手帧）来说那等于永久静止，因此本 crate 的搬运
//! 每轮只 poll 一次搬移、拿到多少提交多少。这里把它钉住。

use abs_buff::Demand;
use buffex::x_deps::abs_buff;
use buffex_smol_adapt::{BuffRead, DefaultAllocConfig};
use futures_lite::{future::block_on, io::AsyncWriteExt};

/// 测试目标：设备只给出 25 字节（远小于环容量）时，`read_async` 也必须及时交付。
///
/// 手段：环容量 64 KiB 的 `BuffRead` 包住一对真实 UNIX domain socket 的一端；另一端
/// 写入 25 字节并**保持连接**（避免 EOF 这条交付路径）。等待用一个「让出若干轮就判超时」
/// 的守卫与读取竞争，避免用例在实现退化时挂死（smol 侧不引入定时器）。
///
/// 判定：读取先完成，且段内恰有 25 个字节。若实现退回「填满段才提交」，守卫会先让完
/// 轮次并 panic。
#[test]
fn short_message_is_delivered_promptly_() {
    block_on(async {
        let (mut peer, dev) = async_net::unix::UnixStream::pair().expect("建 socket 对应成功");
        let mut rx =
            BuffRead::<_, DefaultAllocConfig>::try_new(dev, 64 * 1024, DefaultAllocConfig)
                .expect("容量合法");
        peer.write_all(&[7u8; 25]).await.expect("对端应能写入");

        let demand = Demand::at_least(1);
        // 适配器的异步入口是 `IntoFuture`（可取消），这里显式转成 `Future` 再手动轮询：
        // 让出太多轮还没交付就判超时（避免用例在实现退化时挂死，也不引入定时器）。
        let mut read = core::pin::pin!(core::future::IntoFuture::into_future(
            rx.read_async(&demand)
        ));
        let mut spins = 0usize;
        let mut some = core::future::poll_fn(|cx| {
            if let core::task::Poll::Ready(out) = core::future::Future::poll(read.as_mut(), cx) {
                return core::task::Poll::Ready(out);
            }
            spins += 1;
            if spins > 10_000usize {
                panic!("短消息应当及时交付（不得等环段填满）");
            }
            cx.waker().wake_by_ref();
            core::task::Poll::Pending
        })
        .await;
        let segm = some.as_mut().pick_left().expect("应当借到读段");
        assert_eq!(segm.least_count(), 25, "交付的字节数应与写入量一致");
    });
}
