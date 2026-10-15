//! 集成测试：环适配器与设备适配器在**按需驱动**下的端到端行为。
//!
//! 这里刻意覆盖三类场景：
//!
//! * **写方向收尾**：段 drop 只是提交进环，`flush_async` / `close_async` 才是「送达
//!   设备」的判据；
//! * **读方向 EOF**：设备读到末尾后，环被取空时调用方应看到 `ConsumerError::Closing`，
//!   而不是永久 park；
//! * **真实 socket**：用 `async_net::unix::UnixStream::pair()` 建一对真实 UNIX domain
//!   socket，让「写适配器 → 内核缓冲 → 读适配器」整条链跑通。
//!
//! 全部场景都只用一个 `block_on`：本 crate 不 spawn 任务，搬运挂在调用方自己的 poll 上，
//! 因此不需要 `LocalExecutor` 之类的执行体上下文。

use std::{
    io,
    mem::MaybeUninit,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use abs_buff::Demand;
use buffex::ring::ConsumerError;
use buffex_smol_adapt::{BuffRead, BuffWrite, DefaultAllocConfig, x_deps::abs_buff};
use futures_lite::{
    future::block_on,
    io::{AsyncWrite, Cursor},
};

/// 可跨任务观察的写设备：把每次写入的内容追加到共享 `Vec<u8>`。
///
/// 为什么需要它：`BuffWrite` 拥有设备，测试无法在收尾后把设备取回来，因此观察点必须放在
/// 设备之外。
struct SharedSink(Arc<Mutex<Vec<u8>>>);

impl AsyncWrite for SharedSink {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut guard = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// 写方向：段提交 → 按需搬运 → `flush_async` 之后字节确实落到设备上。
/// - 测试目标：`BuffWrite` 的收尾语义——「段 drop 只是提交进环」，只有 `flush_async`
///   返回之后才保证已提交数据被设备收下；`close_async` 会继续把设备关闭。
/// - 测试手段：把 `SharedSink`（写入即追加到共享 `Vec`）交给 `BuffWrite::try_new`，
///   借一个写段并搬入 5 个字节，先 `flush_async` 检查设备内容，再 `close_async` 并检查
///   环内已无数据。
/// - 判定标准：`flush_async` 返回 `Ok`；设备收到的字节与源字节逐一相等（顺序一致）；
///   `data_size()` 在 flush 之后为 0；`close_async` 返回 `Ok`。
#[test]
fn write_side_flush_delivers_bytes_() {
    let sink = Arc::new(Mutex::new(Vec::<u8>::new()));
    let mut tx = BuffWrite::try_new(SharedSink(Arc::clone(&sink)), 1024, DefaultAllocConfig)
        .expect("容量合法");

    block_on(async {
        let demand = Demand::at_least(1);
        let some = tx.write_async(&demand).await;
        let mut segm = some.pick_left().expect("环是空的，应当借到写段");
        let src = [
            MaybeUninit::new(1u8),
            MaybeUninit::new(2u8),
            MaybeUninit::new(3u8),
            MaybeUninit::new(4u8),
            MaybeUninit::new(5u8),
        ];
        let moved = segm.move_items_from_buff(&src);
        assert_eq!(moved, 5usize, "应把 5 个字节搬进写段");
        // 段在这里 drop：按已写入量 advance_write，下一轮 pump 才看得到它。
        drop(segm);

        tx.flush_async().await.expect("环应被取空");
        // `flush_async` 的契约就是「环被取空」⇒「已提交数据都被设备收下」，
        // 因此这里可以在它返回之后立刻检查设备侧。
        let n = sink
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len();
        assert_eq!(n, 5usize, "flush 返回时设备应已收下全部 5 个字节");
        tx.close_async().await.expect("写方向应正常关闭");
    });

    let got = sink
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    assert_eq!(got, vec![1u8, 2, 3, 4, 5], "设备应按顺序收到全部字节");
}

/// 读方向：设备字节进环 → 调用方搬出 → 取空后看到 `Closing`（EOF）。
/// - 测试目标：`BuffRead` 应在设备读到末尾之后，把「环被取空」合成为
///   `ConsumerError::Closing`；且设备 EOF **不算**设备错误。
/// - 测试手段：以 `Cursor<Vec<u8>>`（futures-lite 已实现 `AsyncRead`）作设备放进
///   `BuffRead::try_new`，先搬出 3 个字节，再读一次。
/// - 判定标准：第一次拿到 3 字节且内容与设备一致；第二次得到 `ConsumerError::Closing`；
///   `take_error()` 为 `None`。
#[test]
fn read_side_yields_bytes_then_closing_() {
    let mut rx = BuffRead::try_new(Cursor::new(vec![9u8, 8, 7]), 1024, DefaultAllocConfig)
        .expect("容量合法");

    block_on(async {
        let demand = Demand::at_least(1);
        let some = rx.read_async(&demand).await;
        let mut segm = some.pick_left().expect("设备里应有 3 个字节");
        let mut window = [MaybeUninit::<u8>::uninit(); 8];
        // SAFETY: `u8` 不需要 drop，`move_items_to_buff` 的安全契约自动满足；
        // `window` 与段没有任何借用关系。
        let moved = unsafe { segm.move_items_to_buff(&mut window) };
        assert_eq!(moved, 3usize, "应搬出 3 个字节");
        drop(segm);
        // SAFETY: 上面已确认前 3 个字节被初始化。
        let got: Vec<u8> = window[..3]
            .iter()
            .map(|m| unsafe { m.assume_init() })
            .collect();
        assert_eq!(got, vec![9u8, 8, 7], "搬出的字节应与设备一致");

        let some = rx.read_async(&demand).await;
        assert!(
            matches!(some.pick_right(), Some(ConsumerError::Closing)),
            "设备结束且环取空之后应给出 Closing"
        );
        assert!(rx.take_error().is_none(), "EOF 不应被记成设备错误");
    });
}

/// 真实 socket：写适配器 → UNIX domain socket → 读适配器，字节与 EOF 全对。
/// - 测试目标：两个环适配器各自接住同一对 UDS 的一端时，整条链（含内核缓冲与
///   `poll_close`）能完成一次有界往返，读端在数据之后看到 `Closing`。
/// - 测试手段：用 `async_net::unix::UnixStream::pair()` 建一对 socket，a 端交给
///   `BuffWrite`、b 端交给 `BuffRead`；写入 8 字节并 `close_async`，然后从读端搬出并比对。
/// - 判定标准：读端的 8 个字节与写入完全一致；随后读到 `ConsumerError::Closing`；
///   两侧收尾都返回 `Ok`。
#[test]
fn uds_roundtrip_between_two_adapters_() {
    let (dev_a, dev_b) = async_net::unix::UnixStream::pair().expect("应能建立 UNIX socket 对");
    let mut tx = BuffWrite::try_new(dev_a, 4096, DefaultAllocConfig).expect("容量合法");
    let mut rx = BuffRead::try_new(dev_b, 4096, DefaultAllocConfig).expect("容量合法");

    block_on(async {
        let payload = [10u8, 20, 30, 40, 50, 60, 70, 80];

        let demand = Demand::at_least(1);
        let some = tx.write_async(&demand).await;
        let mut segm = some.pick_left().expect("环是空的，应当借到写段");
        let src: Vec<MaybeUninit<u8>> = payload.iter().copied().map(MaybeUninit::new).collect();
        assert_eq!(
            segm.move_items_from_buff(&src),
            payload.len(),
            "应把全部载荷搬进写段"
        );
        drop(segm);
        tx.close_async().await.expect("写方向应正常关闭");

        let some = rx.read_async(&demand).await;
        let mut segm = some.pick_left().expect("对端已写入 8 个字节");
        let mut window = [MaybeUninit::<u8>::uninit(); 8];
        // SAFETY: 同 `read_side_yields_bytes_then_closing_`。
        let moved = unsafe { segm.move_items_to_buff(&mut window) };
        assert_eq!(moved, payload.len(), "应搬出全部载荷");
        drop(segm);
        // SAFETY: 上面已确认前 8 个字节被初始化。
        let got: Vec<u8> = window.iter().map(|m| unsafe { m.assume_init() }).collect();
        assert_eq!(got, payload.to_vec(), "跨 socket 收到的字节应与发送一致");

        let some = rx.read_async(&demand).await;
        assert!(
            matches!(some.pick_right(), Some(ConsumerError::Closing)),
            "对端关闭写方向且环取空之后应给出 Closing"
        );
    });
}

/// 按需驱动的 `try_read` 不依赖任何执行体：设备有数据时同步就能借到段。
/// - 测试目标：`TrBuffTryRead::try_read` 在自己的 poll 里推一轮搬运，因此在**同步**
///   上下文（没有执行体、没有 `block_on`）中也能拿到设备已经就绪的数据。
/// - 测试手段：设备用 `Cursor<Vec<u8>>`（`poll_read` 立即就绪），直接在同步代码里调用
///   `try_read` 并窥视段内容。
/// - 判定标准：`try_read` 借到段，且 `iter_slices` 拼出的字节等于设备内容。
#[test]
fn try_read_pumps_synchronously_() {
    let mut rx = BuffRead::try_new(Cursor::new(vec![4u8, 5, 6]), 1024, DefaultAllocConfig)
        .expect("容量合法");
    let demand = Demand::at_least(1);
    let some = rx.try_read(&demand);
    let segm = some.pick_left().expect("设备已就绪，同步借段应当成功");
    let got: Vec<u8> = segm.iter_slices().flatten().copied().collect();
    assert_eq!(got, vec![4u8, 5, 6], "窥视到的字节应与设备一致");
}
