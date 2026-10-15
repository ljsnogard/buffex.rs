//! 集成测试：适配器在任何「无进展」情形下都不得空转（100% CPU）。
//!
//! 背景：`buffex_uds_comp_experiment` 在 `2026-10-02` 观察到 tokio 侧进程 100% CPU
//! 空转——`BuffWrite::pump_` 把 `SomeOf::Both(0, err)` 当成「有进展」，于是
//! `flush_async` / `close_async` 的 `while pump_().await {}` 永不退出，设备错误
//! （对端退出后的 `EPIPE`）同时被吞掉。
//!
//! 这里的设备都是「立刻 `Ready`」的，因此空转时**循环里没有任何让出点**：连
//! `tokio::time::timeout` 都无法插进来。判定手段改用「设备被调用次数」——每个设备
//! 都带一个次数上限，超限就 panic；正常路径下调用次数是个位数，空转则会立刻触发
//! 断言而不是把测试挂死。

use core::{
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
};
use std::{io, sync::Arc};

use tokio::io::AsyncWrite;

// `abs_buff` 是 `buffex` 的再导出依赖，测试里通过这条 `use` 把它引入根作用域，
// 之后的 `abs_buff::…` 路径才可解析（与 `tests/pump_.rs` 的写法一致）。
use buffex::x_deps::abs_buff;

use abs_buff::{
    Demand,
    buffer::TrBuffSegmMut,
};
use buffex_tokio_adapt::{BuffWrite, DefaultAllocConfig};

/// 空转护栏：统计设备被调用的次数，超过上限即 panic。
///
/// 上限的意义是把「空转」从「测试永久挂住」变成「测试立刻失败并指出调用次数」。
#[derive(Debug)]
struct SpinGuard {
    /// 设备被调用的次数。
    calls_: AtomicUsize,
    /// 允许的调用次数上限。
    limit_: usize,
}

impl SpinGuard {
    /// 造一个上限为 `limit` 的护栏。
    fn new_(limit: usize) -> Self {
        SpinGuard {
            calls_: AtomicUsize::new(0),
            limit_: limit,
        }
    }

    /// 记一次调用；超过上限就 panic（说明被测代码在空转）。
    fn tick_(&self, what: &str) {
        let n = self.calls_.fetch_add(1, Ordering::SeqCst) + 1;
        assert!(
            n <= self.limit_,
            "空转：{what} 被调用 {n} 次（上限 {}）",
            self.limit_
        );
    }

    /// 当前调用次数。
    fn count_(&self) -> usize {
        self.calls_.load(Ordering::SeqCst)
    }
}

/// 一个「永远写失败」的设备：每次 `poll_write` 都返回 `BrokenPipe`。
#[derive(Debug)]
struct BrokenPipeDev {
    /// 调用次数护栏。
    guard_: Arc<SpinGuard>,
}

impl AsyncWrite for BrokenPipeDev {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.guard_.tick_("BrokenPipeDev::poll_write");
        Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// 一个「永远写 0 字节」的设备：每次 `poll_write` 都返回 `Ok(0)`。
///
/// `Ok(0)` + 非空缓冲是设备违约：上游 `move_items_into_output_async` 会因此空转，
/// 适配器在设备边界把它翻成 `WriteZero`（见 `ZeroWriteAsErr`）。
#[derive(Debug)]
struct ZeroWriteDev {
    /// 调用次数护栏。
    guard_: Arc<SpinGuard>,
}

impl AsyncWrite for ZeroWriteDev {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.guard_.tick_("ZeroWriteDev::poll_write");
        Poll::Ready(Ok(0))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// 一个「先收下前 `accept_max` 字节、之后一律 `BrokenPipe`」的设备。
///
/// 用来制造 `SomeOf::Both(n > 0, err)`：部分字节确实写出去了，同时带设备错误。
#[derive(Debug)]
struct PartialThenErrDev {
    /// 调用次数护栏。
    guard_: Arc<SpinGuard>,
    /// 已经收下的字节数。
    accepted_: usize,
    /// 允许收下的字节上限。
    accept_max_: usize,
}

impl AsyncWrite for PartialThenErrDev {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.guard_.tick_("PartialThenErrDev::poll_write");
        let me = self.as_mut().get_mut();
        let room = me.accept_max_.saturating_sub(me.accepted_);
        if room == 0 {
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)));
        }
        let n = room.min(buf.len());
        me.accepted_ += n;
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// 往写端提交 `payload` 到环里（只提交，不保证送达设备）。
async fn submit_<W>(tx: &mut BuffWrite<W, DefaultAllocConfig>, payload: &[u8])
where
    W: AsyncWrite + Unpin,
{
    let mut off = 0usize;
    while off < payload.len() {
        let demand = Demand::at_least(1);
        let some = tx.write_async(&demand).await;
        let mut segm = some.pick_left().expect("应能借出写段");
        let moved = segm.move_items_from_as_buff(&payload[off..]);
        assert!(moved > 0, "借出的写段不应为空");
        off += moved;
        drop(segm);
    }
}

/// 把一台**真实**设备包起来，只对 `poll_write` 计数（其余原样转发）。
///
/// 用途：让「真实 tokio 设备路径」也能用同一条空转护栏判定，而不必依赖
/// `tokio::time::timeout`（空转时循环不让出执行体，定时器根本不会触发）。
#[derive(Debug)]
struct CountingDev<W> {
    /// 被包装的真实设备。
    inner_: W,
    /// `poll_write` 调用次数护栏。
    guard_: Arc<SpinGuard>,
}

impl<W> AsyncWrite for CountingDev<W>
where
    W: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.guard_.tick_("CountingDev::poll_write");
        let me = self.as_mut().get_mut();
        Pin::new(&mut me.inner_).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let me = self.as_mut().get_mut();
        Pin::new(&mut me.inner_).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let me = self.as_mut().get_mut();
        Pin::new(&mut me.inner_).poll_shutdown(cx)
    }
}

/// 设备「0 字节 + 错误」时 `flush_async` 必须有界返回，并保留设备错误。
/// - 测试目标：`BuffWrite::flush_async` 不会在「一个字节都没写出且带错误」时空转。
/// - 测试手段：设备每次 `poll_write` 都返回 `BrokenPipe`，并带 8 次调用的空转护栏；
///   先经 `write_async` 把 16 字节提交进环，再 await `flush_async`。
/// - 判断：`flush_async` 返回 `Err`；`take_error` 取回的是 `BrokenPipe`（错误不许被
///   吞掉）；设备调用次数不超过护栏（超限时设备会 panic，测试直接失败）。
#[tokio::test(flavor = "current_thread")]
async fn flush_does_not_spin_on_zero_bytes_with_error_() {
    let guard = Arc::new(SpinGuard::new_(8));
    let dev = BrokenPipeDev {
        guard_: guard.clone(),
    };
    let mut tx =
        BuffWrite::<_, DefaultAllocConfig>::try_new(dev, 4096, DefaultAllocConfig).expect("容量合法");
    submit_(&mut tx, &[7u8; 16]).await;

    let res = tx.flush_async().await;
    assert!(res.is_err(), "设备写失败时 flush_async 应报错，实得 {res:?}");
    let (err, _tag) = tx.take_error().expect("设备错误必须被记录，不许被吞掉");
    assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    assert!(guard.count_() <= 8, "设备调用次数异常：{}", guard.count_());
}

/// 设备「0 字节 + 错误」时 `close_async` 同样必须有界返回。
/// - 测试目标：收尾路径（冲净 + `shutdown`）不因同一个坑空转。
/// - 测试手段：同上一例，换成 await `close_async`。
/// - 判断：返回 `Err`，且 `take_error` 能取回 `BrokenPipe`。
#[tokio::test(flavor = "current_thread")]
async fn close_does_not_spin_on_zero_bytes_with_error_() {
    let guard = Arc::new(SpinGuard::new_(8));
    let dev = BrokenPipeDev {
        guard_: guard.clone(),
    };
    let mut tx =
        BuffWrite::<_, DefaultAllocConfig>::try_new(dev, 4096, DefaultAllocConfig).expect("容量合法");
    submit_(&mut tx, &[9u8; 16]).await;

    let res = tx.close_async().await;
    assert!(res.is_err(), "设备写失败时 close_async 应报错，实得 {res:?}");
    let (err, _tag) = tx.take_error().expect("设备错误必须被记录，不许被吞掉");
    assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    assert!(guard.count_() <= 8, "设备调用次数异常：{}", guard.count_());
}

/// 「写出了一部分又出错」（`Both(n > 0, err)`）时错误同样必须被记下、循环必须终止。
/// - 测试目标：部分交付不掩盖设备错误（旧实现只看「含 `Left`」就当成有进展，
///   会把这类错误吞掉并继续空转）。
/// - 测试手段：设备先收下 8 字节、之后一律 `BrokenPipe`；提交 16 字节后
///   await `flush_async`。
/// - 判断：返回 `Err`；`take_error` 取回 `BrokenPipe`；设备调用次数很少（部分写成功
///   的那次 + 报错的那次，不会反复重试）。
#[tokio::test(flavor = "current_thread")]
async fn partial_delivery_still_records_device_error_() {
    let guard = Arc::new(SpinGuard::new_(8));
    let dev = PartialThenErrDev {
        guard_: guard.clone(),
        accepted_: 0,
        accept_max_: 8,
    };
    let mut tx =
        BuffWrite::<_, DefaultAllocConfig>::try_new(dev, 4096, DefaultAllocConfig).expect("容量合法");
    submit_(&mut tx, &[3u8; 16]).await;

    let res = tx.flush_async().await;
    assert!(res.is_err(), "设备写失败时 flush_async 应报错，实得 {res:?}");
    let (err, _tag) = tx.take_error().expect("设备错误必须被记录，不许被吞掉");
    assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    assert!(guard.count_() <= 8, "设备调用次数异常：{}", guard.count_());
}

/// 设备在非空缓冲上长期 `Ok(0)`（既无进展也无错误）时，收尾必须报错而不是无限重试。
/// - 测试目标：`flush_async` 面对违约的「零字节设备」时不会永久重试。
/// - 测试手段：设备每次 `poll_write` 都返回 `Ok(0)`；提交 16 字节后 await
///   `flush_async`。
/// - 判断：返回 `Err`；`take_error` 取回的错误是 `WriteZero`（零字节设备在适配器里
///   被翻译成终止性错误）；设备调用次数有界。
#[tokio::test(flavor = "current_thread")]
async fn flush_does_not_spin_on_zero_progress_without_error_() {
    let guard = Arc::new(SpinGuard::new_(8));
    let dev = ZeroWriteDev {
        guard_: guard.clone(),
    };
    let mut tx =
        BuffWrite::<_, DefaultAllocConfig>::try_new(dev, 4096, DefaultAllocConfig).expect("容量合法");
    submit_(&mut tx, &[5u8; 16]).await;

    let res = tx.flush_async().await;
    assert!(
        res.is_err(),
        "零进展且无错误时 flush_async 无法完成，应报错而非空转，实得 {res:?}"
    );
    let (err, _tag) = tx
        .take_error()
        .expect("零进展必须被记成错误，否则调用方无从判断");
    assert_eq!(err.kind(), io::ErrorKind::WriteZero);
    assert!(guard.count_() <= 8, "设备调用次数异常：{}", guard.count_());
}

/// 真实 tokio 设备（`duplex`）的写半部在对端退出后必须让收尾有界返回。
/// - 测试目标：复现实验里最典型的现场——「对端已退出，服务端仍回写」，设备给出
///   `EPIPE` / `BrokenPipe`；`close_async` 必须终止并如实报错，而不是空转。
/// - 测试手段：建一个 `tokio::io::duplex`，立刻 drop 对端（写半部此后写必失败）；
///   把剩下的写半部经 [`CountingDev`] 包一层计数，再经 `write_async` 提交 64 字节，
///   最后 await `close_async`。
/// - 判断：`close_async` 返回 `Err`；`take_error` 取回的错误是 `BrokenPipe`；
///   设备调用次数有界（空转会触发 [`SpinGuard`] panic）。
#[tokio::test(flavor = "current_thread")]
async fn close_records_broken_pipe_instead_of_spinning_() {
    let guard = Arc::new(SpinGuard::new_(8));
    let (dev, peer) = tokio::io::duplex(1024);
    // 对端立刻退出：此后写半部上的写入会得到 `BrokenPipe`。
    drop(peer);

    let mut tx = BuffWrite::<_, DefaultAllocConfig>::try_new(
        CountingDev {
            inner_: dev,
            guard_: guard.clone(),
        },
        4096,
        DefaultAllocConfig,
    )
    .expect("容量合法");
    submit_(&mut tx, &[1u8; 64]).await;

    let res = tx.close_async().await;
    assert!(res.is_err(), "对端已退出时 close_async 应报错，实得 {res:?}");
    let (err, _tag) = tx.take_error().expect("设备错误必须被记录，不许被吞掉");
    assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    assert!(guard.count_() <= 8, "设备调用次数异常：{}", guard.count_());
}
