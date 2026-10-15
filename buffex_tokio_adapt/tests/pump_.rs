#![feature(allocator_ext)]

//! 集成测试：两个适配器在真实 tokio 运行时下的验收。
//!
//! 覆盖：入向（设备 → 环 → 调用方）、出向（调用方 → 环 → 设备）、背压、设备 EOF 合成
//! `Closing`、设备错误取回。全部只用 crate 的公开 API 与 `abs_buff` / `buffex` 的
//! 公开 trait。
//!
//! 最后一例验证 [`AllocatorConfig`]：每个分配点都可以换成不同的分配器。
//! 用了 `allocator_api`，因此与调用方一样需要 nightly 的该 feature。

use core::mem::MaybeUninit;

use std::{
    alloc::{Allocator, Global},
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    vec,
    vec::Vec,
};
use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};

use abs_buff::{
    Demand, TrBuffRead, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView},
};
use buffex::{ring::Ring, x_deps::abs_buff};
use buffex_tokio_adapt::{
    TrAllocConfig, BuffRead, BuffWrite,
    DefaultAllocConfig, InvalidCapacity, RingBufOf,
};

/// 确定性负载：第 `i` 个字节为 `(i % 251) as u8`。
fn payload_(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// 从任意实现了 `TrBuffRead` 的读端搬出 `want` 字节（搬出多少即消费多少）。
async fn drain_read_<T>(rx: &mut T, want: usize) -> Vec<u8>
where
    T: TrBuffRead<u8> + Unpin,
    for<'s> T::SegmRef<'s>: TrBuffSegmRef<'s, u8>,
    T::Err: core::fmt::Debug,
{
    let mut got = Vec::with_capacity(want);
    let mut staging: Vec<MaybeUninit<u8>> = Vec::new();
    while got.len() < want {
        // 只用 `at_least(1)`：设备的短读会让环里只有零星几字节，若按「还差多少」
        // 当下限（`at_least(剩余)`），环在写端未关闭时**不会**交出不足下限的数据，
        // 于是永远借不出段。这是 `ring` 的有意语义，调用方应按「拿到多少算多少」
        // 消费（协议层再自行拼帧）。
        let demand = Demand::at_least(1);
        let some = rx.read_async(&demand).await;
        let mut segm = some.pick_left().expect("应能借出读段");
        let now = segm.least_count().min(want - got.len());
        if staging.len() < now {
            staging.resize(now, MaybeUninit::uninit());
        }
        let moved = segm.move_items_to_buff(&mut staging[..now]);
        // SAFETY: `move_items_to_buff` 已初始化前 `moved` 个元素，且
        // `MaybeUninit<u8>` 与 `u8` 同布局、`u8` 无无效位模式。
        let bytes =
            unsafe { core::slice::from_raw_parts(staging.as_ptr().cast::<u8>(), moved) };
        got.extend_from_slice(bytes);
        drop(segm); // 提交：唤醒等待的写端
    }
    got
}

/// 把 `payload` 写进任意实现了 `TrBuffWrite` 的写端，并在写完段后 drop（提交进环）。
async fn fill_write_<T>(tx: &mut T, payload: &[u8])
where
    T: TrBuffWrite<u8> + Unpin,
    for<'s> T::SegmMut<'s>: TrBuffSegmMut<'s, u8>,
    T::Err: core::fmt::Debug,
{
    let mut off = 0usize;
    while off < payload.len() {
        let demand = Demand::at_least(1);
        let some = tx.write_async(&demand).await;
        let mut segm = some.pick_left().expect("应能借出写段");
        let moved = segm.move_items_from_as_buff(&payload[off..]);
        assert!(moved > 0, "借出的写段不应为空");
        off += moved;
        drop(segm); // 提交：进入环，等待泵搬给设备
    }
}

/// 入向：设备 → 环 → 调用方。
/// - 测试目标：`BuffRead` 把任意 `AsyncRead` 设备的数据按需搬进环并借给调用方。
/// - 测试手段：`tokio::io::duplex` 一端作为设备，另一端任务写入 32 KiB；容量 1024 的
///   环必然反复走到「环满 → 设备侧待写」与「环空 → 泵读设备」两条路径。
/// - 判断：调用方抽出的 32 KiB 与负载逐字节一致。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buff_read_delivers_device_bytes_() {
    const TOTAL: usize = 32 * 1024;
    let payload = payload_(TOTAL);
    let (dev, mut peer) = tokio::io::duplex(4096);

    let feed = payload.clone();
    let writer = tokio::spawn(async move {
        peer.write_all(&feed).await.expect("对端应能写完");
        peer.shutdown().await.expect("对端应能关闭");
    });

    let mut rx = BuffRead::<_, DefaultAllocConfig>::try_new(dev, 1024, DefaultAllocConfig).expect("容量合法");
    let got = drain_read_(&mut rx, TOTAL).await;

    writer.await.expect("写端任务不应 panic");
    assert_eq!(got, payload, "入向搬出的字节应与设备数据一致");
}

/// 出向：调用方 → 环 → 设备。
/// - 测试目标：`BuffWrite` 把调用方写进环的数据搬给任意 `AsyncWrite` 设备。
/// - 测试手段：容量 512 的环上写入 32 KiB（必然先填满环、再边冲边写），对端任务
///   `read_exact` 收满，主任务在写完段后 `flush_async` 收尾。
/// - 判断：设备侧收到的字节与负载逐字节一致，且 `flush_async` 正常返回。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buff_write_delivers_bytes_to_device_() {
    const TOTAL: usize = 32 * 1024;
    let payload = payload_(TOTAL);
    let (dev, mut peer) = tokio::io::duplex(2048);

    let reader = tokio::spawn(async move {
        let mut got = vec![0u8; TOTAL];
        tokio::io::AsyncReadExt::read_exact(&mut peer, &mut got)
            .await
            .expect("对端应收到全部字节");
        got
    });

    let mut tx = BuffWrite::<_, DefaultAllocConfig>::try_new(dev, 512, DefaultAllocConfig).expect("容量合法");
    fill_write_(&mut tx, &payload).await;
    tx.flush_async().await.expect("冲刷应正常结束");

    let got = reader.await.expect("读端任务不应 panic");
    assert_eq!(got, payload, "出向送达设备的字节应与负载一致");
    assert!(tx.take_error().is_none(), "不应有设备写错误");
}

/// 出向：`close_async` 冲刷剩余数据并关闭设备。
/// - 测试目标：段 drop 只是提交进环，`close_async` 才保证数据真正送达并 `shutdown`。
/// - 测试手段：写入小于环容量的数据后立刻 `close_async`，对端读到 EOF 为止。
/// - 判断：对端收满全部字节且看到 EOF（`read_to_end` 正常返回）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buff_write_close_flushes_and_shuts_down_() {
    const TOTAL: usize = 4096;
    let payload = payload_(TOTAL);
    let (dev, mut peer) = tokio::io::duplex(256);

    let reader = tokio::spawn(async move {
        let mut got = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut peer, &mut got)
            .await
            .expect("对端应读到 EOF");
        got
    });

    let mut tx = BuffWrite::<_, DefaultAllocConfig>::try_new(dev, 8192, DefaultAllocConfig).expect("容量合法");
    fill_write_(&mut tx, &payload).await;
    tx.close_async().await.expect("关闭应正常结束");

    let got = reader.await.expect("读端任务不应 panic");
    assert_eq!(got, payload, "关闭前应把环内数据全部送达");
}

/// 入向：设备 EOF 被合成为 `ConsumerError::Closing`。
/// - 测试目标：环读空 + 设备结束 ⇒ 调用方看到 `Closing`，而不是无限 `Drained`。
/// - 测试手段：容量 64 的环上喂 200 字节，随后立刻关闭设备；循环 `read_async` 直到
///   拿到 `Closing`，累计搬出的字节。
/// - 判断：拿到的是 `Closing`（不是 `Drained`），且搬出的 200 字节与负载一致。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buff_read_synthesises_closing_on_device_eof_() {
    const TOTAL: usize = 200;
    let payload = payload_(TOTAL);
    let (dev, mut peer) = tokio::io::duplex(1024);

    let feed = payload.clone();
    tokio::spawn(async move {
        peer.write_all(&feed).await.expect("对端应能写完");
        peer.shutdown().await.expect("对端应能关闭");
    });

    let mut rx = BuffRead::<_, DefaultAllocConfig>::try_new(dev, 64, DefaultAllocConfig).expect("容量合法");
    let mut got = Vec::new();
    let mut staging: Vec<MaybeUninit<u8>> = vec![MaybeUninit::uninit(); 64];
    let closing = loop {
        let demand = Demand::at_least(1);
        let some = rx.read_async(&demand).await;
        let mut segm = match some.pick_left() {
            Option::Some(segm) => segm,
            Option::None => break true,
        };
        let now = segm.least_count().min(staging.len());
        let moved = unsafe {
            // SAFETY: `T = u8` 无析构资源；`staging` 与环段无别名。
            segm.move_items_to_buff(&mut staging[..now])
        };
        // SAFETY: 同上，前 `moved` 个元素已初始化。
        let bytes =
            unsafe { core::slice::from_raw_parts(staging.as_ptr().cast::<u8>(), moved) };
        got.extend_from_slice(bytes);
        drop(segm);
    };

    assert!(closing, "设备 EOF 后应报 Closing");
    assert_eq!(got, payload, "EOF 前已搬入的字节不应丢失");
    assert!(rx.take_error().is_none(), "正常 EOF 不应记成错误");
}

/// 故意失败的设备：每次 `poll_read` 都返回 IO 错误。
struct FailingRead;

impl AsyncRead for FailingRead {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "设备坏了")))
    }
}

/// 入向：设备 IO 错误被取回，且读端据此报 `Closing`。
/// - 测试目标：设备错误不会让适配器 panic，而是被暂存并经 `take_error` 取回；空环上
///   读取应报 `Closing`。
/// - 测试手段：用一个 `poll_read` 永远返回 `BrokenPipe` 的设备构造 `BuffRead`，
///   先 `read_async` 拿一次结果，再 `take_error`。
/// - 判断：读端报 `Closing`，且 `take_error` 给出 `BrokenPipe`。
#[tokio::test]
async fn buff_read_records_device_error_() {
    let mut rx = BuffRead::<_, DefaultAllocConfig>::try_new(FailingRead, 64, DefaultAllocConfig).expect("容量合法");

    let demand = Demand::at_least(1);
    let some = rx.read_async(&demand).await;
    let err = some
        .pick_right()
        .expect("设备出错且环为空时应报错");
    assert_eq!(
        err,
        buffex::ring::ConsumerError::<usize>::Closing,
        "环空 + 设备出错应报 Closing"
    );

    let (io_err, tag) = rx.take_error().expect("设备错误应被取回");
    assert_eq!(io_err.kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(tag, abs_buff::error::ReadErrTag::Propagated);
}


/// 记账分配器：转发给 `Global`，但记录每次分配。
#[derive(Clone, Debug, Default)]
struct CountingAlloc {
    events: Arc<std::sync::atomic::AtomicUsize>,
}

impl CountingAlloc {
    fn events_(&self) -> usize {
        self.events.load(std::sync::atomic::Ordering::Relaxed)
    }
}

unsafe impl Allocator for CountingAlloc {
    fn allocate(
        &self,
        layout: core::alloc::Layout,
    ) -> Result<core::ptr::NonNull<[u8]>, core::alloc::AllocError> {
        self.events
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Global.allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: core::ptr::NonNull<u8>, layout: core::alloc::Layout) {
        unsafe { Global.deallocate(ptr, layout) }
    }

    unsafe fn grow(
        &self,
        ptr: core::ptr::NonNull<u8>,
        old: core::alloc::Layout,
        new: core::alloc::Layout,
    ) -> Result<core::ptr::NonNull<[u8]>, core::alloc::AllocError> {
        self.events
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        unsafe { Global.grow(ptr, old, new) }
    }
}

unsafe impl core::alloc::AllocatorClone for CountingAlloc {}

/// 出向辅助：把写端适配器里已提交的数据全部冲给设备。
async fn drain_write_<W, C, S>(tx: &mut BuffWrite<W, C, S>)
where
    W: tokio::io::AsyncWrite + Unpin,
    C: TrAllocConfig,
    S: core::borrow::Borrow<Ring<RingBufOf<C>, u8>>,
{
    let _ = tx.flush_async().await;
}

/// 逐点配置的示例：**每个分配点用不同的记账分配器**，于是可以逐个断言「这个点确实
/// 走了我给的那个分配器」。
#[derive(Clone, Debug, Default)]
struct CountingConfig {
    ring_body: CountingAlloc,
    ring_shared: CountingAlloc,
}

impl TrAllocConfig for CountingConfig {
    type RingBodyAlloc = CountingAlloc;
    type RingSharedAlloc = CountingAlloc;

    fn ring_body_alloc(&self) -> Self::RingBodyAlloc {
        self.ring_body.clone()
    }
    fn ring_shared_alloc(&self) -> Self::RingSharedAlloc {
        self.ring_shared.clone()
    }
}

/// 空的出向设备：收下所有字节但不做别的事。
struct NullSink;

impl tokio::io::AsyncWrite for NullSink {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// `AllocatorConfig`：每个分配点各自生效，且可以逐点给不同分配器。
/// - 测试目标：两个分配点（环体 / 共享容器）分别走配置里对应的那个分配器，而不是被
///   写死成 `Global`；同一个 config 可以同时用于读向与写向。
/// - 测试手段：两个分配点各配一个独立记账的 `CountingAlloc`，分别构造 `BuffRead`、
///   `BuffWrite`（空设备 `NullSink`），跑一次入向与出向搬运后比较计数器的增量。
/// - 判断：构造后两个计数各 > 0；两个方向的搬运都是零拷贝，因此跑完一圈计数**不再
///   增长**；两个计数器互不串台。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn allocator_config_applies_per_allocation_site_() {
    let cfg = CountingConfig::default();
    let ring_body0 = cfg.ring_body.events_();
    let ring_shared0 = cfg.ring_shared.events_();

    // ① 构造：环体 + 共享容器应当各自走自己的分配器。
    let dev_r = tokio::io::empty();
    let dev_w = NullSink;
    let mut rx =
        BuffRead::<_, CountingConfig>::try_new(dev_r, 256, cfg.clone()).expect("容量合法");
    let mut tx =
        BuffWrite::<_, CountingConfig>::try_new(dev_w, 256, cfg.clone()).expect("容量合法");

    assert!(
        cfg.ring_body.events_() > ring_body0,
        "环的缓冲本体应当走 C::RingBodyAlloc"
    );
    assert!(
        cfg.ring_shared.events_() > ring_shared0,
        "装环的 Arc 应当走 C::RingSharedAlloc"
    );
    let ring_body_after_new = cfg.ring_body.events_();
    let ring_shared_after_new = cfg.ring_shared.events_();

    // ② 入向搬运：设备是空的（立即 EOF）。零拷贝路径不该再产生分配。
    let demand = Demand::at_least(1);
    let _ = rx.read_async(&demand).await;
    assert_eq!(
        cfg.ring_body.events_(),
        ring_body_after_new,
        "入向搬运应当零拷贝，不再分配缓冲"
    );

    // ③ 出向搬运：写一点数据再冲出去。
    {
        let demand = Demand::at_least(1);
        let some = tx.write_async(&demand).await;
        let mut segm = some.pick_left().expect("应能借出写段");
        let n = segm.move_items_from_buff(&[MaybeUninit::new(1u8)][..]);
        assert_eq!(n, 1, "应写入 1 字节");
    }
    drain_write_(&mut tx).await;
    assert_eq!(
        cfg.ring_shared.events_(),
        ring_shared_after_new,
        "出向搬运应当零拷贝，不再分配容器"
    );
    let _ = tx.take_error();
}

/// `InvalidCapacity` 在容量越界时给出可读错误。
/// - 测试目标：构造期容量校验失败不会 panic，而是返回带容量的错误。
/// - 测试手段：用一个越界容量（0，小于 ring 的下限 1）构造 `BuffRead`。
/// - 判断：返回 `Err(InvalidCapacity(cap))`，且 `cap` 与传入一致。
#[tokio::test]
async fn invalid_capacity_is_reported_() {
    let err = BuffRead::<_, DefaultAllocConfig>::try_new(
        tokio::io::empty(),
        0,
        DefaultAllocConfig,
    )
    .expect_err("容量 0 会被 ring 拒绝");
    assert_eq!(err, InvalidCapacity(0));
}

/// 每次最多喂 `MAX` 字节的短读设备：用于钉住「设备短读」这条路径。
struct ShortReadDevice {
    data_: Vec<u8>,
    off_: usize,
    /// 单次 `poll_read` 最多给出的字节数（刻意取远小于环容量）。
    max_: usize,
}

impl ShortReadDevice {
    fn new(data: Vec<u8>, max: usize) -> Self {
        ShortReadDevice {
            data_: data,
            off_: 0,
            max_: max,
        }
    }
}

impl AsyncRead for ShortReadDevice {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        if me.off_ >= me.data_.len() {
            // 读到 0：tokio 的 EOF 约定（适配器会把它翻成 `Closing` 标签）。
            return Poll::Ready(Ok(()));
        }
        let n = (me.data_.len() - me.off_).min(me.max_).min(buf.remaining());
        let (start, end) = (me.off_, me.off_ + n);
        buf.put_slice(&me.data_[start..end]);
        me.off_ = end;
        Poll::Ready(Ok(()))
    }
}

/// 设备短读：每段都可能不满，EOF 仍要被正确合成为 `Closing`，且不丢字节。
/// - 测试目标：`BuffRead` 在设备「一次只给几个字节」时能持续搬入并在耗尽后报
///   `Closing`——这正是上游 `move_items_from_input_async` 过去死循环的触发条件
///   （设备返回 0 且无错误）。
/// - 测试手段：用 `ShortReadDevice`（每次最多 7 字节）喂 300 字节，环容量 64；循环
///   `read_async` 直到拿到 `Closing`。
/// - 判断：搬出的 300 字节与负载逐字节一致；终止原因是 `Closing`；`take_error()`
///   为空（正常 EOF 不算设备错误）。
#[tokio::test]
async fn buff_read_survives_short_reads_() {
    const TOTAL: usize = 300;
    let payload = payload_(TOTAL);
    let dev = ShortReadDevice::new(payload.clone(), 7);
    let mut rx = BuffRead::<_, DefaultAllocConfig>::try_new(
        dev,
        64,
        DefaultAllocConfig,
    )
    .expect("容量合法");

    let mut got = Vec::new();
    let mut staging: Vec<MaybeUninit<u8>> = vec![MaybeUninit::uninit(); 64];
    let closing = loop {
        let demand = Demand::at_least(1);
        let some = rx.read_async(&demand).await;
        let mut segm = match some.pick_left() {
            Option::Some(segm) => segm,
            Option::None => break true,
        };
        let now = segm.least_count().min(staging.len());
        // SAFETY: `T = u8` 无析构资源；`staging` 与环段无别名。
        let moved = unsafe { segm.move_items_to_buff(&mut staging[..now]) };
        // SAFETY: 前 `moved` 个元素已初始化。
        let bytes =
            unsafe { core::slice::from_raw_parts(staging.as_ptr().cast::<u8>(), moved) };
        got.extend_from_slice(bytes);
        drop(segm);
    };

    assert!(closing, "设备耗尽后应报 Closing");
    assert_eq!(got, payload, "短读路径不应丢字节或乱序");
    assert!(rx.take_error().is_none(), "正常 EOF 不应记成设备错误");
}
