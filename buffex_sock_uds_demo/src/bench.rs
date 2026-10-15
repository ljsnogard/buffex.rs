//! 与运行时、与适配器都无关的 benchmark 主体。
//!
//! 这里只依赖 `abs_buff` 的 `TrBuffRead` / `TrBuffWrite` 与实验自带的
//! [`FinishWrite`] 收尾能力，因此 tokio 对端与 compio 对端共用同一份实现：
//! 两个对端的差别仅在「怎么建连接、怎么构造适配器」。
//!
//! ## 计时口径
//!
//! * **发送端**：从「写 8 字节总长头部之前」到「`close` 返回」；计的是负载
//!   字节数（不含头部）。
//! * **接收端**：从「读完头部」到「收满负载」。
//! * **RTT**：每轮从「开始写请求」到「读完回显」，p50 / p99 由每轮耗时统计。

use std::{future::Future, time::Instant};

use abs_buff::{
    TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrBuffSegmRef},
};
use buffex::{
    ring::{ConsumerError, ProducerError},
    x_deps::abs_buff,
};

use crate::{
    checksum::RollingSum64,
    cli::{Mode, PeerOptions},
    metrics::{mib_per_sec, percentile_ns, PeerResult},
    proto::{encode_header, pattern_buf},
    pump::{self, Staging},
};

/// 出向适配器的收尾能力。
///
/// `TrBuffWrite` 只保证「字节提交进环」，**不保证已经写进设备**：收尾必须由
/// 适配器自己提供。两个适配器的收尾入口同名但类型不同（tokio 是
/// `flush_async` / `close_async`，compio 是 `flush_async` / `close_async`），
/// 用这个实验内部的小 trait 把差异挡在 benchmark 主体之外。
pub trait FinishWrite {
    /// 把环内已提交的数据全部交给设备。
    fn flush_ring(&mut self) -> impl Future<Output = Result<(), String>>;

    /// 冲刷环内数据并关闭写方向（对端据此看到 EOF）。
    fn close_ring(&mut self) -> impl Future<Output = Result<(), String>>;
}

/// 把 [`pump::PumpError`] 变成可读的错误串。
fn err_(e: pump::PumpError) -> String {
    format!("搬运失败：{e}")
}

/// 单向吞吐的发送端。
///
/// 先写 8 字节总长头部（不计时也不计吞吐），再按 `chunk` 粒度把
/// `opts.bytes` 个负载字节写进写端，最后 `flush` + `close` 收尾。
///
/// # Errors
///
/// 环关闭 / 取消失败或适配器收尾失败时返回错误说明。
pub async fn bench_send<T>(
    tx: &mut T,
    opts: &PeerOptions,
    role: &str,
    transport: &str,
) -> Result<PeerResult, String>
where
    T: TrBuffWrite<u8>
        + TrBuffTryWrite<u8, Err = ProducerError<usize>>
        + FinishWrite
        + Unpin,
    for<'s> T::SegmMut<'s>: TrBuffSegmMut<'s, u8>,
{
    let payload = pattern_buf(opts.chunk);
    let header = encode_header(opts.bytes);
    let mut sum = RollingSum64::new();
    let started = Instant::now();

    pump::write_all(tx, &header).await.map_err(err_)?;
    let mut sent = 0u64;
    while sent < opts.bytes {
        let remain = opts.bytes - sent;
        let n = remain.min(opts.chunk as u64) as usize;
        pump::write_all(tx, &payload[..n]).await.map_err(err_)?;
        sum.update(&payload[..n]);
        sent += n as u64;
    }
    tx.flush_ring().await?;
    tx.close_ring().await?;
    let elapsed = started.elapsed();

    Ok(PeerResult {
        role: role.to_string(),
        backend: opts.backend.name().to_string(),
        transport: transport.to_string(),
        mode: Mode::Send.name().to_string(),
        bytes: opts.bytes,
        chunk: opts.chunk,
        iters: 0,
        ring_cap: opts.ring_cap,
        elapsed_ns: elapsed.as_nanos() as u64,
        mib_per_s: mib_per_sec(opts.bytes, elapsed),
        checksum: sum.finish(),
        received: sent,
        ok: true,
        p50_ns: 0,
        p99_ns: 0,
        avg_ns: 0,
        note: String::new(),
    })
}

/// 单向吞吐的接收端：读头部、再收满 `total` 字节。
///
/// # Errors
///
/// 对端提前关闭（丢字节）、环状态异常时返回错误说明。
pub async fn bench_recv<T>(
    rx: &mut T,
    opts: &PeerOptions,
    role: &str,
    transport: &str,
) -> Result<PeerResult, String>
where
    T: TrBuffRead<u8>
        + TrBuffTryRead<u8, Err = ConsumerError<usize>>
        + Unpin,
    for<'s> T::SegmRef<'s>: TrBuffSegmRef<'s, u8>,
{
    let mut staging = Staging::with_capacity(opts.chunk);
    let mut sum = RollingSum64::new();

    let total = pump::read_header(rx, &mut staging).await.map_err(err_)?;
    let started = Instant::now();

    let mut got = 0u64;
    while got < total {
        let remain = total - got;
        let n = remain.min(opts.chunk as u64);
        let m = pump::read_exact_fold(rx, &mut staging, n, |b| sum.update(b))
            .await
            .map_err(err_)?;
        got += m;
    }
    let elapsed = started.elapsed();

    Ok(PeerResult {
        role: role.to_string(),
        backend: opts.backend.name().to_string(),
        transport: transport.to_string(),
        mode: Mode::Recv.name().to_string(),
        bytes: total,
        chunk: opts.chunk,
        iters: 0,
        ring_cap: opts.ring_cap,
        elapsed_ns: elapsed.as_nanos() as u64,
        mib_per_s: mib_per_sec(total, elapsed),
        checksum: sum.finish(),
        received: got,
        ok: true,
        p50_ns: 0,
        p99_ns: 0,
        avg_ns: 0,
        note: String::new(),
    })
}

/// RTT 模式的客户端：发一条、读一条回显，逐轮计时。
///
/// # 前置约束：`ring_cap == chunk`
///
/// 两个适配器的 `read_async` 都只在「**一个可写环段被填满**」或「设备结束 /
/// 出错」时才把数据交给调用方（实验实测，见 `tests/read_semantics_.rs`）。
/// 乒乓协议里对端发完一条就等回显，不会再补数据，因此环段必须能被这一条消息
/// 恰好填满：`ring_cap` 必须等于 `chunk`（见 `cli::parse_peer_args` 的校验）。
/// 否则两端都会卡在「等环段填满」上，表现为 RTT 永久挂起。
///
/// # Errors
///
/// 回显通道提前关闭、回显内容与请求不一致、环状态异常或收尾失败时返回错误。
pub async fn bench_rtt_client<TX, RX>(
    tx: &mut TX,
    rx: &mut RX,
    opts: &PeerOptions,
    role: &str,
    transport: &str,
) -> Result<PeerResult, String>
where
    TX: TrBuffWrite<u8>
        + TrBuffTryWrite<u8, Err = ProducerError<usize>>
        + FinishWrite
        + Unpin,
    for<'s> TX::SegmMut<'s>: TrBuffSegmMut<'s, u8>,
    RX: TrBuffRead<u8>
        + TrBuffTryRead<u8, Err = ConsumerError<usize>>
        + Unpin,
    for<'s> RX::SegmRef<'s>: TrBuffSegmRef<'s, u8>,
{
    let payload = pattern_buf(opts.chunk);
    let mut staging = Staging::with_capacity(opts.chunk);
    let mut echo: Vec<u8> = Vec::with_capacity(opts.chunk);
    let mut sum = RollingSum64::new();
    let mut latencies: Vec<u64> = Vec::with_capacity(opts.iters as usize);

    let started = Instant::now();
    for round in 0..opts.iters {
        let t0 = Instant::now();
        pump::write_all(tx, &payload).await.map_err(err_)?;
        tx.flush_ring().await?;
        echo.clear();
        let got = pump::read_exact_into(rx, &mut staging, opts.chunk as u64, &mut echo)
            .await
            .map_err(err_)?;
        if got == 0 {
            return Err(format!("第 {round} 轮：回显通道提前关闭"));
        }
        if echo != payload {
            return Err(format!("第 {round} 轮：回显内容与请求不一致"));
        }
        latencies.push(t0.elapsed().as_nanos() as u64);
        sum.update(&payload);
    }
    tx.close_ring().await?;
    let elapsed = started.elapsed();

    let bytes = opts.chunk as u64 * opts.iters;
    let avg = if latencies.is_empty() {
        0
    } else {
        latencies.iter().sum::<u64>() / latencies.len() as u64
    };
    let p50 = percentile_ns(latencies.clone(), 0.5);
    let p99 = percentile_ns(latencies, 0.99);

    Ok(PeerResult {
        role: role.to_string(),
        backend: opts.backend.name().to_string(),
        transport: transport.to_string(),
        mode: Mode::RttClient.name().to_string(),
        bytes,
        chunk: opts.chunk,
        iters: opts.iters,
        ring_cap: opts.ring_cap,
        elapsed_ns: elapsed.as_nanos() as u64,
        mib_per_s: mib_per_sec(bytes, elapsed),
        checksum: sum.finish(),
        received: bytes,
        ok: true,
        p50_ns: p50,
        p99_ns: p99,
        avg_ns: avg,
        note: String::new(),
    })
}

/// RTT 模式的服务端：收一条、回显一条，直到对端关闭。
///
/// 与 [`bench_rtt_client`] 相同：`ring_cap == chunk` 是本模式的硬约束。
///
/// # Errors
///
/// 消息中途断开、环状态异常或收尾失败时返回错误。
pub async fn bench_rtt_server<RX, TX>(
    rx: &mut RX,
    tx: &mut TX,
    opts: &PeerOptions,
    role: &str,
    transport: &str,
) -> Result<PeerResult, String>
where
    RX: TrBuffRead<u8>
        + TrBuffTryRead<u8, Err = ConsumerError<usize>>
        + Unpin,
    for<'s> RX::SegmRef<'s>: TrBuffSegmRef<'s, u8>,
    TX: TrBuffWrite<u8>
        + TrBuffTryWrite<u8, Err = ProducerError<usize>>
        + FinishWrite
        + Unpin,
    for<'s> TX::SegmMut<'s>: TrBuffSegmMut<'s, u8>,
{
    let mut staging = Staging::with_capacity(opts.chunk);
    let mut msg: Vec<u8> = Vec::with_capacity(opts.chunk);
    let mut sum = RollingSum64::new();

    let started = Instant::now();
    let mut served = 0u64;
    loop {
        msg.clear();
        let got = pump::read_exact_into(rx, &mut staging, opts.chunk as u64, &mut msg)
            .await
            .map_err(err_)?;
        if got == 0 {
            break; // 对端收尾：消息边界上的干净 EOF
        }
        pump::write_all(tx, &msg).await.map_err(err_)?;
        tx.flush_ring().await?;
        sum.update(&msg);
        served += got;
    }
    tx.close_ring().await?;
    let elapsed = started.elapsed();

    Ok(PeerResult {
        role: role.to_string(),
        backend: opts.backend.name().to_string(),
        transport: transport.to_string(),
        mode: Mode::RttServer.name().to_string(),
        bytes: served,
        chunk: opts.chunk,
        iters: if opts.chunk == 0 {
            0
        } else {
            served / opts.chunk as u64
        },
        ring_cap: opts.ring_cap,
        elapsed_ns: elapsed.as_nanos() as u64,
        mib_per_s: mib_per_sec(served, elapsed),
        checksum: sum.finish(),
        received: served,
        ok: true,
        p50_ns: 0,
        p99_ns: 0,
        avg_ns: 0,
        note: String::new(),
    })
}
