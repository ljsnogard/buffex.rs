//! **对照组**：不使用任何 buffex 相关 crate，直接拿 `tokio` / `compio` 的
//! `AsyncRead` / `AsyncWrite` 收发。
//!
//! 目的：给「`BuffRead` / `BuffWrite` + ring」这条路径一个同口径的基线。因此本模块
//!
//! * **不 import 任何 `buffex*` crate**，也没有环形缓冲、没有后台泵、没有段（segment）
//!   抽象——只有 socket 上的 `read` / `write`；
//! * 协议、校验和、计时窗口、结果结构（[`PeerResult`]）与 `crate::bench` 完全一致，
//!   于是 `bench_harness` 能拿两组数据直接对比；
//! * 由 [`RawRead`] / [`RawWrite`] 两个小 trait 抹平 tokio 与 compio 的 API 差异，
//!   四个 benchmark 主体仍然只写一份。
//!
//! # 拥有型缓冲：`write_all_owned`
//!
//! compio 的设备接口把 buffer **按值**吞进去再还回来（`AsyncWriteExt::write_all<T: IoBuf>`
//! / `AsyncRead::read<B: IoBufMut>`），且只接受 `'static` 的拥有型缓冲。为了不引入
//! 「每写一次多一次 memcpy」这种对照组自身的负担，接口统一成
//! **把 `Vec<u8>` 交出去、再拿回来**：tokio 侧直接借用写（缓冲原样返回），compio 侧
//! 把同一个 `Vec` 送进驱动再取回，两边在稳态都不额外拷贝。

use std::{future::Future, time::Instant};

use crate::{
    checksum::RollingSum64,
    cli::PeerOptions,
    metrics::{mib_per_sec, percentile_ns, PeerResult},
    proto::{HEADER_LEN, decode_header, encode_header, pattern_buf},
};

/// 直连只读设备：读一次 + 暴露最近一次读到的字节。
pub trait RawRead {
    /// 读一次，最多 `want` 字节；`Ok(0)` 表示对端已关闭（EOF）。
    fn read_some_async(&mut self, want: usize) -> impl Future<Output = Result<usize, String>>;

    /// 最近一次 [`RawRead::read_some_async`] 读到的前 `n` 字节。
    fn bytes(&self, n: usize) -> &[u8];
}

/// 直连只写设备：整块写 + flush + 关闭写方向。
pub trait RawWrite {
    /// 把 `buf` 整块写出去，并把缓冲还给调用方（拥有型，避免额外拷贝）。
    fn write_all_owned(&mut self, buf: Vec<u8>)
    -> impl Future<Output = Result<Vec<u8>, String>>;

    /// 冲刷设备（TCP / UDS 下等价于「数据已交给内核」）。
    fn flush_dev(&mut self) -> impl Future<Output = Result<(), String>>;

    /// 关闭写方向，让对端看到 EOF。
    fn shutdown_dev(&mut self) -> impl Future<Output = Result<(), String>>;
}

/// tokio 直连只读设备。
#[derive(Debug)]
pub struct TokioRead<R> {
    dev_: R,
    /// 复用读缓冲：长度即「本次请求窗口」，稳态一次分配。
    buf_: Vec<u8>,
}

impl<R> TokioRead<R> {
    /// 包一个 tokio 读设备。
    pub fn new(dev: R) -> Self {
        TokioRead {
            dev_: dev,
            buf_: Vec::new(),
        }
    }
}

impl<R> RawRead for TokioRead<R>
where
    R: tokio::io::AsyncRead + Unpin,
{
    async fn read_some_async(&mut self, want: usize) -> Result<usize, String> {
        let want = want.max(1);
        if self.buf_.len() != want {
            self.buf_ = vec![0u8; want];
        }
        tokio::io::AsyncReadExt::read(&mut self.dev_, &mut self.buf_)
            .await
            .map_err(|e| format!("直连读失败：{e}"))
    }

    fn bytes(&self, n: usize) -> &[u8] {
        &self.buf_[..n.min(self.buf_.len())]
    }
}

/// tokio 直连只写设备。
#[derive(Debug)]
pub struct TokioWrite<W> {
    dev_: W,
}

impl<W> TokioWrite<W> {
    /// 包一个 tokio 写设备。
    pub fn new(dev: W) -> Self {
        TokioWrite { dev_: dev }
    }
}

impl<W> RawWrite for TokioWrite<W>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    async fn write_all_owned(&mut self, buf: Vec<u8>) -> Result<Vec<u8>, String> {
        tokio::io::AsyncWriteExt::write_all(&mut self.dev_, &buf)
            .await
            .map_err(|e| format!("直连写失败：{e}"))?;
        Ok(buf)
    }

    async fn flush_dev(&mut self) -> Result<(), String> {
        tokio::io::AsyncWriteExt::flush(&mut self.dev_)
            .await
            .map_err(|e| format!("直连 flush 失败：{e}"))
    }

    async fn shutdown_dev(&mut self) -> Result<(), String> {
        tokio::io::AsyncWriteExt::shutdown(&mut self.dev_)
            .await
            .map_err(|e| format!("直连 shutdown 失败：{e}"))
    }
}

/// compio 直连只读设备。
///
/// `Vec<u8>` 的 `IoBufMut::as_uninit()` 长度等于 **capacity**，驱动读完会把
/// `len` 设成实际读到的字节数，因此这里用「capacity 恰好等于本次请求窗口」的
/// `Vec` 做复用：既不用每次 `resize` 抹零，也不额外拷贝。
#[derive(Debug)]
pub struct CompioRead<R> {
    dev_: R,
    buf_: Vec<u8>,
}

impl<R> CompioRead<R> {
    /// 包一个 compio 读设备。
    pub fn new(dev: R) -> Self {
        CompioRead {
            dev_: dev,
            buf_: Vec::new(),
        }
    }
}

impl<R> RawRead for CompioRead<R>
where
    R: compio::io::AsyncRead + 'static,
{
    async fn read_some_async(&mut self, want: usize) -> Result<usize, String> {
        let want = want.max(1);
        if self.buf_.capacity() != want {
            self.buf_ = Vec::with_capacity(want);
        }
        let buf = core::mem::take(&mut self.buf_);
        let compio::buf::BufResult(res, buf) =
            compio::io::AsyncRead::read(&mut self.dev_, buf).await;
        self.buf_ = buf;
        res.map_err(|e| format!("直连读失败：{e}"))
    }

    fn bytes(&self, n: usize) -> &[u8] {
        &self.buf_[..n.min(self.buf_.len())]
    }
}

/// compio 直连只写设备。
#[derive(Debug)]
pub struct CompioWrite<W> {
    dev_: W,
}

impl<W> CompioWrite<W> {
    /// 包一个 compio 写设备。
    pub fn new(dev: W) -> Self {
        CompioWrite { dev_: dev }
    }
}

impl<W> RawWrite for CompioWrite<W>
where
    W: compio::io::AsyncWrite + 'static,
{
    async fn write_all_owned(&mut self, buf: Vec<u8>) -> Result<Vec<u8>, String> {
        let compio::buf::BufResult(res, buf) =
            compio::io::AsyncWriteExt::write_all(&mut self.dev_, buf).await;
        res.map_err(|e| format!("直连写失败：{e}"))?;
        Ok(buf)
    }

    async fn flush_dev(&mut self) -> Result<(), String> {
        // `flush` / `shutdown` 是 compio `AsyncWrite` 自身的必需方法
        // （`AsyncWriteExt` 只提供 `write_all` 一类的组合子）。
        compio::io::AsyncWrite::flush(&mut self.dev_)
            .await
            .map_err(|e| format!("直连 flush 失败：{e}"))
    }

    async fn shutdown_dev(&mut self) -> Result<(), String> {
        compio::io::AsyncWrite::shutdown(&mut self.dev_)
            .await
            .map_err(|e| format!("直连 shutdown 失败：{e}"))
    }
}

/// 直连版单向吞吐发送端（口径与 [`crate::bench::bench_send`] 一致）。
///
/// # Errors
///
/// 设备读写失败或对端提前关闭时返回错误说明。
pub async fn bench_send<T>(
    tx: &mut T,
    opts: &PeerOptions,
    role: &str,
    transport: &str,
) -> Result<PeerResult, String>
where
    T: RawWrite,
{
    let header = encode_header(opts.bytes);
    let mut payload = pattern_buf(opts.chunk);
    let mut sum = RollingSum64::new();
    let started = Instant::now();

    let _ = tx.write_all_owned(header.to_vec()).await?;
    let mut sent = 0u64;
    while sent < opts.bytes {
        let remain = opts.bytes - sent;
        let n = remain.min(opts.chunk as u64) as usize;
        if n == payload.len() {
            // 稳态：把同一个缓冲交出去再拿回来，不额外拷贝。
            let buf = core::mem::take(&mut payload);
            payload = tx.write_all_owned(buf).await?;
            sum.update(&payload[..n]);
        } else {
            // 只可能出现在最后一次不足 `chunk` 的收尾。
            let tail = tx.write_all_owned(payload[..n].to_vec()).await?;
            sum.update(&tail[..n]);
        }
        sent += n as u64;
    }
    tx.flush_dev().await?;
    tx.shutdown_dev().await?;
    let elapsed = started.elapsed();

    Ok(PeerResult {
        role: role.to_string(),
        backend: opts.backend.name().to_string(),
        transport: transport.to_string(),
        mode: crate::cli::Mode::Send.name().to_string(),
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

/// 直连版单向吞吐接收端：读 8 字节头部，再收满 `total` 字节。
///
/// # Errors
///
/// 头部之前对端就关闭、或负载没读满就 EOF 时返回错误说明。
pub async fn bench_recv<T>(
    rx: &mut T,
    opts: &PeerOptions,
    role: &str,
    transport: &str,
) -> Result<PeerResult, String>
where
    T: RawRead,
{
    let mut head = [0u8; HEADER_LEN];
    let mut got_head = 0usize;
    while got_head < HEADER_LEN {
        let n = rx.read_some_async(HEADER_LEN - got_head).await?;
        if n == 0 {
            return Err("对端在总长头部之前就关闭了".to_string());
        }
        head[got_head..got_head + n].copy_from_slice(rx.bytes(n));
        got_head += n;
    }
    let total = decode_header(&head);

    let mut sum = RollingSum64::new();
    let started = Instant::now();
    let mut got = 0u64;
    while got < total {
        let remain = total - got;
        let n = rx.read_some_async(remain.min(opts.chunk as u64) as usize).await?;
        if n == 0 {
            return Err(format!("对端提前关闭：期望 {total} 字节，实际 {got} 字节"));
        }
        sum.update(rx.bytes(n));
        got += n as u64;
    }
    let elapsed = started.elapsed();

    Ok(PeerResult {
        role: role.to_string(),
        backend: opts.backend.name().to_string(),
        transport: transport.to_string(),
        mode: crate::cli::Mode::Recv.name().to_string(),
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

/// 直连版 RTT 客户端：发一条、收一条回显，逐轮计时。
///
/// # Errors
///
/// 回显通道提前关闭、回显内容与请求不一致、设备读写失败时返回错误。
pub async fn bench_rtt_client<TX, RX>(
    tx: &mut TX,
    rx: &mut RX,
    opts: &PeerOptions,
    role: &str,
    transport: &str,
) -> Result<PeerResult, String>
where
    TX: RawWrite,
    RX: RawRead,
{
    let mut payload = pattern_buf(opts.chunk);
    let mut sum = RollingSum64::new();
    let mut latencies: Vec<u64> = Vec::with_capacity(opts.iters as usize);

    let started = Instant::now();
    for round in 0..opts.iters {
        let t0 = Instant::now();
        let buf = core::mem::take(&mut payload);
        payload = tx.write_all_owned(buf).await?;
        tx.flush_dev().await?;

        let mut got = 0u64;
        while got < opts.chunk as u64 {
            let want = opts.chunk - got as usize;
            let n = rx.read_some_async(want).await?;
            if n == 0 {
                return Err(format!("第 {round} 轮：回显通道提前关闭"));
            }
            // 就地核对回显内容：与请求逐字节一致才算这一轮有效（release 下也要查）。
            let start = got as usize;
            if rx.bytes(n) != &payload[start..start + n] {
                return Err(format!("第 {round} 轮：回显内容与请求不一致"));
            }
            got += n as u64;
        }
        latencies.push(t0.elapsed().as_nanos() as u64);
        sum.update(&payload);
    }
    tx.shutdown_dev().await?;
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
        mode: crate::cli::Mode::RttClient.name().to_string(),
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

/// 直连版 RTT 服务端：收一条、回显一条，直到对端关闭。
///
/// # Errors
///
/// 消息中途断开或设备读写失败时返回错误。
pub async fn bench_rtt_server<RX, TX>(
    rx: &mut RX,
    tx: &mut TX,
    opts: &PeerOptions,
    role: &str,
    transport: &str,
) -> Result<PeerResult, String>
where
    RX: RawRead,
    TX: RawWrite,
{
    let mut msg: Vec<u8> = Vec::with_capacity(opts.chunk);
    let mut sum = RollingSum64::new();
    let started = Instant::now();
    let mut served = 0u64;

    'outer: loop {
        msg.clear();
        while msg.len() < opts.chunk {
            let want = opts.chunk - msg.len();
            let n = rx.read_some_async(want).await?;
            if n == 0 {
                if msg.is_empty() {
                    break 'outer; // 消息边界上的干净 EOF
                }
                return Err(format!(
                    "消息中途断开：期望 {} 字节，实际 {} 字节",
                    opts.chunk,
                    msg.len()
                ));
            }
            msg.extend_from_slice(rx.bytes(n));
        }
        sum.update(&msg);
        served += msg.len() as u64;
        msg = tx.write_all_owned(msg).await?;
        tx.flush_dev().await?;
    }
    tx.shutdown_dev().await?;
    let elapsed = started.elapsed();

    Ok(PeerResult {
        role: role.to_string(),
        backend: opts.backend.name().to_string(),
        transport: transport.to_string(),
        mode: crate::cli::Mode::RttServer.name().to_string(),
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
