//! compio 侧对端进程。
//!
//! 与 `peer_tokio` 对称：建连接、用 [`buffex_compio_adapt`] 的 `BuffRead` /
//! `BuffWrite` 把设备接上环、跑同一个 benchmark 主体。差别只在两处：
//!
//! * 适配器的构造**必须在运行中的 compio runtime 内**（它要 spawn 后台泵），
//!   因此 `main` 用 `#[compio::main]`；
//! * 收尾入口虽然同名（`flush_async` / `close_async`），但类型是 compio 侧
//!   自己的 future（等后台泵把环取空）。

use std::{
    io::Write as _,
    net::SocketAddr,
    path::Path,
};

use compio::net::{TcpListener, TcpStream, UnixListener, UnixStream};

use buffex_compio_adapt::{BuffRead, BuffWrite, DefaultAllocConfig};
use buffex_sock_uds_demo::{
    bench::{bench_recv, bench_rtt_client, bench_rtt_server, bench_send},
    cli::{Backend, Endpoint, Mode, PEER_HELP, PeerOptions, Transport, parse_peer_args},
    metrics::PeerResult,
    raw::{self, CompioRead, CompioWrite},
};

/// 报告里本端的身份。
const ROLE: &str = "compio";

/// 把 `{"event":"ready","addr":...}` 打到 stdout 并立刻 flush，供编排器握手。
fn ready_(addr: &str) -> Result<(), String> {
    println!("{{\"event\":\"ready\",\"addr\":\"{}\"}}", addr);
    std::io::stdout()
        .flush()
        .map_err(|e| format!("flush stdout 失败：{e}"))
}

/// 建连接并跑 benchmark。
async fn run_(opts: &PeerOptions) -> Result<PeerResult, String> {
    match opts.transport {
        Transport::Tcp => run_tcp_(opts).await,
        Transport::Uds => run_uds_(opts).await,
    }
}

/// 解析 TCP 端点：compio 的 `ToSocketAddrsAsync` 对 `&str` 没有 blanket 实现，
/// 而本实验的端点一律是字面 `ip:port`，直接解析成 `SocketAddr` 最省事。
fn parse_addr_(s: &str) -> Result<SocketAddr, String> {
    s.parse::<SocketAddr>()
        .map_err(|e| format!("`{s}` 不是合法的 ip:port：{e}"))
}

/// TCP：`--listen` 侧绑定并 accept，`--connect` 侧主动连接。
async fn run_tcp_(opts: &PeerOptions) -> Result<PeerResult, String> {
    match &opts.endpoint {
        Endpoint::Listen(addr) => {
            let bind = parse_addr_(addr)?;
            let listener = TcpListener::bind(bind)
                .await
                .map_err(|e| format!("绑定 {addr} 失败：{e}"))?;
            let local = listener
                .local_addr()
                .map_err(|e| format!("取本地地址失败：{e}"))?;
            ready_(&local.to_string())?;
            let (stream, peer) = listener
                .accept()
                .await
                .map_err(|e| format!("accept 失败：{e}"))?;
            if opts.nodelay {
                stream
                    .set_nodelay(true)
                    .map_err(|e| format!("设置 TCP_NODELAY 失败：{e}"))?;
            }
            eprintln!("[peer_compio] 已接受来自 {peer} 的连接，本地 {local}");
            let (r, w) = stream.into_split();
            run_split_(r, w, opts, Transport::Tcp).await
        }
        Endpoint::Connect(addr) => {
            let target = parse_addr_(addr)?;
            let stream = TcpStream::connect(target)
                .await
                .map_err(|e| format!("连接 {addr} 失败：{e}"))?;
            if opts.nodelay {
                stream
                    .set_nodelay(true)
                    .map_err(|e| format!("设置 TCP_NODELAY 失败：{e}"))?;
            }
            eprintln!("[peer_compio] 已连接到 {addr}");
            let (r, w) = stream.into_split();
            run_split_(r, w, opts, Transport::Tcp).await
        }
    }
}

/// UDS：`--listen` 侧先清掉陈旧 socket 文件再绑定。
async fn run_uds_(opts: &PeerOptions) -> Result<PeerResult, String> {
    match &opts.endpoint {
        Endpoint::Listen(path) => {
            let p = Path::new(path);
            if p.exists() {
                std::fs::remove_file(p)
                    .map_err(|e| format!("清理旧 socket `{path}` 失败：{e}"))?;
            }
            let listener = UnixListener::bind(p)
                .await
                .map_err(|e| format!("绑定 {path} 失败：{e}"))?;
            ready_(path)?;
            let (stream, _peer) = listener
                .accept()
                .await
                .map_err(|e| format!("accept 失败：{e}"))?;
            eprintln!("[peer_compio] 已接受 UDS 连接：{path}");
            let (r, w) = stream.into_split();
            run_split_(r, w, opts, Transport::Uds).await
        }
        Endpoint::Connect(path) => {
            let stream = UnixStream::connect(path)
                .await
                .map_err(|e| format!("连接 {path} 失败：{e}"))?;
            eprintln!("[peer_compio] 已连接到 UDS {path}");
            let (r, w) = stream.into_split();
            run_split_(r, w, opts, Transport::Uds).await
        }
    }
}

/// 按角色构造适配器、跑对应的 benchmark 主体。
///
/// 注意 compio 侧 `try_new` 会 spawn 后台泵，且返回
/// `TryNewError`（容量非法 / 不在 runtime 内）。`--backend raw` 时改走 [`raw`]
/// （对照组：完全不用 buffex，直接读写 socket）。
async fn run_split_<R, W>(
    r: R,
    w: W,
    opts: &PeerOptions,
    transport: Transport,
) -> Result<PeerResult, String>
where
    R: compio::io::AsyncRead + 'static,
    W: compio::io::AsyncWrite + 'static,
{
    if opts.backend == Backend::Raw {
        return run_raw_(r, w, opts, transport).await;
    }
    let tname = transport.name();
    match opts.mode {
        Mode::Send => {
            let mut tx = BuffWrite::<_, DefaultAllocConfig>::try_new(
                w,
                opts.ring_cap,
                DefaultAllocConfig,
            )
            .map_err(|e| format!("构造 BuffWrite 失败：{e:?}"))?;
            bench_send(&mut tx, opts, ROLE, tname).await
        }
        Mode::Recv => {
            let mut rx = BuffRead::<_, DefaultAllocConfig>::try_new(
                r,
                opts.ring_cap,
                DefaultAllocConfig,
            )
            .map_err(|e| format!("构造 BuffRead 失败：{e:?}"))?;
            bench_recv(&mut rx, opts, ROLE, tname).await
        }
        Mode::RttClient => {
            let mut tx = BuffWrite::<_, DefaultAllocConfig>::try_new(
                w,
                opts.ring_cap,
                DefaultAllocConfig,
            )
            .map_err(|e| format!("构造 BuffWrite 失败：{e:?}"))?;
            let mut rx = BuffRead::<_, DefaultAllocConfig>::try_new(
                r,
                opts.ring_cap,
                DefaultAllocConfig,
            )
            .map_err(|e| format!("构造 BuffRead 失败：{e:?}"))?;
            bench_rtt_client(&mut tx, &mut rx, opts, ROLE, tname).await
        }
        Mode::RttServer => {
            let mut rx = BuffRead::<_, DefaultAllocConfig>::try_new(
                r,
                opts.ring_cap,
                DefaultAllocConfig,
            )
            .map_err(|e| format!("构造 BuffRead 失败：{e:?}"))?;
            let mut tx = BuffWrite::<_, DefaultAllocConfig>::try_new(
                w,
                opts.ring_cap,
                DefaultAllocConfig,
            )
            .map_err(|e| format!("构造 BuffWrite 失败：{e:?}"))?;
            bench_rtt_server(&mut rx, &mut tx, opts, ROLE, tname).await
        }
    }
}

/// 直连对照组：只用 compio 的 `AsyncRead` / `AsyncWrite`，不经任何 buffex crate。
async fn run_raw_<R, W>(
    r: R,
    w: W,
    opts: &PeerOptions,
    transport: Transport,
) -> Result<PeerResult, String>
where
    R: compio::io::AsyncRead + 'static,
    W: compio::io::AsyncWrite + 'static,
{
    let tname = transport.name();
    match opts.mode {
        Mode::Send => {
            let mut tx = CompioWrite::new(w);
            raw::bench_send(&mut tx, opts, ROLE, tname).await
        }
        Mode::Recv => {
            let mut rx = CompioRead::new(r);
            raw::bench_recv(&mut rx, opts, ROLE, tname).await
        }
        Mode::RttClient => {
            let mut tx = CompioWrite::new(w);
            let mut rx = CompioRead::new(r);
            raw::bench_rtt_client(&mut tx, &mut rx, opts, ROLE, tname).await
        }
        Mode::RttServer => {
            let mut rx = CompioRead::new(r);
            let mut tx = CompioWrite::new(w);
            raw::bench_rtt_server(&mut rx, &mut tx, opts, ROLE, tname).await
        }
    }
}

#[compio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let opts = match parse_peer_args(&args) {
        Ok(o) => o,
        Err(e) if e == "PEER_HELP" => {
            println!("{PEER_HELP}");
            return;
        }
        Err(e) => {
            eprintln!("peer_compio 参数错误：{e}\n\n{PEER_HELP}");
            std::process::exit(2);
        }
    };

    match run_(&opts).await {
        Ok(result) => println!("{}", result.to_json_line()),
        Err(e) => {
            eprintln!("peer_compio 失败：{e}");
            std::process::exit(1);
        }
    }
}
