//! tokio 侧对端进程。
//!
//! 只做三件事：建连接（TCP / UDS 各自监听或连接）、用
//! [`buffex_tokio_adapt`] 的 `BuffRead` / `BuffWrite` 把设备接上环、跑
//! [`buffex_sock_uds_demo::bench`] 里的 benchmark 主体。
//!
//! **业务代码里没有任何设备 IO**：所有 socket 读写都由适配器承担，这里只对
//! 环借段、搬字节。
//!
//! 运行模型说明：tokio 侧的适配器是**按需驱动**（不 spawn 后台泵），因此这里
//! 用常见配置 `multi_thread` + 2 个 worker 即可；实际只有一个任务在跑，多线程
//! 运行时对结果没有影响（见报告里的说明）。

use std::{io::Write as _, path::Path};

use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream, UnixListener, UnixStream},
};

use buffex_tokio_adapt::{BuffRead, BuffWrite, DefaultAllocConfig};
use buffex_sock_uds_demo::{
    bench::{bench_recv, bench_rtt_client, bench_rtt_server, bench_send},
    cli::{Backend, Endpoint, Mode, PEER_HELP, PeerOptions, Transport, parse_peer_args},
    metrics::PeerResult,
    raw::{self, TokioRead, TokioWrite},
};

/// 报告里本端的身份。
const ROLE: &str = "tokio";

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

/// TCP：`--listen` 侧绑定并 accept，`--connect` 侧主动连接。
async fn run_tcp_(opts: &PeerOptions) -> Result<PeerResult, String> {
    match &opts.endpoint {
        Endpoint::Listen(addr) => {
            let listener = TcpListener::bind(addr)
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
            eprintln!("[peer_tokio] 已接受来自 {peer} 的连接，本地 {local}");
            let (r, w) = stream.into_split();
            run_split_(r, w, opts, Transport::Tcp).await
        }
        Endpoint::Connect(addr) => {
            let stream = TcpStream::connect(addr)
                .await
                .map_err(|e| format!("连接 {addr} 失败：{e}"))?;
            if opts.nodelay {
                stream
                    .set_nodelay(true)
                    .map_err(|e| format!("设置 TCP_NODELAY 失败：{e}"))?;
            }
            eprintln!("[peer_tokio] 已连接到 {addr}");
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
                .map_err(|e| format!("绑定 {path} 失败：{e}"))?;
            ready_(path)?;
            let (stream, _peer) = listener
                .accept()
                .await
                .map_err(|e| format!("accept 失败：{e}"))?;
            eprintln!("[peer_tokio] 已接受 UDS 连接：{path}");
            let (r, w) = stream.into_split();
            run_split_(r, w, opts, Transport::Uds).await
        }
        Endpoint::Connect(path) => {
            let stream = UnixStream::connect(path)
                .await
                .map_err(|e| format!("连接 {path} 失败：{e}"))?;
            eprintln!("[peer_tokio] 已连接到 UDS {path}");
            let (r, w) = stream.into_split();
            run_split_(r, w, opts, Transport::Uds).await
        }
    }
}

/// 按角色构造适配器、跑对应的 benchmark 主体。
///
/// 适配器只在这里出现：`BuffRead` 接设备的读半部，`BuffWrite` 接写半部。
/// `--backend raw` 时改走 [`raw`]（对照组：完全不用 buffex，直接读写 socket），
/// 协议、计时口径与结果结构都与 buffex 路径一致。
async fn run_split_<R, W>(
    r: R,
    w: W,
    opts: &PeerOptions,
    transport: Transport,
) -> Result<PeerResult, String>
where
    R: AsyncRead + Unpin + 'static,
    W: AsyncWrite + Unpin + 'static,
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

/// 直连对照组：只用 tokio 的 `AsyncRead` / `AsyncWrite`，不经任何 buffex crate。
async fn run_raw_<R, W>(
    r: R,
    w: W,
    opts: &PeerOptions,
    transport: Transport,
) -> Result<PeerResult, String>
where
    R: AsyncRead + Unpin + 'static,
    W: AsyncWrite + Unpin + 'static,
{
    let tname = transport.name();
    match opts.mode {
        Mode::Send => {
            let mut tx = TokioWrite::new(w);
            raw::bench_send(&mut tx, opts, ROLE, tname).await
        }
        Mode::Recv => {
            let mut rx = TokioRead::new(r);
            raw::bench_recv(&mut rx, opts, ROLE, tname).await
        }
        Mode::RttClient => {
            let mut tx = TokioWrite::new(w);
            let mut rx = TokioRead::new(r);
            raw::bench_rtt_client(&mut tx, &mut rx, opts, ROLE, tname).await
        }
        Mode::RttServer => {
            let mut rx = TokioRead::new(r);
            let mut tx = TokioWrite::new(w);
            raw::bench_rtt_server(&mut rx, &mut tx, opts, ROLE, tname).await
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let opts = match parse_peer_args(&args) {
        Ok(o) => o,
        Err(e) if e == "PEER_HELP" => {
            println!("{PEER_HELP}");
            return;
        }
        Err(e) => {
            eprintln!("peer_tokio 参数错误：{e}\n\n{PEER_HELP}");
            std::process::exit(2);
        }
    };

    match run_(&opts).await {
        Ok(result) => println!("{}", result.to_json_line()),
        Err(e) => {
            eprintln!("peer_tokio 失败：{e}");
            std::process::exit(1);
        }
    }
}
