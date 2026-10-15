//! 编排逻辑：拉起两个对端进程、完成握手、收集结果。
//!
//! 编排器（`bench_harness`）与集成测试共用这一层，因此这里**不碰任何业务
//! 字节**：它只负责
//!
//! 1. 决定监听端 / 连接端（监听端总是「被动方」：`recv` 或 `rtt-server`）；
//! 2. 拉起监听端，等它打出 `{"event":"ready","addr":...}` 拿到实际地址
//!    （TCP 端口写 0 时由内核挑，UDS 则回显路径）；
//! 3. 拉起连接端，等两边各自打出 `{"event":"result", ...}`；
//! 4. 超时则杀掉残留进程，保证失败的用例不会挂死整个矩阵。

use std::{
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use crate::{
    cli::{Backend, Endpoint, Mode, PeerOptions, Transport},
    metrics::{json_parse_flat, JsonValue, PeerResult},
};

/// 对端身份（决定跑哪个二进制、报告里怎么称呼）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerRole {
    /// tokio 侧（`peer_tokio`，用 `buffex_tokio_adapt`）。
    Tokio,
    /// compio 侧（`peer_compio`，用 `buffex_compio_adapt`）。
    Compio,
}

impl PeerRole {
    /// 对端名字（也是 `--role` 的报告口径）。
    pub const fn name(self) -> &'static str {
        match self {
            PeerRole::Tokio => "tokio",
            PeerRole::Compio => "compio",
        }
    }

    /// 二进制文件名。
    pub const fn binary_name(self) -> &'static str {
        match self {
            PeerRole::Tokio => "peer_tokio",
            PeerRole::Compio => "peer_compio",
        }
    }

    /// 另一个对端。
    pub const fn peer(self) -> Self {
        match self {
            PeerRole::Tokio => PeerRole::Compio,
            PeerRole::Compio => PeerRole::Tokio,
        }
    }
}

/// 用例类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseKind {
    /// 单向吞吐：发送端 `send`，接收端 `recv`。
    Throughput,
    /// 往返时延：客户端 `rtt-client`，服务端 `rtt-server`。
    Rtt,
}

impl CaseKind {
    /// 报告里的名字。
    pub const fn name(self) -> &'static str {
        match self {
            CaseKind::Throughput => "throughput",
            CaseKind::Rtt => "rtt",
        }
    }
}

/// 一个用例的完整定义。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseSpec {
    /// 用例类型。
    pub kind: CaseKind,
    /// 收发后端（buffex 环 / 直连对照组）。
    pub backend: Backend,
    /// 传输方式。
    pub transport: Transport,
    /// 主动方（吞吐的发送端 / RTT 的客户端）。
    pub client: PeerRole,
    /// 被动方（吞吐的接收端 / RTT 的服务端），也是监听端。
    pub server: PeerRole,
    /// 消息粒度。
    pub chunk: usize,
    /// 吞吐模式的负载总量。
    pub bytes: u64,
    /// RTT 往返次数。
    pub iters: u64,
    /// 适配器内部环容量。
    pub ring_cap: usize,
}

impl CaseSpec {
    /// 报告里的方向标签。
    pub fn direction(&self) -> String {
        match self.kind {
            CaseKind::Throughput => {
                format!("{} → {}", self.client.name(), self.server.name())
            }
            CaseKind::Rtt => {
                format!("{} 客户端", self.client.name())
            }
        }
    }

    /// 报告里的后端标签（buffex 环 / 直连对照组）。
    pub fn backend_label(&self) -> &'static str {
        self.backend.label()
    }
}

/// 一个用例跑完的产出。
#[derive(Debug, Clone)]
pub struct CaseOutcome {
    /// 用例定义。
    pub spec: CaseSpec,
    /// 主动方（发送端 / RTT 客户端）的结果。
    pub client: PeerResult,
    /// 被动方（接收端 / RTT 服务端）的结果。
    pub server: PeerResult,
    /// 编排器观测到的墙钟时间（含进程启动与握手）。
    pub wall: Duration,
}

impl CaseOutcome {
    /// 收发两侧校验和是否对得上（RTT 模式下两侧都只回显/校验，也要求一致）。
    pub fn checksum_agrees(&self) -> bool {
        self.client.checksum == self.server.checksum
    }

    /// 两侧是否都正常收尾、且字节数符合预期。
    pub fn ok(&self) -> bool {
        if !(self.client.ok && self.server.ok) {
            return false;
        }
        if !self.checksum_agrees() {
            return false;
        }
        match self.spec.kind {
            CaseKind::Throughput => {
                self.client.received == self.spec.bytes
                    && self.server.received == self.spec.bytes
            }
            CaseKind::Rtt => {
                let want = self.spec.chunk as u64 * self.spec.iters;
                self.client.received == want && self.server.received == want
            }
        }
    }
}

/// 对端二进制所在目录：默认与当前可执行文件同目录（`cargo run` / `cargo test`
/// 都会把同一 profile 的二进制放在一起）。
///
/// # Errors
///
/// 取不到当前可执行文件路径时返回错误。
pub fn default_peer_dir() -> Result<PathBuf, String> {
    let exe = std::env::current_exe()
        .map_err(|e| format!("取当前可执行文件路径失败：{e}"))?;
    exe.parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| "当前可执行文件没有父目录".to_string())
}

/// 拼出对端二进制的完整路径。
pub fn peer_binary_path(dir: &Path, role: PeerRole) -> PathBuf {
    let mut name = role.binary_name().to_string();
    if cfg!(windows) {
        name.push_str(".exe");
    }
    dir.join(name)
}

/// 把 [`PeerOptions`] 渲染成命令行参数。
pub fn peer_args(opts: &PeerOptions) -> Vec<String> {
    let mut args = vec![
        "--backend".to_string(),
        opts.backend.name().to_string(),
        "--transport".to_string(),
        opts.transport.name().to_string(),
        "--mode".to_string(),
        opts.mode.name().to_string(),
    ];
    match &opts.endpoint {
        Endpoint::Listen(ep) => {
            args.push("--listen".to_string());
            args.push(ep.clone());
        }
        Endpoint::Connect(ep) => {
            args.push("--connect".to_string());
            args.push(ep.clone());
        }
    }
    args.extend([
        "--bytes".to_string(),
        opts.bytes.to_string(),
        "--chunk".to_string(),
        opts.chunk.to_string(),
        "--iters".to_string(),
        opts.iters.to_string(),
        "--ring-cap".to_string(),
        opts.ring_cap.to_string(),
        "--nodelay".to_string(),
        if opts.nodelay { "true" } else { "false" }.to_string(),
    ]);
    args
}

/// 一个已拉起的对端进程：stdout / stderr 各由一个线程按行送回通道。
struct PeerProcess {
    child_: Child,
    stdout_rx_: mpsc::Receiver<String>,
    stderr_rx_: mpsc::Receiver<String>,
    buffered_: Vec<String>,
    finished_: bool,
}

impl PeerProcess {
    /// 拉起一个对端进程。
    fn spawn_(bin: &Path, args: &[String]) -> Result<Self, String> {
        let mut child = Command::new(bin)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("拉起 `{}` 失败：{e}", bin.display()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "子进程没有 stdout 管道".to_string())?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| "子进程没有 stderr 管道".to_string())?;
        Ok(PeerProcess {
            child_: child,
            stdout_rx_: spawn_line_reader_(stdout),
            stderr_rx_: spawn_line_reader_(stderr),
            buffered_: Vec::new(),
            finished_: false,
        })
    }

    /// 等 `{"event":"ready"}` 行，返回其中的 `addr` 字段。
    fn wait_ready_(&mut self, timeout: Duration) -> Result<String, String> {
        let deadline = Instant::now() + timeout;
        loop {
            let remain = deadline.saturating_duration_since(Instant::now());
            if remain.is_zero() {
                return Err("等待对端 ready 超时".to_string());
            }
            match self.stdout_rx_.recv_timeout(remain) {
                Ok(line) => {
                    if let Ok(fields) = json_parse_flat(&line)
                        && let Some((_, JsonValue::Str(event))) =
                            fields.iter().find(|(k, _)| k == "event")
                        && event == "ready"
                    {
                        let addr = fields
                            .iter()
                            .find(|(k, _)| k == "addr")
                            .and_then(|(_, v)| match v {
                                JsonValue::Str(s) => Some(s.clone()),
                                _ => None,
                            })
                            .ok_or_else(|| "ready 行缺少 addr 字段".to_string())?;
                        return Ok(addr);
                    }
                    self.buffered_.push(line);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    return Err("等待对端 ready 超时".to_string())
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(format!(
                        "对端在 ready 之前就退出了（stdout 已关闭，缓冲行：{:?}）",
                        self.buffered_
                    ))
                }
            }
        }
    }

    /// 收完 stdout / stderr、等进程退出，返回 `(结果行, stderr 文本)`。
    fn finish_(mut self, timeout: Duration) -> Result<(PeerResult, String), String> {
        let deadline = Instant::now() + timeout;
        let mut stdout_lines = std::mem::take(&mut self.buffered_);
        let mut stderr_lines: Vec<String> = Vec::new();
        let mut out_open = true;
        let mut err_open = true;
        while (out_open || err_open) && Instant::now() < deadline {
            if out_open {
                match self.stdout_rx_.recv_timeout(Duration::from_millis(20)) {
                    Ok(line) => stdout_lines.push(line),
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => out_open = false,
                }
            }
            if err_open {
                match self.stderr_rx_.recv_timeout(Duration::from_millis(20)) {
                    Ok(line) => stderr_lines.push(line),
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => err_open = false,
                }
            }
        }
        let status = match self.child_.try_wait() {
            Ok(Some(s)) => Some(s),
            Ok(None) => {
                let _ = self.child_.kill();
                let _ = self.child_.wait();
                None
            }
            Err(e) => return Err(format!("等待子进程失败：{e}")),
        };
        self.finished_ = true;
        let stderr = stderr_lines.join("\n");
        let result = stdout_lines
            .iter()
            .find_map(|l| PeerResult::from_json_line(l).ok())
            .ok_or_else(|| {
                format!(
                    "对端没有给出结果行（退出状态 {:?}；stdout：{:?}；stderr：{stderr}）",
                    status, stdout_lines
                )
            })?;
        Ok((result, stderr))
    }
}

impl Drop for PeerProcess {
    fn drop(&mut self) {
        if !self.finished_ {
            let _ = self.child_.kill();
            let _ = self.child_.wait();
        }
    }
}

/// 起一个线程，把管道按行送进通道；EOF 时线程结束、通道自动断开。
fn spawn_line_reader_<R: std::io::Read + Send + 'static>(
    pipe: R,
) -> mpsc::Receiver<String> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(pipe).lines() {
            match line {
                Ok(l) => {
                    if tx.send(l).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    rx
}

/// UDS 路径：放在系统临时目录，带上 pid 与序号避免并发用例冲突。
fn uds_path_(seq: u64) -> PathBuf {
    std::env::temp_dir().join(format!(
        "buffex_uds_comp_exp_{}_{}.sock",
        std::process::id(),
        seq
    ))
}

/// 跑一个用例。
///
/// # Errors
///
/// 进程拉起失败、握手超时、对端异常退出或结果缺失时返回错误说明。
pub fn run_case(peer_dir: &Path, spec: &CaseSpec, seq: u64) -> Result<CaseOutcome, String> {
    let uds_cleanup = if spec.transport == Transport::Uds {
        Some(uds_path_(seq))
    } else {
        None
    };
    let result = run_case_inner_(peer_dir, spec, seq);
    // 无论成败都清掉 socket 文件：失败的用例不该在 /tmp 里留垃圾。
    if let Some(path) = &uds_cleanup {
        let _ = std::fs::remove_file(path);
    }
    result
}

/// [`run_case`] 的实现体（不负责清理 UDS 文件）。
fn run_case_inner_(peer_dir: &Path, spec: &CaseSpec, seq: u64) -> Result<CaseOutcome, String> {
    let started = Instant::now();
    let (listen_ep, connect_ep) = match spec.transport {
        Transport::Tcp => (
            "127.0.0.1:0".to_string(),
            String::new(), // 由 ready 行回填
        ),
        Transport::Uds => {
            let path = uds_path_(seq);
            if path.exists() {
                std::fs::remove_file(&path)
                    .map_err(|e| format!("清理旧 socket `{}` 失败：{e}", path.display()))?;
            }
            let p = path.to_string_lossy().to_string();
            (p.clone(), p)
        }
    };

    let server_mode = match spec.kind {
        CaseKind::Throughput => Mode::Recv,
        CaseKind::Rtt => Mode::RttServer,
    };
    let client_mode = match spec.kind {
        CaseKind::Throughput => Mode::Send,
        CaseKind::Rtt => Mode::RttClient,
    };

    let server_opts = PeerOptions {
        backend: spec.backend,
        transport: spec.transport,
        mode: server_mode,
        endpoint: Endpoint::Listen(listen_ep),
        bytes: spec.bytes,
        chunk: spec.chunk,
        iters: spec.iters,
        ring_cap: spec.ring_cap,
        nodelay: true,
    };
    let server_bin = peer_binary_path(peer_dir, spec.server);
    let mut server = PeerProcess::spawn_(&server_bin, &peer_args(&server_opts))?;

    // 握手：拿到监听端实际使用的地址（TCP 是 `ip:port`，UDS 是路径）。
    let connected_ep = match spec.transport {
        Transport::Tcp => server.wait_ready_(Duration::from_secs(10))?,
        Transport::Uds => {
            // UDS 下地址就是约定好的路径，这里仍等 ready 以确认 bind 成功。
            let _ = server.wait_ready_(Duration::from_secs(10))?;
            connect_ep
        }
    };

    let client_opts = PeerOptions {
        backend: spec.backend,
        transport: spec.transport,
        mode: client_mode,
        endpoint: Endpoint::Connect(connected_ep),
        bytes: spec.bytes,
        chunk: spec.chunk,
        iters: spec.iters,
        ring_cap: spec.ring_cap,
        nodelay: true,
    };
    let client_bin = peer_binary_path(peer_dir, spec.client);
    let client = PeerProcess::spawn_(&client_bin, &peer_args(&client_opts))?;

    // 用例上限：吞吐按「最慢 32 MiB/s」估，RTT 按「最多 60 秒」兜底。
    let io_timeout = match spec.kind {
        CaseKind::Throughput => {
            let secs = (spec.bytes / (32 * 1024 * 1024)).max(10) + 20;
            Duration::from_secs(secs)
        }
        CaseKind::Rtt => Duration::from_secs(120),
    };

    let (client_res, client_err) = client.finish_(io_timeout)?;
    let (server_res, _server_err) = match server.finish_(io_timeout) {
        Ok(v) => v,
        Err(e) => {
            return Err(format!(
                "{e}\n[客户端 stderr]\n{client_err}\n[客户端结果] {client_res:?}"
            ))
        }
    };

    // 对端自报的后端必须与用例一致：不一致多半是跑到了旧的二进制。
    for (side, res) in [("客户端", &client_res), ("服务端", &server_res)] {
        if res.backend != spec.backend.name() {
            return Err(format!(
                "{side}自报后端 `{}` 与用例要求的 `{}` 不一致（对端二进制可能过期）",
                res.backend,
                spec.backend.name()
            ));
        }
    }

    Ok(CaseOutcome {
        spec: spec.clone(),
        client: client_res,
        server: server_res,
        wall: started.elapsed(),
    })
}

/// 按 harness 选项展开出全部用例。
pub fn plan_cases(opts: &crate::cli::HarnessOptions) -> Vec<CaseSpec> {
    let mut cases = Vec::new();
    if !opts.skip_throughput {
        for &backend in &opts.backends {
            for &transport in &opts.transports {
                for &chunk in &opts.chunks {
                    for &client in &[PeerRole::Tokio, PeerRole::Compio] {
                        cases.push(CaseSpec {
                            kind: CaseKind::Throughput,
                            backend,
                            transport,
                            client,
                            server: client.peer(),
                            chunk,
                            bytes: opts.bytes,
                            iters: 0,
                            ring_cap: opts.ring_cap,
                        });
                    }
                }
            }
        }
    }
    if !opts.skip_rtt {
        for &backend in &opts.backends {
            for &transport in &opts.transports {
                for &chunk in &opts.rtt_sizes {
                    for &client in &[PeerRole::Tokio, PeerRole::Compio] {
                        cases.push(CaseSpec {
                            kind: CaseKind::Rtt,
                            backend,
                            transport,
                            client,
                            server: client.peer(),
                            chunk,
                            bytes: 0,
                            iters: opts.rtt_iters,
                            // buffex 的 RTT 要求「一条消息恰好填满一个环段」：环容量
                            // 必须等于消息大小（原因见 `bench::bench_rtt_client`）。
                            // 直连后端没有环，这个字段对它无意义，原样传过去被忽略。
                            ring_cap: if backend == Backend::Buffex {
                                chunk
                            } else {
                                opts.ring_cap
                            },
                        });
                    }
                }
            }
        }
    }
    cases
}
