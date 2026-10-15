//! 命令行参数解析：对端（`peer_tokio` / `peer_compio`）与编排器（`bench_harness`）。
//!
//! 刻意不引入 `clap`：本实验的参数面很小，手写解析能少一个依赖，也方便
//! 集成测试直接构造结构体调用 [`crate::runner`]。

use std::path::PathBuf;

/// 两个对端之间的传输方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// TCP over IPv4 loopback。
    Tcp,
    /// UNIX domain socket（`SOCK_STREAM`）。
    Uds,
}

impl Transport {
    /// 命令行里的名字（也是报告里的名字）。
    pub const fn name(self) -> &'static str {
        match self {
            Transport::Tcp => "tcp",
            Transport::Uds => "uds",
        }
    }

    /// 解析名字。
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "tcp" => Ok(Transport::Tcp),
            "uds" | "unix" => Ok(Transport::Uds),
            other => Err(format!("无法识别的传输方式 `{other}`（可用 tcp / uds）")),
        }
    }
}

/// 对端收发走的「后端」。
///
/// 这是本次实验的对照组开关：`Buffex` 走适配器（环 + `TrBuffRead` /
/// `TrBuffWrite`），`Raw` **完全不碰 buffex 相关的 crate**，直接对着
/// `tokio` / `compio` 的 socket 做 `read` / `write`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// 经 `buffex_tokio_adapt` / `buffex_compio_adapt` 的 `BuffRead` / `BuffWrite`。
    Buffex,
    /// 直连对照组：只有 socket，没有环、没有适配器。
    Raw,
}

impl Backend {
    /// 命令行里的名字（也是报告里的名字）。
    pub const fn name(self) -> &'static str {
        match self {
            Backend::Buffex => "buffex",
            Backend::Raw => "raw",
        }
    }

    /// 报告里用的中文标签。
    pub const fn label(self) -> &'static str {
        match self {
            Backend::Buffex => "buffex 环",
            Backend::Raw => "直连（对照组）",
        }
    }

    /// 解析名字。
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "buffex" => Ok(Backend::Buffex),
            "raw" | "direct" => Ok(Backend::Raw),
            other => Err(format!("无法识别的后端 `{other}`（可用 buffex / raw）")),
        }
    }
}

/// 对端在本次运行里扮演的角色。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// 吞吐测试：只发。
    Send,
    /// 吞吐测试：只收。
    Recv,
    /// 时延测试：发起方（发一条、收一条回显）。
    RttClient,
    /// 时延测试：服务方（收一条、回显一条）。
    RttServer,
}

impl Mode {
    /// 命令行里的名字。
    pub const fn name(self) -> &'static str {
        match self {
            Mode::Send => "send",
            Mode::Recv => "recv",
            Mode::RttClient => "rtt-client",
            Mode::RttServer => "rtt-server",
        }
    }

    /// 解析名字。
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "send" => Ok(Mode::Send),
            "recv" => Ok(Mode::Recv),
            "rtt-client" => Ok(Mode::RttClient),
            "rtt-server" => Ok(Mode::RttServer),
            other => Err(format!(
                "无法识别的模式 `{other}`（可用 send / recv / rtt-client / rtt-server）"
            )),
        }
    }
}

/// 对端的连接方向。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    /// 监听地址（TCP `ip:port`，UDS 路径）；TCP 允许端口写 0 让内核挑。
    Listen(String),
    /// 连接地址（TCP `ip:port`，UDS 路径）。
    Connect(String),
}

/// 一个对端进程的全部参数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerOptions {
    /// 收发后端：适配器（buffex）还是直连（对照组）。
    pub backend: Backend,
    /// 传输方式。
    pub transport: Transport,
    /// 本次扮演的角色。
    pub mode: Mode,
    /// 监听还是连接。
    pub endpoint: Endpoint,
    /// 吞吐模式的负载总字节数（不含 8 字节头部）。
    pub bytes: u64,
    /// 单条消息 / 单次借段的字节数。
    pub chunk: usize,
    /// RTT 模式的往返次数。
    pub iters: u64,
    /// 适配器内部环的容量。
    pub ring_cap: usize,
    /// TCP 下是否设置 `TCP_NODELAY`（RTT 场景必须开，否则会被 Nagle 拖成 40ms）。
    pub nodelay: bool,
}

impl Default for PeerOptions {
    fn default() -> Self {
        PeerOptions {
            backend: Backend::Buffex,
            transport: Transport::Tcp,
            mode: Mode::Send,
            endpoint: Endpoint::Listen("127.0.0.1:0".to_string()),
            bytes: 128 * 1024 * 1024,
            chunk: 64 * 1024,
            iters: 1000,
            ring_cap: 64 * 1024,
            nodelay: true,
        }
    }
}

/// 解析「带单位的大小」：`4096`、`64K`、`64KiB`、`128M`、`1G`（1024 进制）。
pub fn parse_size(s: &str) -> Result<u64, String> {
    let t = s.trim();
    if t.is_empty() {
        return Err("空的大小".to_string());
    }
    let digits_end = t
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(t.len());
    let (num, unit) = t.split_at(digits_end);
    if num.is_empty() {
        return Err(format!("大小 `{s}` 缺少数字部分"));
    }
    let n: u64 = num
        .parse()
        .map_err(|e| format!("大小 `{s}` 的数字部分无法解析：{e}"))?;
    let mult: u64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        other => return Err(format!("大小 `{s}` 的单位 `{other}` 无法识别")),
    };
    n.checked_mul(mult)
        .ok_or_else(|| format!("大小 `{s}` 溢出 u64"))
}

/// 解析逗号分隔的大小列表，例如 `4K,64K,256K`。
pub fn parse_size_list(s: &str) -> Result<Vec<usize>, String> {
    let mut out = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let v = parse_size(part)?;
        let v = usize::try_from(v).map_err(|_| format!("大小 `{part}` 超出 usize"))?;
        out.push(v);
    }
    if out.is_empty() {
        return Err(format!("大小列表 `{s}` 为空"));
    }
    Ok(out)
}

/// 取一个 `--key value` 的参数值。
fn value_of_(args: &[String], i: &mut usize, key: &str) -> Result<String, String> {
    let cur = &args[*i];
    if let Some(v) = cur.strip_prefix(&format!("{key}=")) {
        return Ok(v.to_string());
    }
    *i += 1;
    args.get(*i)
        .cloned()
        .ok_or_else(|| format!("`{key}` 后面缺少取值"))
}

/// 解析对端的命令行参数（不含 argv[0]）。
///
/// # Errors
///
/// 参数缺失、取值非法或互相冲突时返回带说明的错误字符串。
pub fn parse_peer_args(args: &[String]) -> Result<PeerOptions, String> {
    let mut opt = PeerOptions::default();
    let mut transport: Option<Transport> = None;
    let mut mode: Option<Mode> = None;
    let mut endpoint: Option<Endpoint> = None;
    let mut backend: Option<Backend> = None;
    let mut i = 0usize;
    while i < args.len() {
        let cur = args[i].clone();
        let key = cur.split('=').next().unwrap_or("").to_string();
        match key.as_str() {
            "--backend" => {
                backend = Some(Backend::parse(&value_of_(args, &mut i, "--backend")?)?)
            }
            "--transport" => transport = Some(Transport::parse(&value_of_(args, &mut i, "--transport")?)?),
            "--mode" => mode = Some(Mode::parse(&value_of_(args, &mut i, "--mode")?)?),
            "--listen" => {
                endpoint = Some(Endpoint::Listen(value_of_(args, &mut i, "--listen")?))
            }
            "--connect" => {
                endpoint = Some(Endpoint::Connect(value_of_(args, &mut i, "--connect")?))
            }
            "--bytes" => opt.bytes = parse_size(&value_of_(args, &mut i, "--bytes")?)?,
            "--chunk" => {
                let v = parse_size(&value_of_(args, &mut i, "--chunk")?)?;
                opt.chunk = usize::try_from(v).map_err(|_| "chunk 超出 usize".to_string())?;
            }
            "--iters" => {
                opt.iters = value_of_(args, &mut i, "--iters")?
                    .parse()
                    .map_err(|e| format!("iters 无法解析：{e}"))?;
            }
            "--ring-cap" => {
                let v = parse_size(&value_of_(args, &mut i, "--ring-cap")?)?;
                opt.ring_cap = usize::try_from(v).map_err(|_| "ring-cap 超出 usize".to_string())?;
            }
            "--nodelay" => {
                let v = value_of_(args, &mut i, "--nodelay")?;
                opt.nodelay = match v.as_str() {
                    "1" | "true" | "yes" | "on" => true,
                    "0" | "false" | "no" | "off" => false,
                    other => return Err(format!("--nodelay 取值 `{other}` 无法识别")),
                };
            }
            "--help" | "-h" => return Err("PEER_HELP".to_string()),
            other => return Err(format!("无法识别的参数 `{other}`")),
        }
        i += 1;
    }
    opt.backend = backend.unwrap_or(Backend::Buffex);
    opt.transport = transport.ok_or("缺少 --transport")?;
    opt.mode = mode.ok_or("缺少 --mode")?;
    opt.endpoint = endpoint.ok_or("缺少 --listen 或 --connect")?;
    if opt.chunk == 0 {
        return Err("--chunk 必须大于 0".to_string());
    }
    if opt.ring_cap < 2 {
        return Err("--ring-cap 必须不小于 2（ring 的容量下限）".to_string());
    }
    if matches!(opt.mode, Mode::Send | Mode::Recv) && opt.bytes == 0 {
        return Err("吞吐模式下 --bytes 必须大于 0".to_string());
    }
    if matches!(opt.mode, Mode::RttClient | Mode::RttServer) && opt.iters == 0 {
        return Err("RTT 模式下 --iters 必须大于 0".to_string());
    }
    if opt.backend == Backend::Buffex
        && matches!(opt.mode, Mode::RttClient | Mode::RttServer)
        && opt.ring_cap != opt.chunk
    {
        // 实验实测（tests/read_semantics_.rs）：两个适配器的 `read_async` 只在
        // 「一个可写环段被填满」或「设备结束 / 出错」时才交付数据。乒乓协议里
        // 对端发完一条就等回显，不会再补数据，因此必须让「一条消息恰好填满一个
        // 环段」：ring-cap == chunk。否则两端都会卡在等环段填满上（永久挂起）。
        return Err(format!(
            "RTT 模式要求 ring-cap == chunk（当前 ring-cap={} chunk={}）：适配器的 read_async 只在环段被填满时交付数据，一条消息必须恰好填满环",
            opt.ring_cap, opt.chunk
        ));
    }
    Ok(opt)
}

/// 编排器（`bench_harness`）的参数。
#[derive(Debug, Clone)]
pub struct HarnessOptions {
    /// 要跑的后端列表（默认「buffex 环 + 直连对照组」两组都跑）。
    pub backends: Vec<Backend>,
    /// 要跑的传输方式列表。
    pub transports: Vec<Transport>,
    /// 吞吐测试的消息粒度列表（每个都会与每种传输方式、每个方向组合）。
    pub chunks: Vec<usize>,
    /// 每个吞吐用例的负载总字节数。
    pub bytes: u64,
    /// RTT 测试的消息大小列表。
    pub rtt_sizes: Vec<usize>,
    /// RTT 每个用例的往返次数。
    pub rtt_iters: u64,
    /// 适配器内部环的容量（两个对端一致）。
    pub ring_cap: usize,
    /// 对端二进制所在目录；`None` 表示「与 bench_harness 同目录」。
    pub peer_dir: Option<PathBuf>,
    /// markdown 报告输出路径；`None` 表示只打印不落盘。
    pub out: Option<PathBuf>,
    /// 跳过吞吐测试。
    pub skip_throughput: bool,
    /// 跳过 RTT 测试。
    pub skip_rtt: bool,
    /// 每个用例重复次数（报告里取最好成绩）。
    pub repeats: u32,
}

impl Default for HarnessOptions {
    fn default() -> Self {
        HarnessOptions {
            backends: vec![Backend::Buffex, Backend::Raw],
            transports: vec![Transport::Tcp, Transport::Uds],
            chunks: vec![4 * 1024, 64 * 1024, 256 * 1024],
            bytes: 128 * 1024 * 1024,
            rtt_sizes: vec![64, 4096],
            rtt_iters: 1000,
            ring_cap: 64 * 1024,
            peer_dir: None,
            out: None,
            skip_throughput: false,
            skip_rtt: false,
            repeats: 1,
        }
    }
}

/// 解析编排器的命令行参数（不含 argv[0]）。
///
/// # Errors
///
/// 参数缺失、取值非法时返回带说明的错误字符串。
pub fn parse_harness_args(args: &[String]) -> Result<HarnessOptions, String> {
    let mut opt = HarnessOptions::default();
    let mut backends: Option<Vec<Backend>> = None;
    let mut transports: Option<Vec<Transport>> = None;
    let mut i = 0usize;
    while i < args.len() {
        let cur = args[i].clone();
        let key = cur.split('=').next().unwrap_or("").to_string();
        match key.as_str() {
            "--backends" => {
                let raw = value_of_(args, &mut i, "--backends")?;
                let mut list = Vec::new();
                for p in raw.split(',') {
                    let p = p.trim();
                    if p.is_empty() {
                        continue;
                    }
                    list.push(Backend::parse(p)?);
                }
                if list.is_empty() {
                    return Err("--backends 为空".to_string());
                }
                backends = Some(list);
            }
            "--transports" => {
                let raw = value_of_(args, &mut i, "--transports")?;
                let mut list = Vec::new();
                for p in raw.split(',') {
                    let p = p.trim();
                    if p.is_empty() {
                        continue;
                    }
                    list.push(Transport::parse(p)?);
                }
                if list.is_empty() {
                    return Err("--transports 为空".to_string());
                }
                transports = Some(list);
            }
            "--chunks" => opt.chunks = parse_size_list(&value_of_(args, &mut i, "--chunks")?)?,
            "--bytes" => opt.bytes = parse_size(&value_of_(args, &mut i, "--bytes")?)?,
            "--rtt-sizes" => {
                opt.rtt_sizes = parse_size_list(&value_of_(args, &mut i, "--rtt-sizes")?)?
            }
            "--rtt-iters" => {
                opt.rtt_iters = value_of_(args, &mut i, "--rtt-iters")?
                    .parse()
                    .map_err(|e| format!("rtt-iters 无法解析：{e}"))?;
            }
            "--ring-cap" => {
                let v = parse_size(&value_of_(args, &mut i, "--ring-cap")?)?;
                opt.ring_cap = usize::try_from(v).map_err(|_| "ring-cap 超出 usize".to_string())?;
            }
            "--peer-dir" => {
                opt.peer_dir = Some(PathBuf::from(value_of_(args, &mut i, "--peer-dir")?))
            }
            "--out" => opt.out = Some(PathBuf::from(value_of_(args, &mut i, "--out")?)),
            "--repeats" => {
                opt.repeats = value_of_(args, &mut i, "--repeats")?
                    .parse()
                    .map_err(|e| format!("repeats 无法解析：{e}"))?;
            }
            "--skip-throughput" => {
                opt.skip_throughput = true;
                i += 1;
                continue;
            }
            "--skip-rtt" => {
                opt.skip_rtt = true;
                i += 1;
                continue;
            }
            "--help" | "-h" => return Err("HARNESS_HELP".to_string()),
            other => return Err(format!("无法识别的参数 `{other}`")),
        }
        i += 1;
    }
    if let Some(b) = backends {
        opt.backends = b;
    }
    if let Some(t) = transports {
        opt.transports = t;
    }
    if opt.repeats == 0 {
        return Err("--repeats 必须大于 0".to_string());
    }
    if opt.ring_cap < 2 {
        return Err("--ring-cap 必须不小于 2".to_string());
    }
    Ok(opt)
}

/// 对端的帮助文本。
pub const PEER_HELP: &str = "\
peer 参数：
  --backend <buffex|raw>           收发后端：buffex 环 / 直连对照组（默认 buffex）
  --transport <tcp|uds>            传输方式（必填）
  --mode <send|recv|rtt-client|rtt-server>   角色（必填）
  --listen <ip:port|路径>          监听（与 --connect 二选一）
  --connect <ip:port|路径>         连接（与 --listen 二选一）
  --bytes <大小>                   吞吐模式负载总字节数（默认 128M）
  --chunk <大小>                   单条消息 / 单次借段字节数（默认 64K）
  --iters <次数>                   RTT 往返次数（默认 1000）
  --ring-cap <大小>                适配器内部环容量（默认 64K；buffex 的 RTT 必须等于 --chunk；raw 后端忽略）
  --nodelay <true|false>           TCP 是否禁用 Nagle（默认 true）";

/// 编排器的帮助文本。
pub const HARNESS_HELP: &str = "\
bench_harness 参数：
  --backends <buffex,raw>          要跑的后端（默认 buffex,raw：带对照组一起跑）
  --transports <tcp,uds>           要跑的传输方式（默认 tcp,uds）
  --chunks <4K,64K,256K>           吞吐测试的消息粒度（默认 4K,64K,256K）
  --bytes <大小>                   每个吞吐用例的负载总量（默认 128M）
  --rtt-sizes <64,4K>              RTT 消息大小（默认 64,4K）
  --rtt-iters <次数>               RTT 往返次数（默认 1000）
  --ring-cap <大小>                适配器内部环容量（默认 64K；buffex 的 RTT 必须等于 --chunk；raw 后端忽略）
  --repeats <次数>                 每个用例重复次数，报告取最好成绩（默认 1）
  --peer-dir <目录>                对端二进制目录（默认与 bench_harness 同目录）
  --out <文件>                     markdown 报告输出路径（默认 results/bench-<时间>.md）
  --skip-throughput                只跑 RTT
  --skip-rtt                       只跑吞吐";
