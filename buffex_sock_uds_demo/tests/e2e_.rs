//! 端到端集成测试：真的拉起 `peer_tokio` 与 `peer_compio` **两个进程**，让它们
//! 分别用 `buffex_tokio_adapt` / `buffex_compio_adapt` 的 `BuffRead` / `BuffWrite`
//! 经 TCP 或 UNIX domain socket 收发，再用编排层核对结果。
//!
//! 这里覆盖的是「单 crate 测试覆盖不到」的那部分：跨进程、跨运行时、跨适配器。
//! 用例刻意取小载荷（MiB 级），因为它们要真起进程、真建 socket。

use std::path::Path;

use buffex_sock_uds_demo::{
    cli::{Backend, Transport},
    runner::{CaseKind, CaseSpec, PeerRole, run_case},
};

/// 对端二进制所在目录：`cargo test` 会把 bin target 与测试二进制放在同一目录，
/// 因此用 `CARGO_BIN_EXE_*` 的父目录即可。
fn peer_dir_() -> &'static Path {
    Path::new(env!("CARGO_BIN_EXE_peer_tokio"))
        .parent()
        .expect("对端二进制应有父目录")
}

/// 单向吞吐：TCP 上 tokio 发、compio 收。
/// - 测试目标：两个不同运行时的进程各自用自家适配器的 `BuffWrite` / `BuffRead`
///   在 TCP 上完成一次完整搬运，且收发字节完全一致。
/// - 测试手段：`CaseSpec` 指定 `tcp` + 2 MiB 负载 + 64 KiB 粒度 + 64 KiB 环容量
///   （环容量小于负载，必然反复走到「环满 → 冲刷」这条路径），由 `run_case`
///   拉起监听端（compio recv）与连接端（tokio send）。
/// - 判定标准：两侧都 `ok`、都收到 2 MiB、校验和一致（`outcome.ok()` 全为真）。
#[test]
fn tcp_tokio_sends_compio_receives_() {
    let spec = CaseSpec {
        kind: CaseKind::Throughput,
        backend: Backend::Buffex,
        transport: Transport::Tcp,
        client: PeerRole::Tokio,
        server: PeerRole::Compio,
        chunk: 64 * 1024,
        bytes: 2 * 1024 * 1024,
        iters: 0,
        ring_cap: 64 * 1024,
    };
    let outcome = run_case(peer_dir_(), &spec, 1).expect("TCP 吞吐用例应能跑通");
    assert!(outcome.client.ok, "发送端应正常收尾");
    assert!(outcome.server.ok, "接收端应正常收尾");
    assert_eq!(outcome.client.received, spec.bytes, "发送端应发出全部负载");
    assert_eq!(outcome.server.received, spec.bytes, "接收端应收满全部负载");
    assert!(outcome.checksum_agrees(), "两侧校验和应一致");
    assert!(outcome.ok(), "用例应判定为成功");
}

/// 单向吞吐：UDS 上 compio 发、tokio 收，且消息粒度大于环容量。
/// - 测试目标：反方向、反传输方式同样成立；`chunk > ring_cap` 时「借段 → 部分
///   搬运 → 再借段」的循环不会丢字节或乱序。
/// - 测试手段：`uds` + 4 MiB 负载 + 256 KiB 粒度 + 64 KiB 环容量。
/// - 判定标准：两侧各收发 4 MiB，校验和一致，`outcome.ok()` 为真。
#[test]
fn uds_compio_sends_tokio_receives_() {
    let spec = CaseSpec {
        kind: CaseKind::Throughput,
        backend: Backend::Buffex,
        transport: Transport::Uds,
        client: PeerRole::Compio,
        server: PeerRole::Tokio,
        chunk: 256 * 1024,
        bytes: 4 * 1024 * 1024,
        iters: 0,
        ring_cap: 64 * 1024,
    };
    let outcome = run_case(peer_dir_(), &spec, 2).expect("UDS 吞吐用例应能跑通");
    assert!(outcome.checksum_agrees(), "两侧校验和应一致");
    assert_eq!(outcome.client.received, spec.bytes);
    assert_eq!(outcome.server.received, spec.bytes);
    assert!(outcome.ok(), "用例应判定为成功");
}

/// 往返时延：TCP 上 tokio 作客户端、compio 作服务端，回显内容必须逐字节一致。
/// - 测试目标：两个适配器在同一连接上「读 + 写」并存时，乒乓一来一回不丢字节、
///   不乱序（`RttServer` 的收尾也要让客户端看到干净的结束）。
/// - 测试手段：`tcp` + 4 KiB 消息 + 200 次往返 + 环容量 4 KiB（RTT 的硬约束：
///   环段必须能被一条消息恰好填满，否则 `read_async` 不会交付数据）。
/// - 判定标准：两侧校验和一致、客户端记录到 200 次往返（`received == 4 KiB ×
///   200`）、`p50_ns > 0`，且 `outcome.ok()` 为真。
#[test]
fn tcp_rtt_tokio_client_compio_server_() {
    const ITERS: u64 = 200;
    let spec = CaseSpec {
        kind: CaseKind::Rtt,
        backend: Backend::Buffex,
        transport: Transport::Tcp,
        client: PeerRole::Tokio,
        server: PeerRole::Compio,
        chunk: 4096,
        bytes: 0,
        iters: ITERS,
        // RTT 要求「一条消息恰好填满一个环段」，见 bench::bench_rtt_client 的文档。
        ring_cap: 4096,
    };
    let outcome = run_case(peer_dir_(), &spec, 3).expect("TCP RTT 用例应能跑通");
    assert!(outcome.checksum_agrees(), "请求与回显的校验和应一致");
    assert_eq!(outcome.client.received, 4096 * ITERS, "客户端应完成全部往返");
    assert!(outcome.client.p50_ns > 0, "应记录到每轮耗时");
    assert!(outcome.ok(), "用例应判定为成功");
}

/// 往返时延：UDS 上 compio 作客户端、tokio 作服务端。
/// - 测试目标：方向反过来时 RTT 同样成立，且小消息（64 B）的收尾路径正常。
/// - 测试手段：`uds` + 64 B 消息 + 500 次往返。
/// - 判定标准：两侧校验和一致、客户端完成 500 次往返、`outcome.ok()` 为真。
#[test]
fn uds_rtt_compio_client_tokio_server_() {
    const ITERS: u64 = 500;
    let spec = CaseSpec {
        kind: CaseKind::Rtt,
        backend: Backend::Buffex,
        transport: Transport::Uds,
        client: PeerRole::Compio,
        server: PeerRole::Tokio,
        chunk: 64,
        bytes: 0,
        iters: ITERS,
        ring_cap: 64,
    };
    let outcome = run_case(peer_dir_(), &spec, 4).expect("UDS RTT 用例应能跑通");
    assert!(outcome.checksum_agrees(), "请求与回显的校验和应一致");
    assert_eq!(outcome.client.received, 64 * ITERS);
    assert!(outcome.ok(), "用例应判定为成功");
}

/// 测试编排层对「对端二进制不存在」的报错路径。
/// - 测试目标：`run_case` 在找不到对端二进制时给出可读错误，而不是 panic 或挂死。
/// - 测试手段：把 `peer_dir` 指向一个空目录，跑一个最小 TCP 吞吐用例。
/// - 判定标准：返回 `Err`，且错误信息里带上尝试执行的二进制路径。
#[test]
fn missing_peer_binary_is_reported_() {
    let empty = std::env::temp_dir().join(format!("buffex_exp_empty_{}", std::process::id()));
    std::fs::create_dir_all(&empty).expect("应能建临时目录");
    let spec = CaseSpec {
        kind: CaseKind::Throughput,
        backend: Backend::Buffex,
        transport: Transport::Tcp,
        client: PeerRole::Tokio,
        server: PeerRole::Compio,
        chunk: 4096,
        bytes: 4096,
        iters: 0,
        ring_cap: 4096,
    };
    let err = run_case(&empty, &spec, 5).expect_err("对端不存在时应报错");
    assert!(
        err.contains("peer_compio"),
        "错误信息应说明是哪个二进制缺失：{err}"
    );
    let _ = std::fs::remove_dir_all(&empty);
}

/// 对照组：TCP 上 tokio 直连发、compio 直连收（不经任何 buffex crate）。
/// - 测试目标：`--backend raw` 这条「只有 socket、没有环」的路径在跨进程、跨运行时
///   场景下同样能完成一次完整搬运。
/// - 测试手段：与 buffex 用例完全相同的 `CaseSpec`，只把 `backend` 换成 `Raw`；
///   1 MiB 负载 + 64 KiB 粒度（`ring_cap` 对直连无意义，原样传着被忽略）。
/// - 判定标准：两侧都 `ok`、各收发 1 MiB、校验和一致且 `outcome.ok()` 为真。
#[test]
fn tcp_raw_tokio_sends_compio_receives_() {
    let spec = CaseSpec {
        kind: CaseKind::Throughput,
        backend: Backend::Raw,
        transport: Transport::Tcp,
        client: PeerRole::Tokio,
        server: PeerRole::Compio,
        chunk: 64 * 1024,
        bytes: 1024 * 1024,
        iters: 0,
        ring_cap: 64 * 1024,
    };
    let outcome = run_case(peer_dir_(), &spec, 6).expect("直连 TCP 吞吐用例应能跑通");
    assert_eq!(outcome.client.backend, "raw", "对端应自报 raw 后端");
    assert_eq!(outcome.server.backend, "raw", "对端应自报 raw 后端");
    assert_eq!(outcome.client.received, spec.bytes);
    assert_eq!(outcome.server.received, spec.bytes);
    assert!(outcome.checksum_agrees(), "两侧校验和应一致");
    assert!(outcome.ok(), "用例应判定为成功");
}

/// 对照组与 buffex 组搬的是同一串字节：同样输入下两组校验和必须相同。
/// - 测试目标：证明「对照组」与「buffex 路径」的负载、分帧与校验口径完全一致，
///   因此两组的吞吐/RTT 数据可以直接对比。
/// - 测试手段：同一条 `CaseSpec`（UDS、compio 发 → tokio 收、2 MiB、64 KiB 粒度）
///   分别以 `Backend::Buffex` 与 `Backend::Raw` 各跑一次。
/// - 判定标准：两次的客户端校验和相同、服务端校验和相同，且两侧收到的字节数相同。
#[test]
fn raw_and_buffex_agree_on_payload_() {
    let base = CaseSpec {
        kind: CaseKind::Throughput,
        backend: Backend::Buffex,
        transport: Transport::Uds,
        client: PeerRole::Compio,
        server: PeerRole::Tokio,
        chunk: 64 * 1024,
        bytes: 2 * 1024 * 1024,
        iters: 0,
        ring_cap: 64 * 1024,
    };
    let with_buffex = run_case(peer_dir_(), &base, 7).expect("buffex 组应能跑通");
    let raw_spec = CaseSpec {
        backend: Backend::Raw,
        ..base
    };
    let with_raw = run_case(peer_dir_(), &raw_spec, 8).expect("直连组应能跑通");

    assert_eq!(
        with_buffex.client.checksum, with_raw.client.checksum,
        "发送侧：两组对同一负载的校验和应相同"
    );
    assert_eq!(
        with_buffex.server.checksum, with_raw.server.checksum,
        "接收侧：两组对同一负载的校验和应相同"
    );
    assert_eq!(with_buffex.server.received, with_raw.server.received);
    assert!(with_buffex.ok() && with_raw.ok(), "两组都应判定为成功");
}

/// 对照组：UDS 上的 RTT（直连后端不要求 `ring-cap == chunk`）。
/// - 测试目标：直连路径的乒乓收发成立，且 `ring-cap` 这个只对环有意义的参数在直连
///   后端被忽略（不会因为「不等于 chunk」被 CLI 拒绝）。
/// - 测试手段：`backend=raw` + UDS + 4 KiB 消息 + 100 次往返 + `ring_cap` 故意取
///   64 KiB（对直连无意义）。
/// - 判定标准：`run_case` 能跑通、两侧校验和一致、客户端完成全部往返、`p50_ns > 0`。
#[test]
fn uds_raw_rtt_tokio_client_() {
    const ITERS: u64 = 100;
    let spec = CaseSpec {
        kind: CaseKind::Rtt,
        backend: Backend::Raw,
        transport: Transport::Uds,
        client: PeerRole::Tokio,
        server: PeerRole::Compio,
        chunk: 4096,
        bytes: 0,
        iters: ITERS,
        ring_cap: 64 * 1024,
    };
    let outcome = run_case(peer_dir_(), &spec, 9).expect("直连 UDS RTT 用例应能跑通");
    assert!(outcome.checksum_agrees(), "请求与回显的校验和应一致");
    assert_eq!(outcome.client.received, 4096 * ITERS);
    assert!(outcome.client.p50_ns > 0, "应记录到每轮耗时");
    assert!(outcome.ok(), "用例应判定为成功");
}
