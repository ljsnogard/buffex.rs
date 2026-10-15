//! 纯函数层的单元测试：分帧头部、负载模式、滚动校验和、结果 JSON 行、命令行
//! 解析与用例展开。
//!
//! 这一层**不起进程**，因此跑得很快，适合在每次改动后先跑它。

use buffex_sock_uds_demo::{
    checksum::RollingSum64,
    cli::{
        Backend, Endpoint, HarnessOptions, Mode, PeerOptions, Transport, parse_harness_args,
        parse_peer_args, parse_size, parse_size_list,
    },
    metrics::{PeerResult, percentile_ns},
    proto::{HEADER_LEN, decode_header, encode_header, fill_pattern, pattern_buf, pattern_byte},
    runner::{CaseKind, PeerRole, plan_cases},
};

/// 测试分帧头部的编解码往返。
/// - 手段：对若干长度（0、1、251、1 MiB、`u64::MAX`）做 `encode_header` →
///   `decode_header`。
/// - 判断：解出的长度与编码前逐位相同，且头部长度恒为 `HEADER_LEN`。
#[test]
fn header_roundtrip_() {
    for total in [0u64, 1, 251, 1024 * 1024, u64::MAX] {
        let head = encode_header(total);
        assert_eq!(head.len(), HEADER_LEN, "头部长度应固定");
        assert_eq!(decode_header(&head), total, "编解码应往返一致");
    }
}

/// 测试确定性负载模式的取值规则。
/// - 手段：直接检查 `pattern_byte`，再用 `fill_pattern` 从不同绝对下标填充。
/// - 判断：第 `i` 个字节等于 `(i % 251) as u8`；从任意 `base` 填充的缓冲与
///   `pattern_buf` 的对应切片一致。
#[test]
fn pattern_is_deterministic_() {
    assert_eq!(pattern_byte(0), 0);
    assert_eq!(pattern_byte(250), 250);
    assert_eq!(pattern_byte(251), 0, "周期应为 251");
    assert_eq!(pattern_byte(252), 1);

    let whole = pattern_buf(1024);
    let mut part = vec![0u8; 512];
    fill_pattern(&mut part, 300);
    assert_eq!(part, whole[300..812], "从任意下标填充都应与整段一致");
}

/// 测试滚动校验和对「分块方式」不敏感。
/// - 手段：同一串字节分别按「一次性喂入」「按 1 字节喂」「按 7 字节喂」更新三次
///   校验和。
/// - 判断：三次 `finish()` 完全相同——说明尾部暂存与 `u64` 分组折算不依赖分块。
#[test]
fn checksum_is_chunk_agnostic_() {
    let data = pattern_buf(1000);

    let mut a = RollingSum64::new();
    a.update(&data);

    let mut b = RollingSum64::new();
    for byte in &data {
        b.update(&[*byte]);
    }

    let mut c = RollingSum64::new();
    for chunk in data.chunks(7) {
        c.update(chunk);
    }

    assert_eq!(a.finish(), b.finish(), "逐字节喂入应给出同一校验和");
    assert_eq!(a.finish(), c.finish(), "按 7 字节喂入应给出同一校验和");
}

/// 测试校验和能区分不同内容与不同长度。
/// - 手段：对「原数据」「改一个字节」「截短一字节」「补一个字节」四种输入分别求值。
/// - 判断：四种取值两两不同——否则校验和没有对账能力。
#[test]
fn checksum_detects_differences_() {
    let data = pattern_buf(256);

    let mut same = RollingSum64::new();
    same.update(&data);

    let mut flipped = RollingSum64::new();
    let mut other = data.clone();
    other[100] ^= 0xff;
    flipped.update(&other);

    let mut shorter = RollingSum64::new();
    shorter.update(&data[..255]);

    let mut longer = RollingSum64::new();
    longer.update(&data);
    longer.update(&[0]);

    let values = [same.finish(), flipped.finish(), shorter.finish(), longer.finish()];
    for i in 0..values.len() {
        for j in (i + 1)..values.len() {
            assert_ne!(values[i], values[j], "第 {i} 与第 {j} 个校验和不应相同");
        }
    }
}

/// 测试结果结构的 JSON 行编解码往返。
/// - 手段：构造一个字段取非平凡值（含引号与换行的 `note`）的 `PeerResult`，
///   序列化成一行再解析回来。
/// - 判断：解析结果与原结构逐字段相等。
#[test]
fn peer_result_json_roundtrip_() {
    let src = PeerResult {
        role: "tokio".to_string(),
        backend: "raw".to_string(),
        transport: "uds".to_string(),
        mode: "rtt-client".to_string(),
        bytes: 4096 * 1000,
        chunk: 4096,
        iters: 1000,
        ring_cap: 65536,
        elapsed_ns: 123_456_789,
        mib_per_s: 33.25,
        checksum: 0xdead_beef_cafe_f00d,
        received: 4096 * 1000,
        ok: true,
        p50_ns: 4242,
        p99_ns: 9999,
        avg_ns: 5150,
        note: "平均 RTT 4.2 µs，含 \"引号\" 与换行\n第二行".to_string(),
    };
    let line = src.to_json_line();
    assert_eq!(line.lines().count(), 1, "结果必须是一行");
    let back = PeerResult::from_json_line(&line).expect("应能解析回结构");
    assert_eq!(back, src, "往返后应完全一致");
}

/// 测试畸形 JSON 会被拒绝而不是 panic。
/// - 手段：解析缺失字段、类型不符、语法错误的若干输入。
/// - 判断：全部返回 `Err`。
#[test]
fn peer_result_rejects_bad_json_() {
    assert!(PeerResult::from_json_line("not json").is_err());
    assert!(PeerResult::from_json_line("{\"role\":\"tokio\"}").is_err(), "缺字段应报错");
    assert!(
        PeerResult::from_json_line(
            "{\"event\":\"result\",\"role\":1,\"transport\":\"tcp\",\"mode\":\"send\",\"bytes\":1,\
             \"chunk\":1,\"iters\":0,\"ring_cap\":2,\"elapsed_ns\":1,\"mib_per_s\":1.0,\
             \"checksum\":1,\"received\":1,\"ok\":true,\"p50_ns\":0,\"p99_ns\":0,\"note\":\"\"}"
        )
        .is_err(),
        "role 类型不符应报错"
    );
}

/// 测试「带单位的大小」解析。
/// - 手段：解析 `4096`、`64K`、`64KiB`、`2M`、`1G` 以及若干非法输入。
/// - 判断：合法输入给出期望的字节数；非法输入（空、无数字、未知单位、溢出）返回
///   `Err`。
#[test]
fn size_parsing_() {
    assert_eq!(parse_size("4096").expect("纯数字"), 4096);
    assert_eq!(parse_size("64K").expect("K"), 64 * 1024);
    assert_eq!(parse_size("64KiB").expect("KiB"), 64 * 1024);
    assert_eq!(parse_size("2M").expect("M"), 2 * 1024 * 1024);
    assert_eq!(parse_size("1G").expect("G"), 1024 * 1024 * 1024);

    assert!(parse_size("").is_err());
    assert!(parse_size("K").is_err());
    assert!(parse_size("12X").is_err());
    assert!(parse_size("99999999999999999999G").is_err(), "溢出应报错");

    assert_eq!(
        parse_size_list("4K, 64K ,256K").expect("列表"),
        vec![4 * 1024, 64 * 1024, 256 * 1024]
    );
    assert!(parse_size_list(",").is_err());
}

/// 测试对端参数解析的必填项与约束。
/// - 手段：给 `parse_peer_args` 分别喂「完整参数」「缺 --transport」「缺端点」
///   「RTT 且 ring-cap 不等于 chunk」「chunk 为 0」五种输入。
/// - 判断：完整参数解析出期望的字段；四种非法输入全部返回 `Err`，其中 RTT 的
///   容量约束报错信息里说明「一条消息必须恰好填满一个环段」。
#[test]
fn peer_args_validation_() {
    let ok = vec![
        "--transport".to_string(),
        "uds".to_string(),
        "--mode".to_string(),
        "rtt-client".to_string(),
        "--connect".to_string(),
        "/tmp/x.sock".to_string(),
        "--chunk".to_string(),
        "4K".to_string(),
        "--ring-cap".to_string(),
        "4K".to_string(),
        "--iters".to_string(),
        "10".to_string(),
    ];
    let parsed = parse_peer_args(&ok).expect("完整参数应解析成功");
    assert_eq!(parsed.transport, Transport::Uds);
    assert_eq!(parsed.mode, Mode::RttClient);
    assert_eq!(parsed.chunk, 4096);
    assert_eq!(parsed.ring_cap, 4096, "RTT 下环容量必须等于消息大小");
    assert_eq!(parsed.endpoint, Endpoint::Connect("/tmp/x.sock".to_string()));

    let base = |extra: &[&str]| -> Vec<String> {
        let mut v = vec!["--mode".to_string(), "send".to_string()];
        v.extend(extra.iter().map(|s| s.to_string()));
        v
    };
    // 缺 --transport：只给 --mode 与 --connect。
    assert!(parse_peer_args(&base(&["--connect", "127.0.0.1:1"])).is_err(), "缺 --transport");
    // 缺端点：给齐 --transport 与 --mode。
    assert!(
        parse_peer_args(&base(&["--transport", "tcp"])).is_err(),
        "缺 --listen/--connect"
    );

    let rtt = vec![
        "--transport".to_string(),
        "tcp".to_string(),
        "--mode".to_string(),
        "rtt-server".to_string(),
        "--listen".to_string(),
        "127.0.0.1:0".to_string(),
        "--chunk".to_string(),
        "4K".to_string(),
        "--ring-cap".to_string(),
        "64K".to_string(),
    ];
    let err = parse_peer_args(&rtt).expect_err("RTT 下 ring-cap 不等于 chunk 应被拒绝");
    assert!(
        err.contains("ring-cap == chunk"),
        "错误信息应说明「一条消息恰好填满一个环段」：{err}"
    );

    // RTT 下把 ring-cap 设成与 chunk 相同即可通过。
    let rtt_ok = vec![
        "--transport".to_string(),
        "tcp".to_string(),
        "--mode".to_string(),
        "rtt-server".to_string(),
        "--listen".to_string(),
        "127.0.0.1:0".to_string(),
        "--chunk".to_string(),
        "4K".to_string(),
        "--ring-cap".to_string(),
        "4K".to_string(),
    ];
    assert!(parse_peer_args(&rtt_ok).is_ok(), "ring-cap == chunk 应当通过");

    let zero_chunk = vec![
        "--transport".to_string(),
        "tcp".to_string(),
        "--mode".to_string(),
        "recv".to_string(),
        "--listen".to_string(),
        "127.0.0.1:0".to_string(),
        "--chunk".to_string(),
        "0".to_string(),
    ];
    assert!(parse_peer_args(&zero_chunk).is_err(), "chunk=0 应被拒绝");
}

/// 测试编排器参数解析与用例展开。
/// - 手段：先解析一组编排器参数，再用 `plan_cases` 展开用例。
/// - 判断：解析出的列表与传入一致；用例数 = 传输数 × 粒度数 × 2 个方向，
///   且每个用例的被动方都是主动方的另一侧。
#[test]
fn harness_plan_() {
    let args = vec![
        "--backends".to_string(),
        "buffex,raw".to_string(),
        "--transports".to_string(),
        "tcp,uds".to_string(),
        "--chunks".to_string(),
        "4K,64K".to_string(),
        "--rtt-sizes".to_string(),
        "64".to_string(),
        "--rtt-iters".to_string(),
        "5".to_string(),
        "--skip-throughput".to_string(),
    ];
    let opts = parse_harness_args(&args).expect("参数应解析成功");
    assert_eq!(opts.transports.len(), 2);
    assert_eq!(opts.backends, vec![Backend::Buffex, Backend::Raw]);
    assert!(opts.skip_throughput);

    let cases = plan_cases(&opts);
    // 2 组后端 × 2 种传输 × 1 个 RTT 粒度 × 2 个方向。
    assert_eq!(cases.len(), 8);
    for c in &cases {
        assert_eq!(c.kind, CaseKind::Rtt);
        assert_eq!(c.server, c.client.peer(), "被动方应是主动方的另一侧");
        assert_eq!(c.iters, 5);
        // 环容量只对 buffex 组有意义：buffex 必须 ring == chunk；直连组保持 --ring-cap。
        if c.backend == Backend::Buffex {
            assert_eq!(c.ring_cap, c.chunk, "buffex 的 RTT 必须让一条消息恰好填满环段");
        } else {
            assert_eq!(c.ring_cap, opts.ring_cap, "直连组应忽略「一条消息填满环」的约束");
        }
    }
    // 两组后端各占一半，且用例顺序上先 buffex 后 raw。
    assert_eq!(cases.iter().filter(|c| c.backend == Backend::Buffex).count(), 4);
    assert_eq!(cases.iter().filter(|c| c.backend == Backend::Raw).count(), 4);

    let opts2 = HarnessOptions {
        backends: vec![Backend::Buffex],
        transports: vec![Transport::Tcp],
        chunks: vec![4096, 8192],
        skip_rtt: true,
        ..HarnessOptions::default()
    };
    let cases2 = plan_cases(&opts2);
    // 1 组后端 × 1 种传输 × 2 个粒度 × 2 个方向。
    assert_eq!(cases2.len(), 4);
    assert!(cases2.iter().all(|c| c.client != c.server));
    assert!(cases2.iter().all(|c| c.backend == Backend::Buffex));
}

/// 测试命令行里「后端」的解析与标签。
/// - 手段：`Backend::parse` 解析 `buffex` / `raw` / `direct` 与非法取值，并读一遍
///   两个后端的报告标签。
/// - 判断：合法取值映射到对应枚举；非法取值返回 `Err`；标签分别为「buffex 环」与
///   「直连（对照组）」——报告里两组必须能一眼分开。
#[test]
fn backend_parsing_() {
    assert_eq!(Backend::parse("buffex").expect("buffex"), Backend::Buffex);
    assert_eq!(Backend::parse("raw").expect("raw"), Backend::Raw);
    assert_eq!(Backend::parse("direct").expect("direct"), Backend::Raw);
    assert!(Backend::parse("ring").is_err());
    assert_eq!(Backend::Buffex.name(), "buffex");
    assert_eq!(Backend::Raw.name(), "raw");
    assert_eq!(Backend::Buffex.label(), "buffex 环");
    assert_eq!(Backend::Raw.label(), "直连（对照组）");
}

/// 测试对端参数里 `--backend` 的默认值与渲染。
/// - 手段：不带 `--backend` 解析一次（应默认 buffex），带 `--backend raw` 解析一次；
///   再用 `peer_args` 渲染成命令行。
/// - 判断：默认是 `Buffex`；显式给 `raw` 时解析为 `Raw`；渲染结果里含
///   `--backend raw`（保证编排器真的把后端传给了对端）。
#[test]
fn peer_backend_flag_() {
    let base = |extra: &[&str]| -> Vec<String> {
        let mut v = vec![
            "--transport".to_string(),
            "uds".to_string(),
            "--mode".to_string(),
            "recv".to_string(),
            "--listen".to_string(),
            "/tmp/x.sock".to_string(),
        ];
        v.extend(extra.iter().map(|s| s.to_string()));
        v
    };
    let default_parsed = parse_peer_args(&base(&[])).expect("默认应可解析");
    assert_eq!(default_parsed.backend, Backend::Buffex, "缺省后端应是 buffex");

    let raw_parsed = parse_peer_args(&base(&["--backend", "raw"])).expect("raw 应可解析");
    assert_eq!(raw_parsed.backend, Backend::Raw);
    let rendered = buffex_sock_uds_demo::runner::peer_args(&raw_parsed);
    assert!(
        rendered.windows(2).any(|w| w[0] == "--backend" && w[1] == "raw"),
        "渲染出的命令行里应带上 `--backend raw`：{rendered:?}"
    );

    // 直连 RTT 不受「ring-cap == chunk」约束（那个约束只对环成立）。
    let raw_rtt = base(&["--backend", "raw", "--mode", "rtt-client", "--ring-cap", "64K"]);
    let mut raw_rtt = raw_rtt;
    raw_rtt.extend(["--connect".to_string(), "/tmp/x.sock".to_string()]);
    // `--listen` 与 `--connect` 同时给会以最后一个为准，这里只关心解析成功。
    assert!(
        parse_peer_args(&raw_rtt).is_ok(),
        "直连 RTT 不应被「ring-cap == chunk」拒绝"
    );
}

/// 测试 RTT 分位数的取法。
/// - 手段：对 `[5, 1, 4, 2, 3]` 取 `q=0.0`、`0.5`、`1.0`，再对空输入取一次。
/// - 判断：排序后分别得到最小、中位、最大；空输入返回 0（不 panic）。
#[test]
fn percentile_() {
    let samples = vec![5u64, 1, 4, 2, 3];
    assert_eq!(percentile_ns(samples.clone(), 0.0), 1);
    assert_eq!(percentile_ns(samples.clone(), 0.5), 3);
    assert_eq!(percentile_ns(samples, 1.0), 5);
    assert_eq!(percentile_ns(Vec::new(), 0.5), 0);
}

/// 测试对端参数的默认值符合文档口径。
/// - 手段：用最小合法参数构造 `PeerOptions`（只给必填项）。
/// - 判断：`bytes` / `chunk` / `iters` / `ring_cap` / `nodelay` 与文档一致。
#[test]
fn peer_defaults_() {
    let args = vec![
        "--transport".to_string(),
        "tcp".to_string(),
        "--mode".to_string(),
        "send".to_string(),
        "--connect".to_string(),
        "127.0.0.1:1".to_string(),
    ];
    let o = parse_peer_args(&args).expect("最小参数应可解析");
    let d = PeerOptions::default();
    assert_eq!(o.bytes, d.bytes);
    assert_eq!(o.chunk, d.chunk);
    assert_eq!(o.iters, d.iters);
    assert_eq!(o.ring_cap, d.ring_cap);
    assert!(o.nodelay);
    assert_eq!(PeerRole::Tokio.peer(), PeerRole::Compio);
}
