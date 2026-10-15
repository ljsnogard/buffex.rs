//! 把一组用例结果渲染成控制台表格 / markdown 报告 / JSONL 原始记录。

use std::time::{SystemTime, UNIX_EPOCH};

use crate::runner::{CaseKind, CaseOutcome};

/// 当前 UTC 时间的 ISO-8601 表示（`YYYY-MM-DDThh:mm:ssZ`）。
///
/// 不引 `chrono`：只需一个「人类可读的时间戳」，用经典的
/// days-to-civil 算法把 UNIX 秒换算成日期即可。
pub fn now_iso8601() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days_(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// 文件系统友好的时间戳（`YYYYMMDD-hhmmss`），用于报告文件名。
pub fn now_stamp() -> String {
    let iso = now_iso8601();
    let date = iso[0..10].replace('-', "");
    let time = iso[11..19].replace(':', "");
    format!("{date}-{time}")
}

/// Howard Hinnant 的 `civil_from_days`：把「1970-01-01 起的天数」换算成
/// `(年, 月, 日)`。
fn civil_from_days_(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// 纳秒 → 人类可读的短串（用于 RTT 列）。
pub fn ns_human(ns: u64) -> String {
    if ns < 1_000 {
        format!("{ns} ns")
    } else if ns < 1_000_000 {
        format!("{:.2} µs", ns as f64 / 1_000.0)
    } else {
        format!("{:.3} ms", ns as f64 / 1_000_000.0)
    }
}

/// 秒 → 人类可读的短串。
pub fn secs_human(d: std::time::Duration) -> String {
    let s = d.as_secs_f64();
    if s < 1.0 {
        format!("{:.1} ms", s * 1000.0)
    } else {
        format!("{s:.3} s")
    }
}

/// 渲染控制台表格。
pub fn render_console(outcomes: &[CaseOutcome]) -> String {
    let mut s = String::new();
    s.push_str(&format!(
        "{:<16} {:<5} {:<20} {:<10} {:>12} {:>12} {:>10} {:>8}\n",
        "后端", "传输", "方向", "粒度", "发送 MiB/s", "接收 MiB/s", "校验和", "结论"
    ));
    for o in outcomes {
        let (tx, rx) = throughput_pair(o);
        s.push_str(&format!(
            "{:<16} {:<5} {:<20} {:<10} {:>12.1} {:>12.1} {:>10} {:>8}\n",
            o.spec.backend_label(),
            o.spec.transport.name(),
            o.spec.direction(),
            size_human(o.spec.chunk),
            tx,
            rx,
            if o.checksum_agrees() { "一致" } else { "不一致" },
            if o.ok() { "ok" } else { "FAIL" },
        ));
    }
    s
}

/// 吞吐口径：单向用时延的「客户端发送端 / 服务端接收端」。
fn throughput_pair(o: &CaseOutcome) -> (f64, f64) {
    match o.spec.kind {
        CaseKind::Throughput => (o.client.mib_per_s, o.server.mib_per_s),
        CaseKind::Rtt => (o.client.mib_per_s, o.server.mib_per_s),
    }
}

/// 字节数 → 短串。
pub fn size_human(n: usize) -> String {
    if n.is_multiple_of(1024 * 1024) {
        format!("{} MiB", n / (1024 * 1024))
    } else if n.is_multiple_of(1024) {
        format!("{} KiB", n / 1024)
    } else {
        format!("{n} B")
    }
}

/// 渲染 markdown 报告。
pub fn render_markdown(outcomes: &[CaseOutcome]) -> String {
    let mut s = String::new();
    s.push_str("# buffex_tokio_adapt × buffex_compio_adapt 跨进程通信 benchmark\n\n");
    s.push_str(&format!("- 生成时间（UTC）：{}\n", now_iso8601()));
    s.push_str("- 拓扑：`bench_harness` 拉起两个独立进程（tokio 进程 / compio 进程），两者经 TCP 或 UNIX domain socket 互连。\n");
    s.push_str("- 两组对照：\n");
    s.push_str("  - **buffex 环**：tokio 进程只用 `buffex_tokio_adapt::BuffRead` / `BuffWrite`，compio 进程只用 `buffex_compio_adapt::BuffRead` / `BuffWrite`（设备 ↔ 环由适配器搬运）；\n");
    s.push_str("  - **直连（对照组）**：同样的两个进程、同样的协议与计时口径，但**不碰任何 buffex 相关 crate**，直接对 socket 做 `read` / `write`。\n");

    s.push_str("- 口径：MiB/s 按 **1024² 字节** 计；发送端从「写头部前」到「`close_async` 返回」计时，接收端从「读完头部」到「收满负载」计时。\n\n");

    // ---- 吞吐 ----
    s.push_str("## 1. 单向吞吐\n\n");
    s.push_str("| 后端 | 传输 | 方向 | 消息粒度 | 发送端 MiB/s | 接收端 MiB/s | 发送耗时 | 接收耗时 | 校验和 | 结论 |\n");
    s.push_str("| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |\n");
    for o in outcomes.iter().filter(|o| o.spec.kind == CaseKind::Throughput) {
        s.push_str(&format!(
            "| {} | {} | {} | {} | {:.1} | {:.1} | {} | {} | {} | {} |\n",
            o.spec.backend_label(),
            o.spec.transport.name(),
            o.spec.direction(),
            size_human(o.spec.chunk),
            o.client.mib_per_s,
            o.server.mib_per_s,
            secs_human(std::time::Duration::from_nanos(o.client.elapsed_ns)),
            secs_human(std::time::Duration::from_nanos(o.server.elapsed_ns)),
            if o.checksum_agrees() { "一致" } else { "**不一致**" },
            if o.ok() { "ok" } else { "**FAIL**" },
        ));
    }

    // ---- RTT ----
    s.push_str("\n## 2. 往返时延（RTT）\n\n");
    s.push_str("| 后端 | 传输 | 客户端 | 消息大小 | p50 | p99 | 平均 | 往返次数 | 客户端 MiB/s | 校验和 | 结论 |\n");
    s.push_str("| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |\n");
    for o in outcomes.iter().filter(|o| o.spec.kind == CaseKind::Rtt) {
        s.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {:.1} | {} | {} |\n",
            o.spec.backend_label(),
            o.spec.transport.name(),
            o.spec.direction(),
            size_human(o.spec.chunk),
            ns_human(o.client.p50_ns),
            ns_human(o.client.p99_ns),
            ns_human(o.client.avg_ns),
            o.spec.iters,
            o.client.mib_per_s,
            if o.checksum_agrees() { "一致" } else { "**不一致**" },
            if o.ok() { "ok" } else { "**FAIL**" },
        ));
    }

    // ---- 附注 ----
    s.push_str("\n## 3. 附注\n\n");
    if let (Some(min), Some(max)) = (
        outcomes.iter().map(|o| o.wall).min(),
        outcomes.iter().map(|o| o.wall).max(),
    ) {
        s.push_str(&format!(
            "- 单用例进程墙钟（含两个对端的启动、握手与退出）：最小 {}，最大 {}。\n",
            secs_human(min),
            secs_human(max),
        ));
    }
    let mut any_note = false;
    for o in outcomes {
        if !o.ok() {
            any_note = true;
            s.push_str(&format!(
                "- [{}] {} {} {}：客户端 `ok={}` note=`{}`；服务端 `ok={}` note=`{}`；校验和 {} / {}。\n",
                o.spec.backend_label(),
                o.spec.kind.name(),
                o.spec.transport.name(),
                o.spec.direction(),
                o.client.ok,
                o.client.note,
                o.server.ok,
                o.server.note,
                o.client.checksum,
                o.server.checksum,
            ));
        }
    }
    if !any_note {
        s.push_str("- 全部用例校验和一致、两侧正常收尾，无异常记录。\n");
    }

    // ---- 原始记录 ----
    s.push_str("\n## 4. 原始结果（每行一个 JSON）\n\n```json\n");
    for o in outcomes {
        s.push_str(&o.client.to_json_line());
        s.push('\n');
        s.push_str(&o.server.to_json_line());
        s.push('\n');
    }
    s.push_str("```\n");
    s
}

/// 渲染 JSONL 原始记录（每行：用例标签 + 一侧结果）。
pub fn render_jsonl(outcomes: &[CaseOutcome]) -> String {
    let mut s = String::new();
    for o in outcomes {
        s.push_str(&o.client.to_json_line());
        s.push('\n');
        s.push_str(&o.server.to_json_line());
        s.push('\n');
    }
    s
}
