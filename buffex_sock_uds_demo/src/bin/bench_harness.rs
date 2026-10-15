//! 编排器：拉起两个对端进程、跑用例矩阵、打印表格并写报告。
//!
//! 它**不碰业务字节**：只负责决定监听端 / 连接端、握手、收集两边打出来的
//! `{"event":"result", ...}`，再渲染成控制台表格与 markdown 报告。

use std::{path::PathBuf, process::ExitCode};

use buffex_sock_uds_demo::{
    cli::{HARNESS_HELP, HarnessOptions, parse_harness_args},
    report,
    runner::{
        CaseOutcome, PeerRole, default_peer_dir, peer_binary_path, plan_cases, run_case,
    },
};

/// 解析参数；`--help` 时打印帮助并正常退出。
fn parse_args_() -> Result<Option<HarnessOptions>, String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match parse_harness_args(&args) {
        Ok(o) => Ok(Some(o)),
        Err(e) if e == "HARNESS_HELP" => {
            println!("{HARNESS_HELP}");
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

/// 校验两个对端二进制都在，并提醒「对端比编排器旧」的情况。
///
/// 对端与编排器共享 `metrics` 的 JSON 行格式：如果只重新构建了编排器（例如
/// `cargo run --bin bench_harness`），旧对端打出的结果行可能缺少新字段，表现为
/// 「对端没有给出结果行」。这里按 mtime 提示一句，省得踩坑。
fn ensure_peers_(peer_dir: &std::path::Path) -> Result<(), String> {
    let harness_mtime = std::env::current_exe()
        .ok()
        .and_then(|p| std::fs::metadata(p).ok())
        .and_then(|m| m.modified().ok());
    for role in [PeerRole::Tokio, PeerRole::Compio] {
        let p = peer_binary_path(peer_dir, role);
        if !p.exists() {
            return Err(format!(
                "找不到对端二进制 `{}`；请先 `cargo build`（或用 --peer-dir 指定目录）",
                p.display()
            ));
        }
        let peer_mtime = std::fs::metadata(&p).ok().and_then(|m| m.modified().ok());
        if let (Some(h), Some(peer)) = (harness_mtime, peer_mtime)
            && peer < h
        {
            eprintln!(
                "警告：对端 `{}` 比 bench_harness 旧，可能版本不一致（建议先 `cargo build --release`）",
                p.display()
            );
        }
    }
    Ok(())
}

/// 跑完整个矩阵，返回 `(用例结果, 失败说明)`。
fn run_matrix_(opts: &HarnessOptions, peer_dir: &std::path::Path) -> (Vec<CaseOutcome>, Vec<String>) {
    let cases = plan_cases(opts);
    let total = cases.len();
    let mut outcomes: Vec<CaseOutcome> = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    if total == 0 {
        return (outcomes, failures);
    }

    for (idx, spec) in cases.iter().enumerate() {
        let mut best: Option<CaseOutcome> = None;
        for rep in 0..opts.repeats {
            eprintln!(
                "[{}/{}] {} {} {}（第 {}/{} 次）……",
                idx + 1,
                total,
                spec.kind.name(),
                spec.transport.name(),
                spec.direction(),
                rep + 1,
                opts.repeats,
            );
            let seq = idx as u64 * 1000 + rep as u64;
            match run_case(peer_dir, spec, seq) {
                Ok(o) => {
                    eprintln!(
                        "        发送 {:.1} MiB/s，接收 {:.1} MiB/s，p50 {}，校验和 {}",
                        o.client.mib_per_s,
                        o.server.mib_per_s,
                        report::ns_human(o.client.p50_ns),
                        if o.checksum_agrees() { "一致" } else { "**不一致**" },
                    );
                    let better = match &best {
                        Some(b) => o.client.mib_per_s > b.client.mib_per_s,
                        None => true,
                    };
                    if better {
                        best = Some(o);
                    }
                }
                Err(e) => {
                    let msg = format!(
                        "{} {} {}（第 {} 次）：{e}",
                        spec.kind.name(),
                        spec.transport.name(),
                        spec.direction(),
                        rep + 1,
                    );
                    eprintln!("        ! {msg}");
                    failures.push(msg);
                }
            }
        }
        if let Some(o) = best {
            outcomes.push(o);
        }
    }
    (outcomes, failures)
}

fn main() -> ExitCode {
    let opts = match parse_args_() {
        Ok(Some(o)) => o,
        Ok(None) => return ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("bench_harness 参数错误：{e}\n\n{HARNESS_HELP}");
            return ExitCode::from(2);
        }
    };

    let peer_dir: PathBuf = match &opts.peer_dir {
        Some(d) => d.clone(),
        None => match default_peer_dir() {
            Ok(d) => d,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        },
    };
    if let Err(e) = ensure_peers_(&peer_dir) {
        eprintln!("{e}");
        return ExitCode::FAILURE;
    }

    let (outcomes, failures) = run_matrix_(&opts, &peer_dir);

    print!("{}", report::render_console(&outcomes));

    let out_path = match &opts.out {
        Some(p) => p.clone(),
        None => PathBuf::from(format!("results/bench-{}.md", report::now_stamp())),
    };
    if let Some(dir) = out_path.parent()
        && !dir.as_os_str().is_empty()
        && let Err(e) = std::fs::create_dir_all(dir)
    {
        eprintln!("创建报告目录 `{}` 失败：{e}", dir.display());
        return ExitCode::FAILURE;
    }

    let md = report::render_markdown(&outcomes);
    if let Err(e) = std::fs::write(&out_path, md) {
        eprintln!("写报告 `{}` 失败：{e}", out_path.display());
        return ExitCode::FAILURE;
    }
    let jsonl_path = out_path.with_extension("jsonl");
    if let Err(e) = std::fs::write(&jsonl_path, report::render_jsonl(&outcomes)) {
        eprintln!("写原始记录 `{}` 失败：{e}", jsonl_path.display());
        return ExitCode::FAILURE;
    }

    println!("\nmarkdown 报告：{}", out_path.display());
    println!("原始结果：    {}", jsonl_path.display());
    if !failures.is_empty() {
        eprintln!("\n有 {} 个用例失败：", failures.len());
        for f in &failures {
            eprintln!("  - {f}");
        }
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
