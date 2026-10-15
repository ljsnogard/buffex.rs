//! 结果结构与其 JSON 行编解码。
//!
//! 每个对端在退出前往 stdout 打一行 `{"event":"result", ...}`，编排器读这行
//! 汇总。协议是**扁平** JSON（值只有字符串、数字、布尔），因此这里手写一个
//! 几十行的解析器就够了，不必引入 `serde`。

use std::time::Duration;

/// 一个对端进程跑完一轮后的结果。
#[derive(Debug, Clone, PartialEq)]
pub struct PeerResult {
    /// 对端身份：`tokio` 或 `compio`。
    pub role: String,
    /// 收发后端：`buffex`（适配器）或 `raw`（直连对照组）。
    pub backend: String,
    /// 传输方式：`tcp` 或 `uds`。
    pub transport: String,
    /// 角色：`send` / `recv` / `rtt-client` / `rtt-server`。
    pub mode: String,
    /// 计入口径的负载字节数（不含协议头部）。
    pub bytes: u64,
    /// 单条消息 / 单次借段的字节数。
    pub chunk: usize,
    /// RTT 往返次数（吞吐模式为 0）。
    pub iters: u64,
    /// 适配器内部环容量。
    pub ring_cap: usize,
    /// 计时窗口长度（纳秒）。
    pub elapsed_ns: u64,
    /// 吞吐（MiB/s），`bytes / elapsed`。
    pub mib_per_s: f64,
    /// 本端对 payload 计算的滚动校验和。
    pub checksum: u64,
    /// 实际收发 / 回显的字节数。
    pub received: u64,
    /// 本端是否正常收尾。
    pub ok: bool,
    /// RTT 的 p50（纳秒；非 RTT 模式为 0）。
    pub p50_ns: u64,
    /// RTT 的 p99（纳秒；非 RTT 模式为 0）。
    pub p99_ns: u64,
    /// RTT 的算术平均（纳秒；非 RTT 模式为 0）。
    pub avg_ns: u64,
    /// 附注（错误说明、RTT 均值等）。
    pub note: String,
}

impl Default for PeerResult {
    fn default() -> Self {
        PeerResult {
            role: String::new(),
            backend: String::new(),
            transport: String::new(),
            mode: String::new(),
            bytes: 0,
            chunk: 0,
            iters: 0,
            ring_cap: 0,
            elapsed_ns: 0,
            mib_per_s: 0.0,
            checksum: 0,
            received: 0,
            ok: false,
            p50_ns: 0,
            p99_ns: 0,
            avg_ns: 0,
            note: String::new(),
        }
    }
}

/// 计算 MiB/s。
pub fn mib_per_sec(bytes: u64, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64();
    if secs <= 0.0 {
        return 0.0;
    }
    bytes as f64 / (1024.0 * 1024.0) / secs
}

/// 计算 RTT 分位数（输入会被排序）。
///
/// `q` 取 `0.0..=1.0`；空输入返回 0。
pub fn percentile_ns(mut samples: Vec<u64>, q: f64) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    samples.sort_unstable();
    let q = q.clamp(0.0, 1.0);
    let idx = ((samples.len() - 1) as f64 * q).round() as usize;
    samples[idx]
}

/// 扁平 JSON 的值。
#[derive(Debug, Clone, PartialEq)]
pub enum JsonValue {
    /// 字符串值。
    Str(String),
    /// 数字值，**保留原始文本**：校验和是 `u64`，先过 `f64` 会丢精度，
    /// 因此取值时按需分别解析成整数或浮点。
    Num(String),
    /// 布尔值。
    Bool(bool),
}

/// 转义 JSON 字符串里的特殊字符。
pub fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

impl PeerResult {
    /// 序列化成一行 JSON（`event` 固定为 `result`）。
    pub fn to_json_line(&self) -> String {
        format!(
            "{{\"event\":\"result\",\"role\":\"{}\",\"backend\":\"{}\",\"transport\":\"{}\",\"mode\":\"{}\",\
             \"bytes\":{},\"chunk\":{},\"iters\":{},\"ring_cap\":{},\"elapsed_ns\":{},\
             \"mib_per_s\":{},\"checksum\":{},\"received\":{},\"ok\":{},\
             \"p50_ns\":{},\"p99_ns\":{},\"avg_ns\":{},\"note\":\"{}\"}}",
            json_escape(&self.role),
            json_escape(&self.backend),
            json_escape(&self.transport),
            json_escape(&self.mode),
            self.bytes,
            self.chunk,
            self.iters,
            self.ring_cap,
            self.elapsed_ns,
            finite_or_zero_(self.mib_per_s),
            self.checksum,
            self.received,
            self.ok,
            self.p50_ns,
            self.p99_ns,
            self.avg_ns,
            json_escape(&self.note),
        )
    }

    /// 从一行 JSON 解析回来。
    ///
    /// # Errors
    ///
    /// 不是合法扁平 JSON、或缺少必要字段时返回错误说明。
    pub fn from_json_line(s: &str) -> Result<Self, String> {
        let fields = json_parse_flat(s)?;
        let get_str = |k: &str| -> Result<String, String> {
            match fields.iter().find(|(name, _)| name == k) {
                Some((_, JsonValue::Str(v))) => Ok(v.clone()),
                Some((_, other)) => Err(format!("字段 `{k}` 不是字符串：{other:?}")),
                None => Err(format!("缺少字段 `{k}`")),
            }
        };
        let get_u64 = |k: &str| -> Result<u64, String> {
            match fields.iter().find(|(name, _)| name == k) {
                Some((_, JsonValue::Num(text))) => text
                    .parse::<u64>()
                    .map_err(|e| format!("字段 `{k}` 不是非负整数：{e}")),
                Some((_, other)) => Err(format!("字段 `{k}` 不是数字：{other:?}")),
                None => Err(format!("缺少字段 `{k}`")),
            }
        };
        let get_f64 = |k: &str| -> Result<f64, String> {
            match fields.iter().find(|(name, _)| name == k) {
                Some((_, JsonValue::Num(text))) => text
                    .parse::<f64>()
                    .map_err(|e| format!("字段 `{k}` 不是浮点数：{e}")),
                Some((_, other)) => Err(format!("字段 `{k}` 不是数字：{other:?}")),
                None => Err(format!("缺少字段 `{k}`")),
            }
        };
        let get_bool = |k: &str| -> Result<bool, String> {
            match fields.iter().find(|(name, _)| name == k) {
                Some((_, JsonValue::Bool(v))) => Ok(*v),
                Some((_, other)) => Err(format!("字段 `{k}` 不是布尔：{other:?}")),
                None => Err(format!("缺少字段 `{k}`")),
            }
        };
        Ok(PeerResult {
            role: get_str("role")?,
            backend: get_str("backend")?,
            transport: get_str("transport")?,
            mode: get_str("mode")?,
            bytes: get_u64("bytes")?,
            chunk: get_u64("chunk")? as usize,
            iters: get_u64("iters")?,
            ring_cap: get_u64("ring_cap")? as usize,
            elapsed_ns: get_u64("elapsed_ns")?,
            mib_per_s: get_f64("mib_per_s")?,
            checksum: get_u64("checksum")?,
            received: get_u64("received")?,
            ok: get_bool("ok")?,
            p50_ns: get_u64("p50_ns")?,
            p99_ns: get_u64("p99_ns")?,
            avg_ns: get_u64("avg_ns")?,
            note: get_str("note")?,
        })
    }
}

/// 解析一行**扁平** JSON 对象，返回按出现顺序排列的字段。
///
/// # Errors
///
/// 语法不合法时返回带下标的错误说明。
pub fn json_parse_flat(s: &str) -> Result<Vec<(String, JsonValue)>, String> {
    let b = s.as_bytes();
    let mut i = 0usize;
    let mut out = Vec::new();
    skip_ws_(b, &mut i);
    expect_(b, &mut i, b'{')?;
    skip_ws_(b, &mut i);
    if peek_(b, i) == Some(b'}') {
        return Ok(out);
    }
    loop {
        skip_ws_(b, &mut i);
        let key = parse_json_str_(b, &mut i)?;
        skip_ws_(b, &mut i);
        expect_(b, &mut i, b':')?;
        skip_ws_(b, &mut i);
        let val = parse_json_val_(b, &mut i)?;
        out.push((key, val));
        skip_ws_(b, &mut i);
        match peek_(b, i) {
            Some(b',') => i += 1,
            Some(b'}') => {
                i += 1;
                break;
            }
            other => {
                return Err(format!(
                    "下标 {i} 处期望 `,` 或 `}}`，实际 {other:?}"
                ))
            }
        }
    }
    skip_ws_(b, &mut i);
    if i != b.len() {
        return Err(format!("下标 {i} 之后还有多余内容"));
    }
    Ok(out)
}

fn peek_(b: &[u8], i: usize) -> Option<u8> {
    b.get(i).copied()
}

fn skip_ws_(b: &[u8], i: &mut usize) {
    while let Some(c) = peek_(b, *i) {
        if c.is_ascii_whitespace() {
            *i += 1;
        } else {
            break;
        }
    }
}

fn expect_(b: &[u8], i: &mut usize, want: u8) -> Result<(), String> {
    match peek_(b, *i) {
        Some(c) if c == want => {
            *i += 1;
            Ok(())
        }
        other => Err(format!(
            "下标 {} 处期望 `{}`，实际 {:?}",
            *i, want as char, other
        )),
    }
}

fn parse_json_str_(b: &[u8], i: &mut usize) -> Result<String, String> {
    expect_(b, i, b'"')?;
    // 按**字节**收集再统一转 UTF-8：逐字节 `as char` 会把多字节字符拆坏。
    let mut out: Vec<u8> = Vec::new();
    loop {
        let c = peek_(b, *i).ok_or_else(|| format!("下标 {} 处字符串未闭合", *i))?;
        *i += 1;
        match c {
            b'"' => {
                return String::from_utf8(out)
                    .map_err(|e| format!("字符串不是合法 UTF-8：{e}"))
            }
            b'\\' => {
                let e = peek_(b, *i).ok_or_else(|| "转义序列未完成".to_string())?;
                *i += 1;
                match e {
                    b'"' => out.push(b'"'),
                    b'\\' => out.push(b'\\'),
                    b'/' => out.push(b'/'),
                    b'n' => out.push(b'\n'),
                    b'r' => out.push(b'\r'),
                    b't' => out.push(b'\t'),
                    b'b' => out.push(0x08),
                    b'f' => out.push(0x0c),
                    b'u' => {
                        let mut code = parse_hex4_(b, i)?;
                        // 代理对：高位后面必须跟一个低位 `\uDC00..\uE000`。
                        if (0xd800..0xdc00).contains(&code) {
                            expect_(b, i, b'\\')?;
                            expect_(b, i, b'u')?;
                            let low = parse_hex4_(b, i)?;
                            if !(0xdc00..0xe000).contains(&low) {
                                return Err(format!("非法代理对：\\u{code:04x}\\u{low:04x}"));
                            }
                            code = 0x1_0000 + ((code - 0xd800) << 10) + (low - 0xdc00);
                        }
                        let ch = char::from_u32(code)
                            .ok_or_else(|| format!("非法码点 \\u{code:04x}"))?;
                        let mut buf = [0u8; 4];
                        out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                    }
                    other => {
                        return Err(format!("无法识别的转义 `\\{}`", other as char))
                    }
                }
            }
            c => out.push(c),
        }
    }
}

/// 读 4 个十六进制字符。
fn parse_hex4_(b: &[u8], i: &mut usize) -> Result<u32, String> {
    let end = *i + 4;
    if end > b.len() {
        return Err(format!("下标 {} 处 \\u 转义不完整", *i));
    }
    let text = core::str::from_utf8(&b[*i..end])
        .map_err(|e| format!("\\u 转义不是 UTF-8：{e}"))?;
    let code = u32::from_str_radix(text, 16)
        .map_err(|e| format!("\\u 转义 `{text}` 无法解析：{e}"))?;
    *i = end;
    Ok(code)
}

/// 非有限值（NaN / inf）写成 0，保证输出仍是合法 JSON。
fn finite_or_zero_(v: f64) -> f64 {
    if v.is_finite() { v } else { 0.0 }
}

fn parse_json_val_(b: &[u8], i: &mut usize) -> Result<JsonValue, String> {
    match peek_(b, *i) {
        Some(b'"') => Ok(JsonValue::Str(parse_json_str_(b, i)?)),
        Some(b't') => {
            if b[*i..].starts_with(b"true") {
                *i += 4;
                Ok(JsonValue::Bool(true))
            } else {
                Err(format!("下标 {} 处期望 true", *i))
            }
        }
        Some(b'f') => {
            if b[*i..].starts_with(b"false") {
                *i += 5;
                Ok(JsonValue::Bool(false))
            } else {
                Err(format!("下标 {} 处期望 false", *i))
            }
        }
        Some(_) => {
            let start = *i;
            while let Some(c) = peek_(b, *i) {
                if c.is_ascii_digit() || matches!(c, b'-' | b'+' | b'.' | b'e' | b'E') {
                    *i += 1;
                } else {
                    break;
                }
            }
            if start == *i {
                return Err(format!("下标 {start} 处期望一个值"));
            }
            let text = core::str::from_utf8(&b[start..*i])
                .map_err(|e| format!("下标 {start} 处数字不是 UTF-8：{e}"))?;
            text.parse::<f64>()
                .map(|_| JsonValue::Num(text.to_string()))
                .map_err(|e| format!("数字 `{text}` 无法解析：{e}"))
        }
        None => Err("输入意外结束".to_string()),
    }
}
