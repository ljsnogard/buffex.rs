//! 两个进程之间的**极简协议**：定长头部 + 负载。
//!
//! 协议本身不是本实验的被测对象——它只解决一个问题：收端怎么知道「该收多少
//! 字节才算一轮结束」，从而把计时窗口钉在**纯数据搬运**上（不把对端启动、
//! 关闭握手算进吞吐）。
//!
//! ```text
//! 吞吐模式： [ 8 字节总长(小端 u64) ][ total 个负载字节 ]
//! RTT  模式： [ chunk 字节请求 ] [ chunk 字节回显 ] × iters（消息长度由两侧
//!            命令行约定，不带头部，省掉每轮的分帧开销）
//! ```

/// 总长头部占用的字节数（小端 `u64`）。
pub const HEADER_LEN: usize = 8;

/// 负载模式的周期：第 `i` 个字节为 `(i % 251) as u8`（251 是质数，避免与
/// 常见缓冲大小对齐而看不出错位）。
pub const PATTERN_PERIOD: u64 = 251;

/// 把总负载长度编码成 8 字节小端头部。
pub fn encode_header(total: u64) -> [u8; HEADER_LEN] {
    total.to_le_bytes()
}

/// 从 8 字节小端头部解出总负载长度。
pub fn decode_header(buf: &[u8; HEADER_LEN]) -> u64 {
    u64::from_le_bytes(*buf)
}

/// 确定性负载：负载第 `i` 个字节的取值。
pub fn pattern_byte(i: u64) -> u8 {
    (i % PATTERN_PERIOD) as u8
}

/// 用 [`pattern_byte`] 填充 `buf`，`base` 是 `buf[0]` 在整段负载中的绝对下标。
pub fn fill_pattern(buf: &mut [u8], base: u64) {
    for (k, b) in buf.iter_mut().enumerate() {
        *b = pattern_byte(base + k as u64);
    }
}

/// 生成一块 `len` 字节的确定性负载（从绝对下标 0 开始）。
pub fn pattern_buf(len: usize) -> Vec<u8> {
    let mut buf = vec![0u8; len];
    fill_pattern(&mut buf, 0);
    buf
}
