//! 滚动校验和：按小端 `u64` 分组做回绕累加。
//!
//! 用途是**收发两侧对账**（确认 benchmark 期间没有丢字节 / 串序），不是密码学
//! 摘要，因此选了吞吐远高于逐字节 FNV 的实现：每 8 字节一次读 + 一次加法，
//! 在内存带宽量级上运行，不会成为百 MiB/s 级搬运的瓶颈。
//!
//! 支持分块喂入（`update` 可任意次调用，块长度不必对齐 8 字节），尾部余数会
//! 被暂存到 `finish` 时统一折算。

/// 滚动校验和的状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RollingSum64 {
    /// 已折算进校验和的 `u64` 字之和。
    acc_: u64,
    /// 尚未凑满 8 字节的尾部字节。
    tail_: [u8; 8],
    /// 尾部已存字节数（`0..8`）。
    tail_len_: usize,
}

impl RollingSum64 {
    /// 新建全零状态。
    pub const fn new() -> Self {
        RollingSum64 {
            acc_: 0,
            tail_: [0u8; 8],
            tail_len_: 0,
        }
    }

    /// 喂入一段字节（长度任意，可多次调用）。
    pub fn update(&mut self, mut bytes: &[u8]) {
        // 先把上次剩下的尾部补齐。
        if self.tail_len_ > 0 {
            let need = 8 - self.tail_len_;
            let take = need.min(bytes.len());
            self.tail_[self.tail_len_..self.tail_len_ + take]
                .copy_from_slice(&bytes[..take]);
            self.tail_len_ += take;
            bytes = &bytes[take..];
            if self.tail_len_ == 8 {
                self.acc_ = self.acc_.wrapping_add(u64::from_le_bytes(self.tail_));
                self.tail_len_ = 0;
            }
        }

        // 不用 clippy 建议的 `as_chunks::<8>()`：那是尚未稳定的 `slice_as_chunks`
        // feature，而本 crate 自身不需要再引入 nightly feature（nightly 只是依赖链
        // 的要求）。按 8 字节分组 + `remainder()` 的写法语义完全一致。
        #[allow(clippy::chunks_exact_to_as_chunks)]
        let mut chunks = bytes.chunks_exact(8);
        for c in &mut chunks {
            // 显式下标取值：`chunks_exact(8)` 保证长度恰好 8，且不引入 `unwrap`。
            let word = u64::from_le_bytes([
                c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7],
            ]);
            self.acc_ = self.acc_.wrapping_add(word);
        }
        let rem = chunks.remainder();
        // 只有真的取到余数时才改写尾部：否则「把上次的尾巴补齐后再没剩字节」这一
        // 步会被误清空（逐字节喂入时必现）。
        if !rem.is_empty() {
            self.tail_[..rem.len()].copy_from_slice(rem);
            self.tail_len_ = rem.len();
        }
    }

    /// 收尾并给出校验和。
    pub fn finish(&self) -> u64 {
        let mut word = [0u8; 8];
        word[..self.tail_len_].copy_from_slice(&self.tail_[..self.tail_len_]);
        let acc = self.acc_.wrapping_add(u64::from_le_bytes(word));
        // 混入尾部长度，避免「总长不同但补齐后同值」的平凡碰撞。
        acc ^ (self.tail_len_ as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)
    }
}

impl Default for RollingSum64 {
    fn default() -> Self {
        Self::new()
    }
}
