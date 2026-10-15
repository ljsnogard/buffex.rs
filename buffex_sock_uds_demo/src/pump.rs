//! 与运行时无关的收发泵：只依赖 `abs_buff` 的 `TrBuffRead` / `TrBuffWrite`。
//!
//! tokio 侧（`buffex_tokio_adapt::BuffRead` / `BuffWrite`）与 compio 侧
//! （`buffex_compio_adapt::BuffRead` / `BuffWrite`）都实现同一套段 trait，因此
//! 实验里的搬运代码只写这一份：借段、搬字节、drop 段提交。**设备（socket）与
//! 环之间的搬运完全由适配器负责**，本模块一行设备 IO 都不写。
//!
//! ## 消费语义提醒
//!
//! * `iter_slices()` 只是窥视，不推进消费——本模块不用它；
//! * `move_items_to_buff` / `move_items_from_as_buff` 搬出多少就消费多少，
//!   段 drop 时按该量提交；
//! * 出向「段 drop」只代表提交进环，不代表送达设备：`Send` 侧收尾必须
//!   `flush_async` / `close_async`（见两个对端二进制）。

use core::mem::MaybeUninit;

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView},
};
use buffex::{
    ring::{ConsumerError, ProducerError},
    x_deps::abs_buff,
};

/// 收发过程中可能出现的失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PumpError {
    /// 写端已关闭（`ProducerError::Closing`）。
    WriteClosed,
    /// 写等待被取消（`ProducerError::Cancelled`）。
    WriteCancelled,
    /// 读端已关闭（`ConsumerError::Cancelled`）。
    ReadCancelled,
    /// 环报出「本轮请求不可能满足」。
    Unsatisfiable,
    /// 环报出了在 `Demand::at_least(1)` 下不该出现的状态（例如 `Stuffed`）。
    RingStuck(&'static str),
    /// 借到了段却搬出 0 个元素（正常路径不应发生）。
    ZeroMove,
    /// 负载还没搬够 `want` 字节对端就关闭了。
    UnexpectedEof {
        /// 期望的字节数。
        want: u64,
        /// 实际搬到的字节数。
        got: u64,
    },
}

impl core::fmt::Display for PumpError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            PumpError::WriteClosed => write!(f, "写端已关闭"),
            PumpError::WriteCancelled => write!(f, "写等待被取消"),
            PumpError::ReadCancelled => write!(f, "读等待被取消"),
            PumpError::Unsatisfiable => write!(f, "环上请求不可能满足"),
            PumpError::RingStuck(s) => write!(f, "环状态异常：{s}"),
            PumpError::ZeroMove => write!(f, "借到段却搬出 0 个元素"),
            PumpError::UnexpectedEof { want, got } => {
                write!(f, "对端提前关闭：期望 {want} 字节，实际 {got} 字节")
            }
        }
    }
}

impl std::error::Error for PumpError {}

/// 把 `payload` 全部写进写端。
///
/// 每次借段、`move_items_from_as_buff` 搬一部分、drop 段提交，直到整段写完。
/// 段可以小于 `payload`（环容量小于负载时必然如此），因此循环天然处理部分搬运。
///
/// # Errors
///
/// 写端关闭 / 取消、或环报出异常状态时返回 [`PumpError`]。
pub async fn write_all<T>(tx: &mut T, payload: &[u8]) -> Result<(), PumpError>
where
    T: TrBuffWrite<u8>
        + TrBuffTryWrite<u8, Err = ProducerError<usize>>
        + Unpin,
    for<'s> T::SegmMut<'s>: TrBuffSegmMut<'s, u8>,
{
    let mut off = 0usize;
    while off < payload.len() {
        let demand = Demand::at_least(1);
        let some = tx.write_async(&demand).await;
        if let Some(err) = some.as_ref().pick_right() {
            return Err(match err {
                ProducerError::Closing => PumpError::WriteClosed,
                ProducerError::Cancelled => PumpError::WriteCancelled,
                ProducerError::Stuffed(_) => {
                    PumpError::RingStuck("at_least(1) 下报 Stuffed")
                }
                ProducerError::Unsatisfiable => PumpError::Unsatisfiable,
            });
        }
        let mut segm = some.pick_left().ok_or(PumpError::ZeroMove)?;
        let moved = segm.move_items_from_as_buff(&payload[off..]);
        // 段在这里 drop：按已写入量提交进环，唤醒等待的读端。
        drop(segm);
        if moved == 0 {
            return Err(PumpError::ZeroMove);
        }
        off += moved;
    }
    Ok(())
}

/// 读端中转台：把环段搬进内部 `MaybeUninit` 缓冲，并给出已初始化的前缀。
///
/// 复用同一个 `Staging` 可以让稳态搬运零分配（缓冲随 `want` 增长一次，之后
/// 一直复用）。
#[derive(Debug, Default)]
pub struct Staging {
    /// 已搬入的字节（前 `len_` 个有效，但长度即容量，用 `pull` 的返回值界定有效区）。
    buf_: Vec<MaybeUninit<u8>>,
}

impl Staging {
    /// 新建空中转台。
    pub const fn new() -> Self {
        Staging { buf_: Vec::new() }
    }

    /// 预留 `cap` 字节的中转缓冲。
    pub fn with_capacity(cap: usize) -> Self {
        Staging {
            buf_: Vec::with_capacity(cap),
        }
    }

    /// 当前中转缓冲的字节容量。
    pub fn capacity(&self) -> usize {
        self.buf_.len()
    }

    /// 取最近一次 [`Staging::pull`] 搬入的前 `n` 个字节（`n` 会被夹到容量内）。
    pub fn bytes(&self, n: usize) -> &[u8] {
        let n = n.min(self.buf_.len());
        // SAFETY: `pull` 通过 `move_items_to_buff` 初始化了前 `n` 个元素；
        // `MaybeUninit<u8>` 与 `u8` 同布局，且 `u8` 没有无效位模式。
        unsafe { core::slice::from_raw_parts(self.buf_.as_ptr().cast::<u8>(), n) }
    }

    /// 从读端搬出**至多** `want` 字节，返回实际搬出的字节数。
    ///
    /// 返回 `Ok(0)` 表示读端已到终态（`ConsumerError::Closing`，即设备 EOF /
    /// 对端关闭写方向）。用 `Demand::at_least(1)` 借段：这样设备短读时环不会
    /// 因为「凑不够下限」而扣着数据不发。
    ///
    /// # Errors
    ///
    /// 等待被取消或环报出异常状态时返回 [`PumpError`]。
    pub async fn pull<R>(&mut self, rx: &mut R, want: usize) -> Result<usize, PumpError>
    where
        R: TrBuffRead<u8>
            + TrBuffTryRead<u8, Err = ConsumerError<usize>>
            + Unpin,
        for<'s> R::SegmRef<'s>: TrBuffSegmRef<'s, u8>,
    {
        let want = want.max(1);
        if self.buf_.len() < want {
            self.buf_.resize(want, MaybeUninit::uninit());
        }
        loop {
            let demand = Demand::at_least(1);
            let some = rx.read_async(&demand).await;
            if let Some(err) = some.as_ref().pick_right() {
                match err {
                    // 暂时没数据（例如设备这一轮刚好没读到）：继续等。
                    ConsumerError::Drained(_) => continue,
                    ConsumerError::Closing => return Ok(0),
                    ConsumerError::Cancelled => return Err(PumpError::ReadCancelled),
                    ConsumerError::Unsatisfiable => return Err(PumpError::Unsatisfiable),
                }
            }
            let mut segm = some.pick_left().ok_or(PumpError::ZeroMove)?;
            let now = segm.least_count().min(self.buf_.len());
            let moved = segm.move_items_to_buff(&mut self.buf_[..now]);
            // 段在这里 drop：按已搬出量提交，唤醒等待的生产端。
            drop(segm);
            if moved == 0 {
                return Err(PumpError::ZeroMove);
            }
            return Ok(moved);
        }
    }
}

/// 从读端搬出恰好 `want` 字节，**追加**到 `dst`。
///
/// 返回实际搬出的字节数：`Ok(0)` 表示「这条消息开始之前读端就关了」（干净的
/// EOF，RTT 服务端靠它收尾）；`Ok(want)` 表示搬满；中间量不会返回，而是报
/// [`PumpError::UnexpectedEof`]（消息中途断开属于协议错误）。
///
/// # Errors
///
/// 消息中途 EOF、等待被取消、环状态异常时返回 [`PumpError`]。
pub async fn read_exact_into<R>(
    rx: &mut R,
    staging: &mut Staging,
    want: u64,
    dst: &mut Vec<u8>,
) -> Result<u64, PumpError>
where
    R: TrBuffRead<u8>
        + TrBuffTryRead<u8, Err = ConsumerError<usize>>
        + Unpin,
    for<'s> R::SegmRef<'s>: TrBuffSegmRef<'s, u8>,
{
    let mut got = 0u64;
    while got < want {
        let remain = (want - got).min(usize::MAX as u64) as usize;
        let n = staging.pull(rx, remain).await?;
        if n == 0 {
            return if got == 0 {
                Ok(0)
            } else {
                Err(PumpError::UnexpectedEof { want, got })
            };
        }
        dst.extend_from_slice(staging.bytes(n));
        got += n as u64;
    }
    Ok(got)
}

/// 从读端搬出恰好 `want` 字节，逐段交给 `fold` 处理（不落地，适合只算校验和）。
///
/// # Errors
///
/// 没搬满 `want` 就对端关闭、等待被取消、环状态异常时返回 [`PumpError`]。
pub async fn read_exact_fold<R, F>(
    rx: &mut R,
    staging: &mut Staging,
    want: u64,
    mut fold: F,
) -> Result<u64, PumpError>
where
    R: TrBuffRead<u8>
        + TrBuffTryRead<u8, Err = ConsumerError<usize>>
        + Unpin,
    for<'s> R::SegmRef<'s>: TrBuffSegmRef<'s, u8>,
    F: FnMut(&[u8]),
{
    let mut got = 0u64;
    while got < want {
        let remain = (want - got).min(usize::MAX as u64) as usize;
        let n = staging.pull(rx, remain).await?;
        if n == 0 {
            return Err(PumpError::UnexpectedEof { want, got });
        }
        fold(staging.bytes(n));
        got += n as u64;
    }
    Ok(got)
}

/// 读一轮吞吐测试的 8 字节总长头部。
///
/// # Errors
///
/// 头部还没收全对端就关闭（`UnexpectedEof`）、等待被取消、环状态异常时返回
/// [`PumpError`]。
pub async fn read_header<R>(
    rx: &mut R,
    staging: &mut Staging,
) -> Result<u64, PumpError>
where
    R: TrBuffRead<u8>
        + TrBuffTryRead<u8, Err = ConsumerError<usize>>
        + Unpin,
    for<'s> R::SegmRef<'s>: TrBuffSegmRef<'s, u8>,
{
    let mut buf: Vec<u8> = Vec::with_capacity(crate::proto::HEADER_LEN);
    let got = read_exact_into(rx, staging, crate::proto::HEADER_LEN as u64, &mut buf).await?;
    if got == 0 {
        return Err(PumpError::UnexpectedEof {
            want: crate::proto::HEADER_LEN as u64,
            got: 0,
        });
    }
    let mut head = [0u8; crate::proto::HEADER_LEN];
    head.copy_from_slice(&buf);
    Ok(crate::proto::decode_header(&head))
}
