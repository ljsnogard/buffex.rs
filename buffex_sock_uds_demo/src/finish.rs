//! 给两个适配器的**出向**类型补上统一的收尾能力。
//!
//! `TrBuffWrite` 只保证「字节提交进环」，真正把环内数据交给设备要靠适配器自己
//! 的 `flush_async` / `close_async`。两个适配器的这两个入口同名但类型不同
//! （tokio 侧按需推进、compio 侧等后台泵），因此用 [`FinishWrite`] 这个实验内部
//! 的小 trait 把差异挡在 benchmark 主体之外。
//!
//! 实现放在**本 crate**（而不是两个对端二进制）里：trait 定义在本 crate，对被
//! 实现类型而言本 crate 是外部 crate——只有在定义 trait 的 crate 里写 impl 才不
//! 违反孤儿规则。

use core::borrow::Borrow;

use buffex::ring::Ring;

use crate::bench::FinishWrite;

/// tokio 侧出向适配器的收尾：`flush_async` / `close_async` 会持续推进
/// 「环 → 设备」的搬运（该适配器是按需驱动、不 spawn 后台泵）。
impl<W, C, S> FinishWrite for buffex_tokio_adapt::BuffWrite<W, C, S>
where
    W: tokio::io::AsyncWrite + Unpin,
    C: buffex_tokio_adapt::TrAllocConfig,
    S: Borrow<Ring<buffex_tokio_adapt::RingBufOf<C>, u8>>,
{
    async fn flush_ring(&mut self) -> Result<(), String> {
        match self.flush_async().await {
            Result::Ok(()) => Result::Ok(()),
            Result::Err(e) => {
                let extra = match self.take_error() {
                    Option::Some((io_err, tag)) => {
                        format!("；设备错误：{io_err:?}（{tag:?}）")
                    }
                    Option::None => String::new(),
                };
                Result::Err(format!("tokio 写端 flush 失败：{e:?}{extra}"))
            }
        }
    }

    async fn close_ring(&mut self) -> Result<(), String> {
        match self.close_async().await {
            Result::Ok(()) => Result::Ok(()),
            Result::Err(e) => {
                let extra = match self.take_error() {
                    Option::Some((io_err, tag)) => {
                        format!("；设备错误：{io_err:?}（{tag:?}）")
                    }
                    Option::None => String::new(),
                };
                Result::Err(format!("tokio 写端 close 失败：{e:?}{extra}"))
            }
        }
    }
}

/// compio 侧出向适配器的收尾：等后台泵把环取空（`flush_async`），必要时再
/// `shutdown` 设备（`close_async`）。
impl<W, C, S> FinishWrite for buffex_compio_adapt::BuffWrite<W, C, S>
where
    W: compio::io::AsyncWrite + 'static,
    C: buffex_compio_adapt::TrAllocConfig,
    S: Borrow<Ring<buffex_compio_adapt::RingBufOf<C>, u8>> + 'static,
{
    async fn flush_ring(&mut self) -> Result<(), String> {
        match self.flush_async().await {
            Result::Ok(()) => Result::Ok(()),
            Result::Err(e) => {
                let extra = match self.take_error() {
                    Option::Some((io_err, tag)) => {
                        format!("；设备错误：{io_err:?}（{tag:?}）")
                    }
                    Option::None => String::new(),
                };
                Result::Err(format!("compio 写端 flush 失败：{e:?}{extra}"))
            }
        }
    }

    async fn close_ring(&mut self) -> Result<(), String> {
        match self.close_async().await {
            Result::Ok(()) => Result::Ok(()),
            Result::Err(e) => {
                let extra = match self.take_error() {
                    Option::Some((io_err, tag)) => {
                        format!("；设备错误：{io_err:?}（{tag:?}）")
                    }
                    Option::None => String::new(),
                };
                Result::Err(format!("compio 写端 close 失败：{e:?}{extra}"))
            }
        }
    }
}
