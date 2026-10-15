//! # buffex_uds_comp_experiment
//!
//! 一个 demo 性质的实验 workspace：用 `buffex_tokio_adapt` 与
//! `buffex_compio_adapt` 分别作为**两个进程**的通信底座，在 **TCP** 与
//! **UNIX domain socket** 上做收发，并分别做 benchmark。
//!
//! ## 进程形态
//!
//! ```text
//!   bench_harness（父进程：只负责拉起两个对端、收结果、出报告，不碰业务字节）
//!         ├── peer_tokio   —— tokio 进程，设备与环之间用 buffex_tokio_adapt
//!         └── peer_compio  —— compio 进程，设备与环之间用 buffex_compio_adapt
//!                  ▲
//!                  └── 两者经 TCP 或 UNIX domain socket 互连
//! ```
//!
//! 两个对端都**只**通过适配器 crate 提供的 `BuffRead` / `BuffWrite` 收发：
//! 设备（socket）与环之间的搬运由适配器负责，业务代码只对着
//! [`TrBuffRead`](abs_buff::TrBuffRead) / [`TrBuffWrite`](abs_buff::TrBuffWrite)
//! 借段、搬字节。
//!
//! ## 模块
//!
//! | 模块 | 职责 |
//! | --- | --- |
//! | [`bench`] | benchmark 主体（与运行时、与适配器都无关，两个对端共用） |
//! | [`proto`] | 极简分帧：8 字节小端总长头部 + 确定性负载模式 |
//! | [`checksum`] | 按 `u64` 分组的滚动校验和（收发两侧对账用） |
//! | [`pump`] | **与运行时无关**的收发泵，只依赖 `abs_buff` 的段 trait |
//! | [`raw`] | **对照组**：不用 buffex，直接拿 tokio/compio 的 `AsyncRead`/`AsyncWrite` 收发 |
//! | [`cli`] | 对端与 harness 的命令行参数解析 |
//! | [`finish`] | 两个适配器出向类型的统一收尾能力（`FinishWrite` 的实现） |
//! | [`metrics`] | 结果结构与其 JSON 行编解码 |
//! | [`runner`] | 拉起两个对端进程、握手、收结果的编排逻辑 |
//! | [`report`] | 把一组结果渲染成 markdown 报告 |
//!
//! ## 为什么收发泵能共用
//!
//! 两个适配器虽然驱动模型不同（tokio 侧按需推进、compio 侧后台泵），但**调用方
//! 看到的 trait 是同一套**：`TrBuffRead<u8>` / `TrBuffWrite<u8>`，段类型也都由
//! `abs_buff` 定义（两个适配器引用的是同源同分支的 `abs_buff`）。因此实验里
//! 的搬运代码只写一遍（见 [`pump`]），两个对端的差别仅在「怎么建连接、怎么构造
//! 适配器」。

pub mod bench;
pub mod checksum;
pub mod cli;
pub mod finish;
pub mod metrics;
pub mod proto;
pub mod pump;
pub mod raw;
pub mod report;
pub mod runner;
