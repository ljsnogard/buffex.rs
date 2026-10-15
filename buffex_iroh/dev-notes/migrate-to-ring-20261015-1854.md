# buffex_iroh 迁移到 `buffex::ring`（20261015-1854）

## 起因：编译不过

`buffex.rs` 把 `buffex` 家族与 `abs_buff` 家族并成一个 workspace 之后，`buffex_iroh`
（以及 `abs_buff_stdio_adapt` 的测试目标）编译失败。两个 crate 的病因不同：

* **`abs_buff_stdio_adapt`**：它是「旧 `abs_buff` 家族」的成员，`dev-dependencies`
  里 `buffex` / `abs_buff-testkit` 仍指向**上游 git 源**，而本 crate 依赖的是
  **本地 path 版** `abs_buff`。Cargo 因此解析出两个 `abs_buff` 实例，测试里
  `Ring` 实现的是 git 版 `TrBuffRead`，与本 crate 用的 path 版无法互操作
  （`the trait bound ... is not satisfied`）。旧的 `[patch]` 注释段就是当初的绕法。
  并成一个 workspace 之后，正解是让这些依赖也走 path（见
  `abs_buff_stdio_adapt/Cargo.toml`）。

* **`buffex_iroh`**：它写于 `buffex` 还有 `circular_buff` 的年代，直接使用
  `CircularBuffBuilder` / `Producer` / `Consumer` / `DevProducer` / `DevConsumer`。
  `buffex` 重写成 `buffex::ring`（`Ring` + `RingReader` / `RingWriter`，且把
  「环 ↔ 设备」的搬运交给适配器自己写）之后，这些类型统统不存在了。

## 关键判断：iroh 流就是 tokio 设备

`iroh::endpoint::RecvStream` / `SendStream` 是 `noq::RecvStream` / `SendStream` 的
重导出，它们**已经实现** tokio 的 `AsyncRead` / `AsyncWrite`，而且
`SendStream` 的 `poll_shutdown` 正是 `finish()`。也就是说：

* 本 crate 不需要自己的设备层（原来的 `device.rs`：`StreamInput` / `StreamOutput`
  把 iroh 流包成 `abs_buff` 的 `TrInput` / `TrOutput`）；
* workspace 里已经有现成的、**已迁移到 `buffex::ring`** 的通用适配器
  `buffex_tokio_adapt::{BuffRead, BuffWrite}`（`iroh` 流可直接传入）。

## 三个候选方案与取舍

| 方案 | 做法 | 取舍 |
| --- | --- | --- |
| A（采纳） | 薄封装 `buffex_tokio_adapt::{BuffRead, BuffWrite}`，删掉 `device` 模块 | 改动最小；与 `buffex_tokio_adapt` 不重复实现搬运；代价是 `pub mod device` 消失（公开 API 收缩），`close_async` 的返回类型由 `bool` 变成内层 future 的语义 |
| B | 保留 `device` 模块，参照 `buffex_tokio_adapt` 用 `buffex::ring` 手写搬运泵 | 公开 API 全不变；但要复制 500~700 行，还要重新踩一遍 `buffex_tokio_adapt` 已经踩过的坑（零拷贝段提交、零进展终止、不 busy-wait、取消语义） |
| C | 内部复用 `BuffRead` / `BuffWrite`，`device` 模块原样保留 | 折中，但会留下一个没人引用的公开模块 |

选 A 的理由：`buffex_tokio_adapt` 的搬运逻辑是同一批人为了解决同一批问题（见它的
`write-20261002-0317.md` 与 `local-pump-and-eager-read-20261007-2230.md`）写出来的，
在 `buffex_iroh` 里重写一遍只会产生第二份需要同步维护的实现。**「环 ↔ 设备」的搬运
不属于 iroh 这一层**——iroh 只贡献「流是 tokio 设备」这一事实。

## 迁移后的公开 API 变化（需要知情）

* `pub mod device`（`StreamInput` / `StreamOutput`）删除；`StreamInputErr` /
  `StreamOutputErr` 移到 crate 根导出（类型不变，仍是 `TaggedError<io::Error, _>`）。
* `take_error` 由 `async fn take_error(&self)` 变为 `fn take_error(&mut self)`：
  内层适配器的错误槽是 `&mut self`，且旧签名依赖 `tokio::sync::Mutex` 的异步锁。
* `try_read` / `try_write` / `read_async` / `write_async` 的**具体返回类型**换成
  内层适配器的（`RingSegmRef` / `RingSegmMut` + `ConsumerError<usize>` /
  `ProducerError<usize>`）；`try_new` 的错误类型仍是 `usize`（把内层的
  `InvalidCapacity` 摊平），保持旧签名。
* 增加了若干类型别名（`IrohReaderSegmRef` 等）与 `TrConsumerState` 的实现
  （后者是 HEAD 里迁移到一半时就有的）。

## 顺手修掉的两处测试问题（与 `Demand` 语义漂移有关）

迁移后集成测试从「编译不过」变成「跑不过」，都不是适配器的问题，而是测试用了旧语义：

1. **`Demand::less_than(n)` 是严格小于**（上界 `n - 1`）。测试写
   `less_than(剩余字节数)`，剩余 1 字节时上界就是 0；`buffex::ring` 的
   `try_write_internal_` / `try_read_internal_` 有 `debug_assert!(take > 0)`，
   于是一借段就 panic。改成**含端点**的 `no_more_than(剩余字节数)`（测试本意就是
   「最多剩余字节数」）。
2. **`Endpoint::close()` 不会等对端取走「已经 `finish()`、但还没送达」的尾部数据**。
   `try_write_shutdown_flushes_all_buffered_data` 里客户端写完就关端点，服务端会
   永久停在 4064/3936 字节上（连接不发数据也不报错，直到空闲超时）。同一个文件里的
   第一个测试早就用 `done_rx` / `done_tx` 挡住过这个坑，这里照做：服务端收全后回执，
   客户端等回执再关端点。

## 验证

* `cargo check --workspace --all-targets`、`cargo clippy --workspace --all-targets`
  均无警告无错误；
* `abs_buff_stdio_adapt` 14 个单元测试通过；
* `buffex_iroh` 3 个集成测试（真实 iroh 连接）+ 2 个文档测试通过，连跑 3 次稳定；
* `python3 format_use_imports.py --check buffex.rs/buffex_iroh buffex.rs/abs_buff_stdio_adapt`
  合规；新写的源文件已过 `rustfmt`。
