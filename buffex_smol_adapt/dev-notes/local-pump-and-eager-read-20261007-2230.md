# 自驱动写环 + 读侧「有数据就交付」（smol 侧）

日期：2026-10-07 22:30
涉及：`buffex_smol_adapt/src/{lib,read_,write_}.rs`、`Cargo.toml`、
`tests/{local_pump_,read_eager_}.rs`
同构改动与完整因果链：`buffex_tokio_adapt/dev-notes/local-pump-and-eager-read-20261007-2230.md`
下游：`smux_v1_sock_demo`（`dev-notes/adaptor-driven-transport-20261007-2230.md`）

## 摘要（smol 侧与 tokio 侧的差别）

1. **写侧自驱动**：新增 `BuffWrite::try_new_local`，构造时经 `abs_art-smol` 的
   `LocalScope`（= 本线程那条 `LocalExecutor` 的别名）`spawn_local` 一个「环 → 设备」的
   后台泵；`Drop` = 关生产端 + `detach` 泵（smol 的 `Task` 丢弃即**取消**，必须显式
   detach），因此「丢连接」不会丢掉已提交进环的字节。
2. **smol 与 tokio 的唯一差别**：smol 没有「环境运行时」这一前提——
   `abs_art_smol::Runtime::current()` 与 `local_scope()` 在任何线程上都能调，因此
   `try_new_local` **不要求**调用点处于某个驱动栈内（smol 侧没有 `TryNewError`，只可能
   因容量失败）。代价同样在这里：作用域不挑上下文，但只有**持有它的线程**
   （`smol::block_on(scope.run_until(..))`）驱动它时泵才会推进。
3. **读侧「有数据就交付」**：`pump_` 每轮只 poll 一次搬移（零拷贝），拿到多少提交多少；
   park 点移到 `read_async` 循环里的 `wait_input_`（借段 + 「恰好 1 字节」的搬移，
   这一次等待本身就是一次提交）。

## 验收

| 用例 | 结果 |
| --- | --- |
| `tests/local_pump_.rs`（自驱动交付；丢适配器仍把已提交字节搬完） | 2/2 |
| `tests/read_eager_.rs`（环容量 64 KiB、设备只给 25 字节也必须交付） | 1/1 |
| 既有 `tests/pump_smol.rs` + 文档测试 | 全通过 |
| `smux_v1_sock_demo` 全量环回（含 smol 的两组配对） | 3/3 |
