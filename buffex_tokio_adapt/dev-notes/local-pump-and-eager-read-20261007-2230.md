# 自驱动写环 + 读侧「有数据就交付」：两处缺口的因果链

日期：2026-10-07 22:30
涉及：`buffex_tokio_adapt/src/{lib,read_,write_}.rs`、`Cargo.toml`、
`tests/{local_pump_,read_eager_}.rs`
下游：`smux_v1_sock_demo`（`dev-notes/adaptor-driven-transport-20261007-2230.md`）
同构改动：`buffex_smol_adapt`、`buffex_compio_adapt`

## 1. 现场：协议栈把适配器当纯 `TrBuffWrite` 用时，最后一段永远出不去

`smux_v1` 的写泵只做一件事：从连接写环借一段字节，搬进 `C::ConnTx`（本适配器），
drop 提交；连接写环一空就 park 等新帧。适配器的「环 → 设备」原先只在调用方调用
`write_async` / `flush_async` / `close_async` 时推进，于是：

```text
写泵：write_async（借段）→ 填 → drop 提交 → 写环空 → park
                                                   ↑
                                提交进适配器环的最后一段，再也没人搬
```

实测（`smux_v1_sock_demo` 的复现件，tokio 侧 stderr 加探针）：

```text
[prod] write_async ×9     ← 握手帧 9 个分片全部提交
[pump] segm 25 bytes      ← 泵只搬了这一批
[pump] park               ← 之后再无唤醒
```

对端因此只收到半帧。**根因不是 socket 特殊**：泵要 await
`move_items_into_output_async`，它的 future 由 `abs_buff` 经 GAT 投影
（`TrOutput::WriteAsync<'f>`）取得，而 GAT 投影不传递 auto trait——`tokio::spawn` /
`smol::spawn` 要求的 `Send` 即使设备本身 `Send` 也证明不出来
（rust-lang/rust#100013；README 里已有同一结论的编译探针）。**能自驱动的只有本地执行体**。

## 2. 修法（加法式，既有 `try_new` 语义不动）

新增 `BuffWrite::try_new_local(dev, cap, cfg)`：

* 构造时把设备与环的读半部移进后台泵，泵经 `abs_art-tokio` 的 `LocalScope`
  （= 本线程那条 `LocalSet` 的别名）`spawn_local`；`TrLocalScope::spawn_local` 不要求
  `Send`，这是唯一可行的投递点；
* 适配器此后只代理环的写端：`try_write` / `write_async` 就是环的借段；
* 泵退出时用守卫**关闭环的消费端**，让写端拿到 `ProducerError::Closing` 而不是永久 park；
* `flush_async` = 等环被泵取空；`close_async` = 关生产端 + 等泵排空并 `shutdown`；
* **`Drop` = 关生产端 + detach 泵**：上层（`smux_v1` 的写泵）只会丢连接，不会替适配器
  调收尾入口；只丢句柄会让已提交的字节随泵一起消失。

约束与代价：

* tokio 侧的构造点必须落在 `LocalSet` 的驱动栈内（`run_until` 内），否则 `spawn_local`
  panic；已写进 `try_new_local` 的 `# Panics`。不在 tokio 上下文内时返回
  `TryNewError::NotInRuntime`。
* smol 侧没有环境执行体，构造不挑上下文；但队列只有被**持有它的线程**驱动时泵才推进。
* 读侧不加自驱动：`BuffRead` 由协议栈的读泵反复 `read_async` 驱动，本来就有人推。

## 3. 紧接着暴露的第二个缺口：读侧「填满段才交付」

修完写侧，`smux_v1_sock_demo` 仍停在握手处，这次卡在对端。单测探针（环容量 64 KiB、
只写 25 字节、500 ms）证明：`BuffRead::read_async` **不交付**。

原因是读泵直接用 `move_items_from_input_async`，而它的语义是**填满段**：
`Demand::no_more_than(room)` 的上限即段长，设备给出 25 字节后转为空闲时它只是
`Pending`——数据扣在段里、一个字节也不提交。请求 / 应答式上层（握手帧、乒乓）
因此必然静止。这条早在 `dev-notes/write-20261002-0317.md` §6.1 就记为已知缺口。

修法：改成「**设备给多少就提交多少**」。

* tokio / smol（零拷贝保住）：每轮只 **poll 一次**搬移 future（`poll_fn` 立刻给
  `Ready`），设备此刻能给的搬多少算多少，随后 drop 段提交；丢弃未完成的搬移时，
  已读进段内的字节已经推进了段内偏移，drop 段即提交，没读到的部分不计入。
  `pump_` 因此**不再自 park**（否则调用方会被挡在 pump 后面、看不到已提交的字节），
  park 点移到 `read_async` 循环里的 `wait_input_`：借段 + 「恰好 1 字节」的搬移，
  这次等待本身就是一次提交。
* compio：设备读是**提交型**操作，丢弃未完成的 future 即取消它、已完成的读结果会丢，
  所以不用那一招；改成「先单独读一次设备（`read` 就是一次系统调用，读到多少算多少），
  再把这一批拷进环段逐段提交」。代价是 compio 侧本就存在的中转拷贝之外多一次拷贝。

## 4. 验收

| 用例 | 结果 |
| --- | --- |
| `tests/local_pump_.rs`（自驱动形态交付；**丢适配器仍把已提交字节搬完**） | 2/2 |
| `tests/read_eager_.rs`（环容量 64 KiB、设备只给 25 字节也必须交付） | 1/1 |
| 既有 `tests/pump_.rs` / `tests/no_spin_.rs` + 文档测试 | 全通过（按需驱动形态语义未变） |
| `buffex_sock_uds_demo/tests/read_semantics_.rs` | 5/5（旧的两条「不交付」用例已改成「必须交付」） |
| `smux_v1_sock_demo` 全量环回（debug，3 组配对 × 8 × 8 MiB） | 3/3 |

## 5. 遗留

1. 本 crate 尚未推到 gitee；下游暂时走本机路径依赖。
2. `Tokio` 侧 `try_new_local` 只在 `LocalSet` 驱动栈内可用，这限制是 `spawn_local` 的
   固有前提，不是本 crate 的选择。
3. 上游 `abs_buff` 的输入搬移若改成「有数据就交付」，两侧适配器都可以回到最直白的
   写法（compio 连那一次多余拷贝也能省掉）；那属于上游语义变更，需团队先决策。
