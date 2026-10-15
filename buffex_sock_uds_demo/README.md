# buffex_sock_uds_demo

一个 **demo 性质**的实验 crate：让**两个进程**（一个 tokio、一个 compio）在
**TCP** 与 **UNIX domain socket** 上互通并做 benchmark，同时跑**两组对照**：

| 后端（`--backend`） | tokio 进程用什么 | compio 进程用什么 |
| --- | --- | --- |
| `buffex`（默认） | [`buffex_tokio_adapt`](../buffex_tokio_adapt) 的 `BuffRead` / `BuffWrite` | [`buffex_compio_adapt`](../buffex_compio_adapt) 的 `BuffRead` / `BuffWrite` |
| `raw`（对照组） | 只用 `tokio::io::AsyncRead` / `AsyncWrite` | 只用 `compio::io::AsyncRead` / `AsyncWrite` |

两组的**协议、计时窗口、校验和、进程编排、报告格式完全一致**，差别只在「设备 ↔
缓冲」这一段：buffex 组经适配器的环收发（业务代码只对着
[`TrBuffRead`](https://docs.rs/abs_buff) / `TrBuffWrite` 借段搬字节），对照组直接
读写 socket，**不碰任何 buffex 相关 crate**（见 `src/raw.rs`）。

编排器（`bench_harness`）不碰任何业务字节，只负责拉起两个对端、握手、收结果、出报告。

```text
  bench_harness（父进程）
        ├── peer_tokio   —— tokio 进程：--backend buffex 走适配器 / raw 走直连
        └── peer_compio  —— compio 进程：同上
                 ▲
                 └── 两者经 TCP 或 UNIX domain socket 互连
```

## 快速开始

```bash
cargo build --release            # 需要 nightly（buffex/abs_buff 用了 nightly feature）
cargo run  --release --bin bench_harness
```

跑完会在 `results/` 下生成 markdown 报告与 `.jsonl` 原始记录，同时在控制台打印
一张汇总表。默认矩阵：`buffex`/`raw` 两组后端 × `tcp`/`uds` × 消息粒度
`4K/64K/256K` × 两个方向（各 128 MiB）＋ 两组后端 × 两种传输 × RTT `64B/4K`
（各 1000 次往返），合计 40 个用例。

仓库里已留一份实跑记录：[`results/bench-final.md`](results/bench-final.md)
（40 个用例 × `--repeats 3` 取最好成绩；两组对照的逐项对比与解读见
[`dev-notes/experiment-20261002-0058.md`](dev-notes/experiment-20261002-0058.md) §6）。

常用参数：

```bash
# 只跑 UDS、两种粒度、每例 512 MiB
cargo run --release --bin bench_harness -- \
    --transports uds --chunks 64K,256K --bytes 512M

# 只跑对照组（不带 buffex），或只跑 buffex 组
cargo run --release --bin bench_harness -- --backends raw
cargo run --release --bin bench_harness -- --backends buffex

# 只跑 RTT
cargo run --release --bin bench_harness -- --skip-throughput --rtt-sizes 64,1K --rtt-iters 5000

# 指定报告路径与对端目录（对端二进制默认与 bench_harness 同目录）
cargo run --release --bin bench_harness -- --out results/mine.md --peer-dir target/release
```

单独手工跑一个用例（便于排查）：

```bash
./target/release/peer_compio --transport uds --mode recv --listen /tmp/x.sock \
    --chunk 64K --ring-cap 64K &
./target/release/peer_tokio --transport tcp --mode send --connect ...   # 见 --help
```

每个对端都支持 `--help`；`--listen` 侧会先往 stdout 打一行
`{"event":"ready","addr":...}` 供编排器握手，结束时打一行
`{"event":"result",...}`。

## 模块

| 模块 | 职责 |
| --- | --- |
| `bench` | benchmark 主体（与运行时、与适配器都无关，两个对端共用） |
| `proto` | 极简分帧：8 字节小端总长头部 + 确定性负载模式 |
| `checksum` | 按 `u64` 分组的滚动校验和（收发两侧对账） |
| `pump` | 与运行时无关的收发泵，只依赖 `abs_buff` 的段 trait |
| `raw` | **对照组**：不用 buffex，直接对 socket 做 `read` / `write`（协议与计时口径同 `bench`） |
| `cli` | 对端与编排器的命令行解析 |
| `finish` | 两个适配器出向类型的统一收尾能力（`FinishWrite` 的实现） |
| `metrics` | 结果结构与其 JSON 行编解码 |
| `runner` | 拉起两个对端进程、握手、收结果的编排逻辑 |
| `report` | markdown / 控制台 / JSONL 渲染 |

依赖上有一处特殊安排：`buffex_tokio_adapt` 从 git 取 `buffex`，而
`buffex_compio_adapt` 用同仓本地路径（它需要 `close` 一族）。两者同名同版本
不同源会让 Cargo 锁文件冲突，因此本 crate 用

```toml
[patch."https://gitee.com/lino_snsalias/buffex.git"]
buffex = { path = "../buffex" }
```

把 git 源统一到本地路径：整个依赖图里只有一份 `buffex`，两个适配器共用同一个
`Ring` 类型，实验代码因此能写一份泛型收发泵给两边用。

## benchmark 口径

* **单向吞吐**：发送端先写 8 字节总长头部（不计时也不计吞吐），随后按 `--chunk`
  粒度写满 `--bytes` 字节，计时窗口是「写头部之前 → `close` 返回」；接收端从
  「读完头部」计时到「收满负载」。两侧各自上报 MiB/s（1024² 字节）。
* **RTT**：客户端发一条 `chunk` 字节消息、收到同样一条回显算一次往返，
  逐轮计时后上报 p50/p99；服务端原样回显。
* 两侧都会对负载做滚动校验和，编排器核对「校验和一致 + 字节数一致」才判 ok。

### 已知约束（实验发现，务必先读）

1. **`read_async` 只在「一个可写环段被填满」或「设备结束 / 出错」时交付数据**
   （tokio 侧按需驱动、compio 侧后台泵，行为一致；见
   `tests/read_semantics_.rs` 的特征测试）。这是 **buffex 组**的约束，对照组没有环、
   不受影响。因此：
   * 乒乓式 RTT 必须让**一条消息恰好填满一个环段**：buffex 组下 `--ring-cap` 必须
     等于 `--chunk`（CLI 会拒绝不满足的取值，`bench_harness` 也会自动这么传）；
     `--backend raw` 没有环，`--ring-cap` 被忽略、也不受这条约束；
   * 单向吞吐不受影响：发送端持续灌数据，最后 `close` 让接收端看到 EOF。
2. `try_read`（poll-once）会按「已搬入量」提交被放弃的搬运，因此**能**拿到部分
   数据——需要低延迟小消息时可以用它，代价是调用方自己轮询。
3. 对照组的 compio 侧只能拿**拥有型**缓冲（`AsyncWriteExt::write_all<T: IoBuf>` /
   `AsyncRead::read<B: IoBufMut>`），所以 `src/raw.rs` 把接口设计成「把 `Vec<u8>`
   交出去再拿回来」，稳态不额外拷贝；这跟 compio 的 buffex 适配器（也必须过一次
   中转缓冲）是同一类约束，两组在这点上可比。
4. 上游 `buffex_tokio_adapt::BuffWrite::pump_` 对 `SomeOf::Both(0, err)` 的处理
   是「有左值就返回 `true`」，会让 `flush_async` 的 `while pump_() {}` 无限空转
   并吞掉设备错误（本实验在「对端已退出后服务端仍回写」时观察到 100% CPU 空转）。
   本实验不改动别的 crate，只在此备案；对端出错时靠编排器的超时兜底。

## 测试

```bash
cargo test                    # 全部：纯函数层 + 读语义特征 + 跨进程 e2e
cargo test --test proto_         # 分帧 / 校验和 / JSON 行 / CLI 解析（不起进程）
cargo test --test read_semantics_ # 「环段填满才交付」的特征测试（tokio & compio）
cargo test --test e2e_           # 真起两个进程：两组后端 × TCP/UDS × 两个方向 × 吞吐/RTT
```

## 构建前提

* **nightly** 工具链：`buffex` / `abs_buff` 使用 `impl_trait_in_assoc_type`、
  `allocator_api` 等 nightly feature。
* 本 crate 是**独立 workspace**，不应被加入其它 workspace（它同时引用两个适配器）。
* 依赖全部来自 git / 同仓路径。若本机 `~/.cargo` 已缓存这些 git 源，可以加
  `--offline` 构建；否则需要能访问 gitee。
