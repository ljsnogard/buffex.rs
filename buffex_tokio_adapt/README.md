# buffex_tokio_adapt

把**任意** `tokio::io::AsyncRead` / `AsyncWrite` 设备接上 `buffex` 环，变成
`TrBuffRead` / `TrBuffWrite`。

| 类型 | 方向 | 调用方拿到的 trait |
| --- | --- | --- |
| `BuffRead` | 设备 → 环 | `abs_buff::TrBuffRead` / `TrBuffTryRead` |
| `BuffWrite` | 环 → 设备 | `abs_buff::TrBuffWrite` / `TrBuffTryWrite` |

两个适配器各自持有一个 `buffex` 环（`Arc<Ring<…>>`，经 `Ring::split_unchecked`
拆成读写两半）与一台设备。收发双工要的是**两个**适配器（各一个环）——`ring` 的
安全前提是 SPSC，一个环没法同时被「泵」和「调用方」当成两侧用。

## 分配点由 `TrAllocConfig` 决定

实现里有两个**互相独立**的分配点，各自在 `TrAllocConfig` 里有一个关联类型，由调用方
逐点选分配器：

| 关联类型 | 用在哪 | 类型形态 |
| --- | --- | --- |
| `C::RingBodyAlloc` | 环的缓冲本体 | `RingBufOf<C>` = `mm_ptr::Owned<[MaybeUninit<u8>], _>` |
| `C::RingSharedAlloc` | 装环的容器（含引用计数块） | `SharedRingOf<C>` = `mm_ptr::Shared<Ring<…>, _>` |

两个方向的搬运都是**零拷贝**（段 ↔ `TrInput` / `TrOutput` 直连），所以除环本身之外没有
别的分配点。

入向有一个契约细节：`abs_buff` 的输入搬移**不允许**「返回 0 个且无错误」
（`assert!(cc > 0 || x.contains_right())`，提交 `e4092c8`——那正是它过去死循环的输入），
而 tokio 的 `AsyncRead` 用「读到 0」表示 EOF。这一层翻译由设备适配器
`abs_buff_tokio_adapt::ReadAsInput` 负责：它在读到 0 时返回带 `Closing`（终止性标签）
的错误，上游据此正常收尾。

构造时把配置实例传进去；默认实现 `DefaultAllocConfig` 两点全用 `Global`：

```rust,ignore
// 默认：全 Global
let mut rx = BuffRead::<_, DefaultAllocConfig>::try_new(dev, 4096, DefaultAllocConfig)?;

// 自定义：实现 TrAllocConfig，逐点挑分配器
#[derive(Clone)]
struct MyConfig { /* 每个分配点一个分配器实例 */ }

impl TrAllocConfig for MyConfig {
    type RingBodyAlloc = MyAlloc;      // 例如 arena
    type RingSharedAlloc = Global;     // 引用计数块用 Global
    fn ring_body_alloc(&self) -> Self::RingBodyAlloc { self.body.clone() }
    fn ring_shared_alloc(&self) -> Self::RingSharedAlloc { Global }
}

let mut rx = BuffRead::<_, MyConfig>::try_new(dev, 4096, my_config)?;
```

两个关联类型都要求 `core::alloc::AllocatorClone`（而**不是**只写
`Allocator + Clone`）——本 toolchain 上 `mm_ptr::Shared<T, A>: Clone` 的实现要求
`A: AllocatorClone`，标准库不会从 `Clone` 自动推导；拆分
（`Ring::split_unchecked` 内部 `clone()` 容器）正需要它。调用方因此需要在 crate 根写
`#![feature(allocator_api)]`。

## 驱动模型：两种，按上层协议栈的形状选

| 形态 | 构造入口 | 谁在驱动「环 → 设备」 |
| --- | --- | --- |
| **按需** | `BuffWrite::try_new` | 调用方：`try_write` / `write_async` / `flush_async` / `close_async` 每次顺带推一轮 |
| **自驱动** | `BuffWrite::try_new_local` | 后台泵：构造时经 `abs_art-tokio` 的 `LocalScope` 投到**本线程**队列 |

两者共用的接口语义：

- `BuffRead::try_read` / `BuffWrite::try_write` 是 try-once：同步推一轮搬运
  （设备此刻没数据 / 收不下就如实返回 `Drained` / `Stuffed`）；
- `BuffRead::read_async` / `BuffWrite::write_async` 在等待条件满足的过程中持续
  推进搬运，条件不满足时 park 在环的等待槽上（不忙等）；
- `BuffWrite::flush_async` / `BuffWrite::close_async` 用于收尾：把环内已提交但
  还没送达设备的数据冲出去。

按需形态**不 spawn 任何任务**：`tokio::spawn` 之外、`join!` 里、甚至同步上下文里拿
`try_*` 都能用；代价是「谁记得调收尾入口」由调用方负责——上层协议栈只把适配器当
`TrBuffWrite` 用时（例如 `smux_v1` 的写泵：提交完最后一个分片就 park 等新帧），最后
一段会永远留在环里。

自驱动形态补上这个缺口。为什么不做 `tokio::spawn` 的「内部后台泵」：泵要 await
`move_items_into_output_async`，它的 future 由 GAT 投影（`TrOutput::WriteAsync<'f>`）
取得，而 GAT 投影不传递 auto trait，`Send` 即使对 socket 这种 `Send` 设备也证明不出来
（rustc issue #100013）。可行的投递点是**本线程**的 `LocalSet`——
`abs_art-tokio::LocalScope::spawn_local` 不要求 `Send`。代价是构造点必须落在
`run_until` 内（见 `try_new_local` 的 `# Panics`）。

## 读侧：有数据就交付

读泵每轮只 **poll 一次**「设备 → 段」的搬移，设备此刻能给的搬多少算多少，随后 drop 段
提交；`pump_` 因此不 park，park 点在 `read_async` 循环里（借段 + 「恰好 1 字节」的搬移，
这次等待本身就是一次提交）。**不能**直接 await 上游的填段搬移：那是「段填满才提交」，
设备给出 25 字节后转为空闲时数据会被扣在段里——握手帧这类请求 / 应答式上层会永久静止。

## 不空转（no busy-wait）的保证

出向泵**不会**在设备不给进展时空转 CPU，`flush_async` / `close_async` 一定有界返回：

1. `pump_` 的返回值只表示「真的向设备交付了至少 1 个字节」，不以「结果里含 `Left`」
   为准——`SomeOf::Both(0, err)` 也含 `Left`，按它算进展就是无限循环；
2. **任何**带错误的结果（含 `Both(n, err)`）都先记进错误槽，再决定是否有进展：设备
   错误不允许被吞掉，调用方经 `take_error` 能拿到 `EPIPE` 这类真实原因；
3. 收尾循环遇到「环里还有数据、但一整轮零字节交付且设备未报错」时记一条
   `WriteZero` 并终止，不重试到天荒地老；
4. 设备在**非空**缓冲上返回 `Ok(0)`（tokio 只在空缓冲时允许 0）是设备违约，内部会在
   设备边界把它翻成 `WriteZero`；否则上游 `move_items_into_output_async` 会自己空转
   且不让出执行体（连 `tokio::time::timeout` 都插不进去）。

回归测试见 `tests/no_spin_.rs`（设备调用次数超限即 panic，把「空转」变成测试失败而
不是挂死）。背景与实测见 `dev-notes/write-20261002-0317.md`。

## 用法骨架

```rust,no_run
# #![feature(allocator_api)]
use abs_buff::{Demand, TrBuffRead};
use buffex_tokio_adapt::{BuffRead, BuffWrite, DefaultAllocatorConfig};

# async fn demo(dev_r: tokio::io::DuplexStream, dev_w: tokio::io::DuplexStream)
# -> std::io::Result<()> {
// 入向：设备 → 环 → 调用方。第三个参数是分配器配置。
let mut rx = BuffRead::<_, DefaultAllocatorConfig>::try_new(
    dev_r, 4096, DefaultAllocatorConfig,
)
.expect("容量合法");
let demand = Demand::at_least(1);
let some = rx.read_async(&demand).await;
// ……消费借到的段（move_items_to_buff / move_items_into_output_async）……

// 出向：调用方 → 环 → 设备。
let mut tx = BuffWrite::<_, DefaultAllocatorConfig>::try_new(
    dev_w, 4096, DefaultAllocatorConfig,
)
.expect("容量合法");
// ……借段写入后收尾，保证环内数据确实送达设备……
tx.close_async().await.expect("写方向应正常关闭");
# Ok(())
# }
```

## 两条容易踩的语义

**1. 窥视不等于消费。** `buffex` 的段有两种视图：

- `iter_slices()` 是**窥视**：给出 `&[u8]`，既不推进段内已消费量，drop 段时也不提交；
- `move_items_to_buff` / `move_items_into_output_async` **搬出多少就消费多少**，
  drop 段时按该量推进位置。

**2. 写段 drop 只是提交进环。** 它**不代表已经写进设备**；调用方收尾时必须 await
一次 `flush_async`（或 `close_async`），否则最后一批数据会留在环里。

## EOF 与错误

`buffex::ring::Ring` 没有对外的 `close`，设备 EOF / IO 错误写不进环的状态字，因此
适配器自己记：

- 设备 `read` 返回 0 → 置 EOF 标志；此后**环被取空**时把 `ConsumerError::Drained`
  合成为 `ConsumerError::Closing`，调用方由此看到 EOF；
- 设备出错 → 记入错误槽并停止搬运，环内已搬入的数据仍可读；错误经
  `BuffRead::take_error` / `BuffWrite::take_error` 取回（带 `ReadErrTag` / `WriteErrTag`）。

## 构建与测试

```bash
cd buffex_tokio_adapt
cargo test          # 13 个集成用例 + 5 个文档用例
                    #   tests/pump_.rs（8）：入向 32 KiB、出向 32 KiB、close 冲刷 +
                    #     shutdown、EOF 合成 Closing、设备错误取回、逐点分配器配置生效、
                    #     容量错误、短读设备不丢字节
                    #   tests/no_spin_.rs（5）：0 字节 + 错误、部分交付 + 错误、
                    #     非空缓冲 Ok(0)、真实 duplex 对端退出、flush/close 不空转
                    # （另：取数请用 `at_least(1)` 拿「当前就绪的」，别按「还差多少」
                    #   当下限——环在写端未关闭时不会交出不足下限的数据）
cargo clippy --all-targets
```

依赖 `buffex`（nightly feature），因此本 crate 也需要 nightly 工具链（还用到
`allocator_api`）。

## 已知限制（需要在产品语境里权衡）

**入向按「环段填满 / 设备结束」交付。** 读侧泵把设备数据搬到**一个写段填满**（或设备
EOF / 出错）之后才提交进环，因此设备上只有一条小消息、对端又不再补数据时，
`read_async` 会停在「等段填满」上（`try_read` 的 poll-once 路径能拿到部分数据）。
乒乓式请求/应答协议会踩中这一点，当前只能靠「环容量 = 消息长度」规避。
背景见 `buffex_uds_comp_experiment` 的 `dev-notes/experiment-20261002-0058.md` §3。
