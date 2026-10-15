# `abs_buff-testkit`

`abs_buff` 的共享测试支撑：给实现/测试 `TrBuffSegmRef` / `TrBuffSegmMut`
（以及 `TrInput` / `TrOutput`）的 crate 复用，避免每个消费方各抄一份设备与断言
函数，进而出现「副本随着 trait 演进而漂移」的问题。

- `TestErr`：测试用错误类型；
- `TestInput` / `TestOutput`：数据与读写位置放在 `Arc` 里，设备被缓冲拥有期间
  测试仍可观察进度；
- `test_move_items_to_segm` / `test_move_items_from_segm`：`move_items_*` 的
  trait 默认实现的泛型断言函数。

## 装配

以 git 依赖引用 `abs_buff`。本地同时改动多个仓库时，在消费方把 git 依赖 patch
到本地路径：

```toml
[patch."https://gitee.com/lino_snsalias/abs_buff.git"]
abs_buff = { path = "../abs_buff" }
```
