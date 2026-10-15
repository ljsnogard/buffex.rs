//! 测试共用的支持代码（仅 `cfg(test)` 编译）。

/// 让一个异步用例在 **tokio** 与 **compio** 两种**真实运行时**下各跑一遍。
///
/// 用法：把用例写成 `async fn name_()`，紧随其后写 `dual_runtime_test_!(name_);`。
/// 不自己 `block_on`、不手动轮询——那样测的是「假设的世界」，而不是真实运行时里
/// 被 waker 驱动的行为。
macro_rules! dual_runtime_test_ {
    ($name:ident) => {
        #[allow(non_snake_case, missing_docs)]
        mod $name {
            #[tokio::test]
            async fn tokio_() {
                super::$name().await
            }

            #[compio::test]
            async fn compio_() {
                super::$name().await
            }
        }
    };
}

pub(crate) use dual_runtime_test_;
