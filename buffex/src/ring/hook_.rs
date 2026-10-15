use abs_buff::{Demand, x_deps::abs_cancel};
use abs_cancel::TrMayCancel;

use crate::ring::ring_core_::RingState;

/// 一环（读半 / 写半）的等待与唤醒钩子。
///
/// `park_async` 返回的 future 在**首次 poll** 里完成协议三步：
/// 「登记 waker」→「发布 STNDBY 兴趣位」→「复检条件」；顺序不可颠倒（见
/// `half_.rs` 顶部的协议说明）。
pub trait TrPark {
    type ParkAsync<'f>: TrMayCancel<'f, MayCancelOutput = Option<Self::Err>>
    where
        Self: 'f;

    type Err;

    /// 对端提交之后尝试唤醒等待者：判定、认领、取走都在内部完成，`wake()` 在锁外调用。
    fn wake(&self, state: &RingState);

    /// 构造本次等待的 future；`state` 用来发布/撤回兴趣位并复检条件。
    fn park_async<'f>(
        &'f self,
        demand: &'f Demand<usize>,
        state: &'f RingState,
    ) -> Self::ParkAsync<'f>;
}
