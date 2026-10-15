use core::{
    future::{Future, IntoFuture},
    marker::PhantomData,
    pin::Pin,
    sync::atomic::AtomicUsize,
    task::{Context, Poll, Waker},
};

use abs_buff::{Demand, x_deps::abs_cancel};
use abs_cancel::{NonCancellableToken, TrMayCancel, TrCancellationToken};
use atomic_sync::mutex::preemptive::SpinningMutexOwned;

use super::{
    error_::{ConsumerError, ProducerError},
    hook_::TrPark,
    ring_core_::RingState,
};

// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 等待槽：SPSC 唤醒协议的数据结构
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
//
// 数据区不加锁（每个字节按 rp/wp 分区、所有权由状态字的 CAS 转移），但等待槽是**一个
// 会被两侧写的单值信箱**：持有者要填/撤，对端要取。它没有像 rp/wp 那样的分区，所以
// 必须有原子仲裁 —— 这里用 `atomic_sync` 的自旋锁把整个 `Option` 包起来，锁只覆盖
// 槽位的 move（几条指令），`Waker::clone` / `wake` 一律在锁外。
//
// 协议（每一侧对称，下面以「读端等数据」为例；状态位上再加一把自旋锁）：
//
//   等待者：① 锁内「填槽 + 发布 CONSUMER_STNDBY」（同一个临界区）
//           ② 复检条件（对端的提交可能落在①之前的检查与①的发布之间）
//              —— 已满足则撤回并回去重试，否则入睡
//   对端：  ③ 快照看到 CONSUMER_STNDBY → 锁内「按登记里的 demand 判定够不够
//              → 抢认领权（CAS 1→0）→ 取走登记」→ 锁外 `wake()`
//
// 关键不变式：
// * 登记（填槽）**先于**发布兴趣位，因此「兴趣位=1 ⇒ 槽里有登记」，对端永远不需要
//   等内容，最多因为锁被占而自旋几条指令；
// * 槽位访问全在锁内，认领用状态字的 CAS 仲裁，所以「取走」恰好发生一次；
// * `Demand` 按值存在登记里，对端判定用的是副本 —— 不再有指向调用者栈的裸指针，
//   审计里的悬垂指针（第 6 条）随之消失。

/// 登记到等待槽里的一次等待。
#[derive(Clone, Debug)]
struct WaitReg_ {
    /// 等待者的唤醒器。
    waker_: Waker,
    /// 本次等待的 `Demand`（按值保存，对端只用它的副本做判定）。
    demand_: Demand<usize>,
}

/// 一环（读半或写半）的等待槽。
#[derive(Debug)]
struct RingHalf_ {
    /// 单槽信箱：`Some` 表示当前有一个已登记的等待者。
    wait_slot_: SpinningMutexOwned<Option<WaitReg_>>,
}

impl RingHalf_ {
    const fn new_() -> Self {
        RingHalf_ {
            // `SpinningMutexOwned::new(data, cell)` 是 const；锁字初值 0 表示未上锁。
            wait_slot_: SpinningMutexOwned::new(Option::None, AtomicUsize::new(0)),
        }
    }

    /// 在锁内执行 `f`；锁只覆盖 `f` 本身（`f` 里只允许做槽位的 move）。
    ///
    /// `wait()` 走 `NonCancellableToken`，只会自旋到拿到锁，不会返回 `Cancelled`；
    /// 因此错误分支不可达，这里用 `unreachable!` 把它标出来而不是 `unwrap`。
    #[inline]
    fn with_slot_<R>(&self, f: impl FnOnce(&mut Option<WaitReg_>) -> R) -> R {
        let mut session = self.wait_slot_.lock_session();
        let mut guard = match session.lock().wait() {
            Result::Ok(guard) => guard,
            Result::Err(_) => unreachable!("spinning mutex without cancellation cannot fail"),
        };
        f(&mut guard)
    }

    /// 等待者：锁内「填槽 + 发布兴趣位」。
    ///
    /// 二者同处一个临界区，保证与对端的「认领 + 取走」互斥：不会出现「对端认领了旧
    /// 登记、却取走了刚填进来的新登记」这类交错。
    ///
    /// 返回 `false` 表示发布失败（兴趣位已经是 1）—— 正常协议下不该发生，调用方应当
    /// 撤回这次登记并回去重试，而不是在错误状态上入睡。
    #[inline]
    fn publish_wait_(
        &self,
        waker: Waker,
        demand: &Demand<usize>,
        publish: impl FnOnce() -> bool,
    ) -> bool {
        self.with_slot_(|slot| {
            debug_assert!(slot.is_none(), "wait slot should be empty before publish");
            *slot = Option::Some(WaitReg_ {
                waker_: waker,
                demand_: demand.clone(),
            });
            publish()
        })
    }

    /// 「判定 → 认领 → 取走」三步都在锁内完成；只有抢到 `claim` 的一方动槽位。
    ///
    /// `enough` 收到的是登记里的下限（`None` 按 1 计，与 `try_read_` / `try_write_`
    /// 的判定一致）：不够就不取走，让等待者继续睡着。
    #[inline]
    fn take_for_wake_(
        &self,
        enough: impl FnOnce(usize) -> bool,
        claim: impl FnOnce() -> bool,
    ) -> Option<WaitReg_> {
        self.with_slot_(|slot| {
            let reg = slot.as_ref()?;
            if !enough(reg.demand_.min().unwrap_or(1usize)) {
                return Option::None;
            }
            if !claim() {
                return Option::None;
            }
            slot.take()
        })
    }

    /// 等待者撤回本次等待（幂等）：同样要抢认领权，抢不到说明对端已经取走。
    #[inline]
    fn withdraw_(&self, claim: impl FnOnce() -> bool) {
        let _ = self.take_for_wake_(|_| true, claim);
    }
}

// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// passive::ConsumerHook
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

#[derive(Debug)]
pub struct Consumer<T> {
    ring_half_: RingHalf_,
    _unused_t_: PhantomData<fn() -> T>,
}

impl<T> Consumer<T> {
    pub const fn new() -> Self {
        Consumer {
            ring_half_: RingHalf_::new_(),
            _unused_t_: PhantomData,
        }
    }

    /// 等待者协议（读端）三步：登记 → 发布 → 复检。
    ///
    /// 返回 `true` 表示**不要睡**（条件已满足，或发布失败需要重试），调用方应回到外层
    /// 重新跑 `try_read_`。
    fn try_register_(
        &self,
        waker: &Waker,
        demand: &Demand<usize>,
        state: &RingState,
    ) -> bool {
        let published = self.ring_half_.publish_wait_(
            waker.clone(),
            demand,
            || state.try_publish_consumer_standby_(),
        );
        if !published {
            // 兴趣位已被占：撤回可能的残留登记，交给调用方重试。
            self.withdraw_(state);
            return true;
        }
        // 复检：对端的提交可能落在「条件检查」与「发布」之间。用与 `try_read_`
        // 相同的判据，命中就说明这一觉不该睡 —— 但必须先把刚发布的登记撤回，
        // 否则槽位与兴趣位会留在 half 上（下一次 park 就会撞上残留登记）。
        if state.can_consume_(demand) {
            self.withdraw_(state);
            return true;
        }
        false
    }

    /// 撤回本次等待（唤醒后清理 / 取消 / drop），幂等。
    fn withdraw_(&self, state: &RingState) {
        self.ring_half_
            .withdraw_(|| state.try_claim_consumer_standby_());
    }
}

impl<T> Default for Consumer<T> {
    fn default() -> Self {
        Consumer::new()
    }
}

impl<T> TrPark for Consumer<T> {
    type ParkAsync<'f> = ConsumerParkAsync<'f, T> where Self: 'f;
    type Err = ConsumerError<usize>;

    fn wake(&self, state: &RingState) {
        // 锁内完成「按登记里的下限判定 → 认领 → 取走」；真正唤醒放到锁外。
        // 「生产端已关闭」也算可唤醒：等待者醒来后会拿到 `Closing`，而不是永久睡下去
        // （判据必须与 `RingState::can_consume_` 保持一致，否则会漏唤醒）。
        let Option::Some(reg) = self.ring_half_.take_for_wake_(
            |min_demand| state.data_size() >= min_demand || state.is_producer_closed(),
            || state.try_claim_consumer_standby_(),
        ) else {
            return;
        };
        reg.waker_.wake();
    }

    #[inline]
    fn park_async<'f>(
        &'f self,
        demand: &'f Demand<usize>,
        state: &'f RingState,
    ) -> Self::ParkAsync<'f> {
        ConsumerParkAsync {
            consumer_: self,
            demand_: demand,
            state_: state,
        }
    }
}

// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// passive::ConsumerParkAsync
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// `park_async` 返回的适配器。它只持有借用：登记发生在 [`ConsumerParkFuture`] 的首次
/// poll，所以丢弃适配器本身不需要任何清理。
pub struct ConsumerParkAsync<'a, T> {
    consumer_: &'a Consumer<T>,
    demand_: &'a Demand<usize>,
    state_: &'a RingState,
}

impl<'a, T> IntoFuture for ConsumerParkAsync<'a, T> {
    type IntoFuture = ConsumerParkFuture<'a, T, NonCancellableToken>;
    type Output = Option<ConsumerError<usize>>;

    fn into_future(self) -> Self::IntoFuture {
        ConsumerParkFuture::new_(
            self.consumer_,
            self.demand_,
            self.state_,
            NonCancellableToken::new(),
        )
    }
}

impl<'a, T> TrMayCancel<'a> for ConsumerParkAsync<'a, T> {
    type MayCancelFuture<'f, C> = ConsumerParkFuture<'a, T, C>
    where
        'f: 'a,
        Self: 'f,
        C: 'f + TrCancellationToken;

    type MayCancelOutput = Option<ConsumerError<usize>>;

    fn may_cancel_with<C>(self, cancel: C) -> Self::MayCancelFuture<'a, C>
    where
        C: 'a + TrCancellationToken,
    {
        ConsumerParkFuture::new_(self.consumer_, self.demand_, self.state_, cancel)
    }
}

// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// passive::ConsumerParkFuture
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 读端的等待 future：首次 poll 完成「登记 → 发布 → 复检」，之后等待被唤醒或取消。
///
/// * `Poll::Pending` —— 已登记并入睡；
/// * `Poll::Ready(None)` —— 不要睡（被唤醒，或复检发现条件已满足），回去重试；
/// * `Poll::Ready(Some(Cancelled))` —— 取消令牌先到。
pub struct ConsumerParkFuture<'a, T, K>
where
    K: TrCancellationToken,
{
    consumer_: &'a Consumer<T>,
    demand_: &'a Demand<usize>,
    state_: &'a RingState,
    cancel_tok_: Option<K>,
    cancel_sig_: Option<<K::ChildToken as TrCancellationToken>::Cancellation>,
    /// 本次 future 是否已完成「登记 + 发布」。
    registered_: bool,
}

impl<'a, T, K> ConsumerParkFuture<'a, T, K>
where
    K: TrCancellationToken,
{
    const fn new_(
        consumer: &'a Consumer<T>,
        demand: &'a Demand<usize>,
        state: &'a RingState,
        cancel: K,
    ) -> Self {
        ConsumerParkFuture {
            consumer_: consumer,
            demand_: demand,
            state_: state,
            cancel_tok_: Option::Some(cancel),
            cancel_sig_: Option::None,
            registered_: false,
        }
    }
}

impl<'a, T, K> Future for ConsumerParkFuture<'a, T, K>
where
    K: TrCancellationToken,
{
    type Output = Option<ConsumerError<usize>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let make_cancelled = || ConsumerError::Cancelled;
        poll_park_future_(self, make_cancelled, cx)
    }
}

impl<'a, T, K> Drop for ConsumerParkFuture<'a, T, K>
where
    K: TrCancellationToken,
{
    fn drop(&mut self) {
        // 挂起中被丢弃（取消 / `select` 落败）：必须撤回登记，否则兴趣位与槽位会留在
        // half 上，污染下一次 park（审计里的第 1、5 条）。
        if self.registered_ {
            self.consumer_.withdraw_(self.state_);
        }
    }
}

// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// passive::ProducerHook
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

#[derive(Debug)]
pub struct Producer<T> {
    ring_half_: RingHalf_,
    _unused_t_: PhantomData<fn() -> T>,
}

impl<T> Producer<T> {
    pub const fn new() -> Self {
        Producer {
            ring_half_: RingHalf_::new_(),
            _unused_t_: PhantomData,
        }
    }

    /// 等待者协议（写端）三步：登记 → 发布 → 复检。见 [`Consumer::try_register_`]。
    fn try_register_(
        &self,
        waker: &Waker,
        demand: &Demand<usize>,
        state: &RingState,
    ) -> bool {
        let published = self.ring_half_.publish_wait_(
            waker.clone(),
            demand,
            || state.try_publish_producer_standby_(),
        );
        if !published {
            self.withdraw_(state);
            return true;
        }
        if state.can_produce_(demand) {
            self.withdraw_(state);
            return true;
        }
        false
    }

    /// 撤回本次等待（幂等）。
    fn withdraw_(&self, state: &RingState) {
        self.ring_half_
            .withdraw_(|| state.try_claim_producer_standby_());
    }
}

impl<T> Default for Producer<T> {
    fn default() -> Self {
        Producer::new()
    }
}

impl<T> TrPark for Producer<T> {
    type ParkAsync<'f> = ProducerParkAsync<'f, T> where Self: 'f;
    type Err = ProducerError<usize>;

    fn wake(&self, state: &RingState) {
        // 见 `Consumer::wake`：判据必须与 `RingState::can_produce_` 一致。
        let Option::Some(reg) = self.ring_half_.take_for_wake_(
            |min_demand| state.free_size() >= min_demand || state.is_consumer_closed(),
            || state.try_claim_producer_standby_(),
        ) else {
            return;
        };
        reg.waker_.wake();
    }

    #[inline]
    fn park_async<'f>(
        &'f self,
        demand: &'f Demand<usize>,
        state: &'f RingState,
    ) -> Self::ParkAsync<'f> {
        ProducerParkAsync {
            producer_: self,
            demand_: demand,
            state_: state,
        }
    }
}

// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// passive::ProducerParkAsync
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// `park_async` 返回的适配器。同 [`ConsumerParkAsync`]：只持有借用，drop 无需清理。
pub struct ProducerParkAsync<'a, T> {
    producer_: &'a Producer<T>,
    demand_: &'a Demand<usize>,
    state_: &'a RingState,
}

impl<'a, T> IntoFuture for ProducerParkAsync<'a, T> {
    type IntoFuture = ProducerParkFuture<'a, T, NonCancellableToken>;
    type Output = Option<ProducerError<usize>>;

    fn into_future(self) -> Self::IntoFuture {
        ProducerParkFuture::new_(
            self.producer_,
            self.demand_,
            self.state_,
            NonCancellableToken::new(),
        )
    }
}

impl<'a, T> TrMayCancel<'a> for ProducerParkAsync<'a, T> {
    type MayCancelFuture<'f, C> = ProducerParkFuture<'a, T, C>
    where
        'f: 'a,
        Self: 'f,
        C: 'f + TrCancellationToken;

    type MayCancelOutput = Option<ProducerError<usize>>;

    fn may_cancel_with<C>(self, cancel: C) -> Self::MayCancelFuture<'a, C>
    where
        C: 'a + TrCancellationToken,
    {
        ProducerParkFuture::new_(self.producer_, self.demand_, self.state_, cancel)
    }
}

// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// passive::ProducerParkFuture
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 写端的等待 future。语义同 [`ConsumerParkFuture`]。
pub struct ProducerParkFuture<'a, T, K>
where
    K: TrCancellationToken,
{
    producer_: &'a Producer<T>,
    demand_: &'a Demand<usize>,
    state_: &'a RingState,
    cancel_tok_: Option<K>,
    cancel_sig_: Option<<K::ChildToken as TrCancellationToken>::Cancellation>,
    registered_: bool,
}

impl<'a, T, K> ProducerParkFuture<'a, T, K>
where
    K: TrCancellationToken,
{
    const fn new_(
        producer: &'a Producer<T>,
        demand: &'a Demand<usize>,
        state: &'a RingState,
        cancel: K,
    ) -> Self {
        ProducerParkFuture {
            producer_: producer,
            demand_: demand,
            state_: state,
            cancel_tok_: Option::Some(cancel),
            cancel_sig_: Option::None,
            registered_: false,
        }
    }
}

impl<'a, T, K> Future for ProducerParkFuture<'a, T, K>
where
    K: TrCancellationToken,
{
    type Output = Option<ProducerError<usize>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let make_cancelled = || ProducerError::Cancelled;
        poll_park_future_(self, make_cancelled, cx)
    }
}

impl<'a, T, K> Drop for ProducerParkFuture<'a, T, K>
where
    K: TrCancellationToken,
{
    fn drop(&mut self) {
        // 同 `ConsumerParkFuture`：挂起中被丢弃也要撤回登记。
        if self.registered_ {
            self.producer_.withdraw_(self.state_);
        }
    }
}

// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 协议步骤在两个 future 上的实现
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

trait TrFutBorrowRingHalf_: Future {
    type CancelTok: TrCancellationToken;

    fn registered(&self) -> bool;

    fn set_registered_(&mut self, registered: bool);

    /// 首次 poll 的协议三步；`true` 表示「不要睡」（条件已满足或需要重试）。
    fn try_register_(&mut self, waker: &Waker) -> bool;

    /// 撤回本次等待（幂等）。
    fn withdraw_(&mut self);

    fn cancel_sig(
        &mut self,
    ) -> &mut Option<
        <<Self::CancelTok as TrCancellationToken>::ChildToken
            as TrCancellationToken>::Cancellation
    >;

    fn cancel_tok(
        &mut self
    ) -> &mut Option<Self::CancelTok>;
}

impl<'a, T, K> TrFutBorrowRingHalf_ for ConsumerParkFuture<'a, T, K>
where
    K: TrCancellationToken,
{
    type CancelTok = K;

    fn registered(&self) -> bool {
        self.registered_
    }

    fn set_registered_(&mut self, registered: bool) {
        self.registered_ = registered;
    }

    fn try_register_(&mut self, waker: &Waker) -> bool {
        self.consumer_
            .try_register_(waker, self.demand_, self.state_)
    }

    fn withdraw_(&mut self) {
        self.consumer_.withdraw_(self.state_);
    }

    fn cancel_sig(
        &mut self,
    ) -> &mut Option<
        <<Self::CancelTok as TrCancellationToken>::ChildToken
            as TrCancellationToken>::Cancellation
    > {
        &mut self.cancel_sig_
    }

    fn cancel_tok(
        &mut self
    ) -> &mut Option<Self::CancelTok> {
        &mut self.cancel_tok_
    }
}

impl<'a, T, K> TrFutBorrowRingHalf_ for ProducerParkFuture<'a, T, K>
where
    K: TrCancellationToken,
{
    type CancelTok = K;

    fn registered(&self) -> bool {
        self.registered_
    }

    fn set_registered_(&mut self, registered: bool) {
        self.registered_ = registered;
    }

    fn try_register_(&mut self, waker: &Waker) -> bool {
        self.producer_
            .try_register_(waker, self.demand_, self.state_)
    }

    fn withdraw_(&mut self) {
        self.producer_.withdraw_(self.state_);
    }

    fn cancel_sig(
        &mut self,
    ) -> &mut Option<
        <<Self::CancelTok as TrCancellationToken>::ChildToken
            as TrCancellationToken>::Cancellation
    > {
        &mut self.cancel_sig_
    }

    fn cancel_tok(
        &mut self
    ) -> &mut Option<Self::CancelTok> {
        &mut self.cancel_tok_
    }
}

/// 实现一个与具体运行时无关的手动唤醒，与 Cancellation Token 竞争信号
/// 哪一个先到达
fn poll_park_future_<F, E>(
    future: Pin<&mut F>,
    make_cancelled: impl FnOnce() -> E,
    cx: &mut Context<'_>,
) -> Poll<Option<E>>
where
    F: TrFutBorrowRingHalf_,
{
    let this = unsafe { future.get_unchecked_mut() };
    let x = if !this.registered() {
        // 首次 poll：登记 → 发布 → 复检。三者顺序即协议，不可颠倒。
        if this.try_register_(cx.waker()) {
            // 复检发现条件已满足（对端的提交落在外层「检查」与本步「发布」之间）：
            // 不能睡，交回调用方重试。
            return Poll::Ready(Option::None);
        }
        this.set_registered_(true);
        // 第一次 poll 时取得 cancellation signal。
        if this.cancel_sig().is_none() {
            let child_tok = this.cancel_tok()
                .as_ref()
                .map(|tk| tk.child_token())
                .expect("cancel token already taken");

            *this.cancel_sig() = Option::Some(child_tok.cancellation());
        }
        // cancellation signal 与 hook 共用当前 waker。
        let Option::Some(cancel_sig) = this.cancel_sig().as_mut() else {
            unreachable!()
        };
        let f = unsafe { Pin::new_unchecked(cancel_sig) };
        if f.poll(cx).is_ready() {
            Poll::Ready(Option::Some(make_cancelled()))
        } else {
            Poll::Pending
        }
    } else {
        // 后续 poll：被唤醒（含 spurious）或被取消。两种都交回调用方重试/返回。
        if this.cancel_tok().as_ref().is_some_and(|tk| tk.is_cancelled()) {
            Poll::Ready(Option::Some(make_cancelled()))
        } else {
            Poll::Ready(Option::None)
        }
    };
    // 收尾：Ready 时撤回登记（幂等；对端已认领时它是空操作）。
    if x.is_ready() {
        this.withdraw_();
        this.set_registered_(false);
    }
    x
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 校验 park future 首次 poll 完成「登记 + 发布」，挂起中被 drop 时撤回。
    /// - 测试目标：首次 poll 后兴趣位应为 1、槽位应有登记；drop（取消）之后兴趣位清零、
    ///   槽位清空。读写两端都要如此，否则残留状态会污染下一次 park（同步空转 / 对端按
    ///   过时 demand 判定）。
    /// - 测试手段：用一个空 `RingState` 分别构造读端与写端的 park future，用空 waker
    ///   poll 一次确认挂起，随后 drop，读出兴趣位与槽位。
    /// - 判定标准：poll 后对应侧的兴趣位为真且槽位为 `Some`；drop 后分别为假 / `None`。
    #[test]
    fn park_future_registers_and_withdraws_() {
        let state = RingState::new(2);
        let demand = Demand::at_least(1);

        // -- 读端 --
        let consumer = Consumer::<u8>::new();
        {
            let mut park = std::boxed::Box::pin(
                consumer.park_async(&demand, &state).into_future(),
            );
            let mut cx = Context::from_waker(Waker::noop());
            assert!(
                park.as_mut().poll(&mut cx).is_pending(),
                "首次 park 应挂起（非取消令牌的 cancellation 永不就绪）"
            );
            assert!(state.is_consumer_standby_(), "首次 poll 后应已发布兴趣位");
            assert!(
                consumer.ring_half_.with_slot_(|slot| slot.is_some()),
                "首次 poll 后槽位应有登记"
            );
        } // drop：必须撤回

        assert!(
            !state.is_consumer_standby_(),
            "park future drop 后应撤回兴趣位"
        );
        assert!(
            consumer.ring_half_.with_slot_(|slot| slot.is_none()),
            "park future drop 后槽位应为空"
        );

        // -- 写端（对称） --
        // 容量 2 的新环是空的（free == 2），写作 `at_least(1)` 会立即满足、不 park；
        // 这里用永远不满足的下限，专门测「登记 → 发布 → drop 撤回」这条机械路径。
        let producer = Producer::<u8>::new();
        let stuck_demand = Demand::at_least(3);
        {
            let mut park = std::boxed::Box::pin(
                producer.park_async(&stuck_demand, &state).into_future(),
            );
            let mut cx = Context::from_waker(Waker::noop());
            assert!(
                park.as_mut().poll(&mut cx).is_pending(),
                "首次 park 应挂起（非取消令牌的 cancellation 永不就绪）"
            );
            assert!(state.is_producer_standby_(), "首次 poll 后应已发布兴趣位");
        }
        assert!(
            !state.is_producer_standby_(),
            "park future drop 后应撤回兴趣位"
        );
        assert!(
            producer.ring_half_.with_slot_(|slot| slot.is_none()),
            "park future drop 后槽位应为空"
        );
    }

    /// 校验「认领」互斥：一次登记只会被取走一次。
    /// - 测试目标：`take_for_wake_` 的「判定 + 认领 + 取走」在锁内原子完成；认领过一次
    ///   之后兴趣位清零、槽位为空，再认领与撤回都必须是空操作。
    /// - 测试手段：手动发布一次登记，然后连续认领两次，最后再撤回一次。
    /// - 判定标准：第一次认领拿到登记且兴趣位归零；第二次认领与后续撤回都返回 `None`
    ///   （`withdraw_` 无返回值，用槽位仍为空来断言）。
    #[test]
    fn slot_claim_happens_exactly_once_() {
        let state = RingState::new(2);
        let demand = Demand::at_least(1);
        let consumer = Consumer::<u8>::new();

        assert!(
            consumer.ring_half_.publish_wait_(
                Waker::noop().clone(),
                &demand,
                || state.try_publish_consumer_standby_(),
            ),
            "首次发布应成功"
        );
        assert!(state.is_consumer_standby_());

        let first = consumer.ring_half_.take_for_wake_(
            |_| true,
            || state.try_claim_consumer_standby_(),
        );
        assert!(first.is_some(), "第一次认领应拿到登记");
        assert!(!state.is_consumer_standby_(), "认领后兴趣位应归零");

        let second = consumer.ring_half_.take_for_wake_(
            |_| true,
            || state.try_claim_consumer_standby_(),
        );
        assert!(second.is_none(), "登记只可能被取走一次");

        consumer.ring_half_
            .withdraw_(|| state.try_claim_consumer_standby_());
        assert!(
            consumer.ring_half_.with_slot_(|slot| slot.is_none()),
            "撤回后槽位仍为空"
        );
    }
}
