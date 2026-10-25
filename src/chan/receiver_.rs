//! 消费端句柄与异步接收。

use alloc::sync::Arc;
use core::{borrow::BorrowMut, sync::atomic::Ordering, task::Poll};

use abs_cancel::{ok_or::XtOkOr, TrCancellationToken};
use gen_mcf2::gen_may_cancel_future;

use crate::{MpmcBuf, buf_::Pop_};

use super::{
    config_::{DefaultConfig, TrConfig},
    core_::{MpmcChan, TryPop_},
    error_::{RecvError, TryRecvError},
    metrics_::metric_,
    util_::CANCEL_CHECK_EVERY,
    wait_queue_::WaitGuard,
};

/// 消费端句柄。
///
/// `Receiver` 可以被 [`Clone`]；每个句柄持有一个 `Arc<MpmcChan>`。最后一个
/// `Receiver` 被 Drop 时会把接收方向标记为关闭，并唤醒两个方向的全部等待者。
///
/// # Examples
///
/// ```
/// use mpmc_chan::{MpmcBuf, MpmcChan};
///
/// let mut buf = MpmcBuf::<u32, 2>::new();
/// let (tx, rx) = MpmcChan::new(&mut buf);
/// tx.try_send(1).unwrap();
/// assert_eq!(rx.try_recv(), Ok(1));
/// ```
pub struct Receiver<'a, T, const N: usize, B = &'a mut MpmcBuf<T, N>, C = DefaultConfig>
where
    B: BorrowMut<MpmcBuf<T, N>>,
    C: TrConfig + 'a,
{
    core_: Arc<MpmcChan<'a, T, N, B, C>, C::ChanAlloc>,
}

impl<'a, T, const N: usize, B, C> Receiver<'a, T, N, B, C>
where
    B: BorrowMut<MpmcBuf<T, N>>,
    C: TrConfig + 'a,
{
    /// 内部构造：增加接收端句柄计数。
    pub(super) fn new_(core_: Arc<MpmcChan<'a, T, N, B, C>, C::ChanAlloc>) -> Self {
        core_.receivers_.fetch_add(1, Ordering::Relaxed);
        Receiver { core_ }
    }

    /// 尝试立即接收一个元素。
    ///
    /// # Errors
    ///
    /// - [`TryRecvError::Empty`]：环空，或头槽已预留但生产者尚未发布；
    /// - [`TryRecvError::Closed`]：通道已关闭且没有更多元素。已入队的元素会先被
    ///   drain，因此不会因为关闭而丢弃。
    ///
    /// # Examples
    ///
    /// ```
    /// use mpmc_chan::{MpmcBuf, MpmcChan, TryRecvError};
    ///
    /// let mut buf = MpmcBuf::<u8, 2>::new();
    /// let (tx, rx) = MpmcChan::new(&mut buf);
    /// assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    /// tx.try_send(1).unwrap();
    /// assert_eq!(rx.try_recv(), Ok(1));
    /// drop(tx);
    /// assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
    /// ```
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self.core_.try_pop_try_() {
            TryPop_::Value(value) => {
                self.core_.notify_prod_();
                Result::Ok(value)
            }
            // 头槽已预留但生产者尚未发布属于 would-block，不能据此断言「已关闭
            // 且无元素」：在途生产者马上就会发布。
            TryPop_::Pending => Result::Err(TryRecvError::Empty),
            TryPop_::Empty => {
                if self.core_.is_closed_() {
                    Result::Err(TryRecvError::Closed)
                } else {
                    Result::Err(TryRecvError::Empty)
                }
            }
        }
    }

    /// 异步接收一个元素；空时进入本方向等待队列。
    ///
    /// 返回的 [`RecvAsync`] 可以直接 `.await`，也可以先
    /// `.may_cancel_with(token)` 换成可取消版本。
    ///
    /// # Errors
    ///
    /// - [`RecvError::Closed`]：通道已关闭且没有更多元素；已入队元素会先被 drain；
    /// - [`RecvError::Cancelled`]：等待被取消。
    ///
    /// # Examples
    ///
    /// ```
    /// use futures_lite::future::block_on;
    /// use mpmc_chan::{MpmcBuf, MpmcChan};
    ///
    /// let mut buf = MpmcBuf::<u8, 2>::new();
    /// let (tx, rx) = MpmcChan::new(&mut buf);
    /// tx.try_send(7).unwrap();
    /// assert_eq!(block_on(async { rx.recv_async().await }), Ok(7));
    /// ```
    pub fn recv_async<'f>(&'f self) -> RecvAsync<'a, 'f, 'f, T, B, C, N>
    where
        'a: 'f,
    {
        RecvAsync::new(self)
    }

    /// 关闭接收方向：此后不再接受新发送；已入队元素仍可继续接收直到空。
    pub fn close(&self) {
        self.core_.rx_closed_.store(true, Ordering::Release);
        self.core_.notify_all_();
    }

    /// 通道是否已关闭（任一方向）。
    pub fn is_closed(&self) -> bool {
        self.core_.is_closed_()
    }

    /// 容量 `N`。
    pub fn capacity(&self) -> usize {
        self.core_.capacity_()
    }

    /// 当前可观测到的元素数（近似值，包含已领取但尚未发布的位置）。
    pub fn len(&self) -> usize {
        self.core_.len_()
    }

    /// 当前是否可观测为空（近似判断，与 [`Self::len`] 同源）。
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'a, T, const N: usize, B, C> Clone for Receiver<'a, T, N, B, C>
where
    B: BorrowMut<MpmcBuf<T, N>>,
    C: TrConfig + 'a,
{
    fn clone(&self) -> Self {
        self.core_.receivers_.fetch_add(1, Ordering::Relaxed);
        Receiver {
            core_: Arc::clone(&self.core_),
        }
    }
}

impl<'a, T, const N: usize, B, C> Drop for Receiver<'a, T, N, B, C>
where
    B: BorrowMut<MpmcBuf<T, N>>,
    C: TrConfig + 'a,
{
    fn drop(&mut self) {
        let old = self.core_.receivers_.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(old > 0, "Receiver 句柄计数下溢");
        if old == 1 {
            self.core_.rx_closed_.store(true, Ordering::Release);
            self.core_.notify_all_();
        }
    }
}

/// 异步接收流程；由 `gen_mcf2` 生成 `RecvAsync` / `RecvFuture` 两件套。
#[gen_may_cancel_future(Recv, pub, new(pub(crate)))]
async fn recv_async_<'a, 'f, T, B, const N: usize, Cfg, C>(
    receiver: &'f Receiver<'a, T, N, B, Cfg>,
    cancel: C,
) -> Result<T, RecvError>
where
    'a: 'f,
    B: BorrowMut<MpmcBuf<T, N>>,
    Cfg: TrConfig + 'a,
    C: TrCancellationToken,
{
    if cancel.is_cancelled() {
        return Result::Err(RecvError::Cancelled);
    }

    // 快路径：无锁 CAS 一次 Vyukov pop。关闭后仍要先尝试 drain。
    match receiver.core_.try_pop_fast_() {
        TryPop_::Value(value) => {
            receiver.core_.notify_prod_();
            return Result::Ok(value);
        }
        TryPop_::Empty => {
            if receiver.core_.is_closed_() {
                return Result::Err(RecvError::Closed);
            }
        }
        // 头槽已预留但生产者尚未发布：即使已关闭也要等发布，不能直接判 Closed。
        TryPop_::Pending => {}
    }

    // P1：快路径失败后先做有界自旋，抓生产者刚发布的元素。每轮都是直接无锁
    // 尝试；预算有界，每 `CANCEL_CHECK_EVERY` 次检查取消。
    // `Pending` + 已关闭仍要继续自旋/慢路径，等对向发布在途元素。
    {
        metric_!(SPIN_POP_ENTER);
        for spin in 0..Cfg::FAST_SPIN {
            if spin % CANCEL_CHECK_EVERY == 0 && cancel.is_cancelled() {
                metric_!(POP_SLOW_CANCEL);
                return Result::Err(RecvError::Cancelled);
            }
            match receiver.core_.try_pop_fast_() {
                TryPop_::Value(value) => {
                    metric_!(SPIN_POP_OK);
                    receiver.core_.notify_prod_();
                    return Result::Ok(value);
                }
                TryPop_::Empty => {
                    if receiver.core_.is_closed_() {
                        metric_!(POP_SLOW_CLOSED);
                        return Result::Err(RecvError::Closed);
                    }
                }
                TryPop_::Pending => {}
            }
            core::hint::spin_loop();
        }
    }

    metric_!(POP_SLOW_ENTER);

    // 慢路径：进入消费方向等待队列。节点由 `WaitGuard` 持有，future 被取消/
    // 丢弃时自动归还 SlabList。
    let mut guard: Option<WaitGuard<'_, Cfg::ConsWaitSlab>> = Option::None;
    loop {
        if cancel.is_cancelled() {
            metric_!(POP_SLOW_CANCEL);
            return Result::Err(RecvError::Cancelled);
        }

        match receiver.core_.buf_.borrow().try_pop_state_() {
            Pop_::Value(value) => {
                if let Option::Some(mut wait) = guard.take() {
                    wait.finish_();
                }
                metric_!(POP_SLOW_OK);
                receiver.core_.notify_prod_();
                return Result::Ok(value);
            }
            // 头槽已预留但尚未发布：等待生产者发布，不要在这里判 Closed。
            Pop_::Pending => {}
            Pop_::Empty => {
                if receiver.core_.is_closed_() {
                    metric_!(POP_SLOW_CLOSED);
                    return Result::Err(RecvError::Closed);
                }
            }
        }

        // 入队 → 立即复检（入队分支返回 Ready）→ 若仍失败则真正 park。
        let child = cancel.child_token();
        let notified = child
            .cancellation()
            .ok_or(core::future::poll_fn(|cx| {
                let mut finished = false;
                let poll = match guard.as_mut() {
                    Option::None => {
                        guard = Option::Some(WaitGuard::arm_(
                            &receiver.core_.cons_wait_,
                            cx.waker().clone(),
                        ));
                        Poll::Ready(())
                    }
                    Option::Some(wait) => {
                        let poll = wait.poll_(cx);
                        finished = poll.is_ready();
                        match poll {
                            Poll::Ready(()) => metric_!(POP_SLOW_WAKE),
                            Poll::Pending => metric_!(POP_SLOW_PARK),
                        }
                        poll
                    }
                };
                if finished {
                    guard = Option::None;
                }
                poll
            }))
            .await
            .err();

        if notified.is_none() {
            metric_!(POP_SLOW_CANCEL);
            return Result::Err(RecvError::Cancelled);
        }
    }
}
