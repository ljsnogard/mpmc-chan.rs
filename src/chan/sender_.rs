//! 生产端句柄与异步发送。

use alloc::sync::Arc;
use core::{borrow::BorrowMut, sync::atomic::Ordering, task::Poll};

use abs_cancel::{ok_or::XtOkOr, TrCancellationToken};
use gen_mcf2::gen_may_cancel_future;

use crate::MpmcBuf;

use super::{
    config_::{DefaultConfig, TrConfig},
    core_::MpmcChan,
    error_::{SendError, TrySendError},
    metrics_::metric_,
    util_::CANCEL_CHECK_EVERY,
    wait_queue_::WaitGuard,
};

/// 生产端句柄。
///
/// `Sender` 可以被 [`Clone`]；每个句柄持有一个 `Arc<MpmcChan>`。最后一个 `Sender`
/// 被 Drop 时会把发送方向标记为关闭，并唤醒两个方向的全部等待者。
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
pub struct Sender<'a, T, const N: usize, B = &'a mut MpmcBuf<T, N>, C = DefaultConfig>
where
    B: BorrowMut<MpmcBuf<T, N>>,
    C: TrConfig + 'a,
{
    core_: Arc<MpmcChan<'a, T, N, B, C>, C::ChanAlloc>,
}

impl<'a, T, const N: usize, B, C> Sender<'a, T, N, B, C>
where
    B: BorrowMut<MpmcBuf<T, N>>,
    C: TrConfig + 'a,
{
    /// 内部构造：增加发送端句柄计数。
    pub(super) fn new_(core_: Arc<MpmcChan<'a, T, N, B, C>, C::ChanAlloc>) -> Self {
        core_.senders_.fetch_add(1, Ordering::Relaxed);
        Sender { core_ }
    }

    /// 尝试立即发送一个元素。
    ///
    /// # Errors
    ///
    /// - [`TrySendError::Full`]：环满、目标槽尚未回收；元素原样返回。
    /// - [`TrySendError::Closed`]：接收侧已关闭，或发送侧已显式关闭；元素原样返回。
    ///
    /// # Examples
    ///
    /// ```
    /// use mpmc_chan::{MpmcBuf, MpmcChan, TrySendError};
    ///
    /// let mut buf = MpmcBuf::<u8, 2>::new();
    /// let (tx, rx) = MpmcChan::new(&mut buf);
    /// assert_eq!(tx.try_send(1), Ok(()));
    /// assert_eq!(tx.try_send(2), Ok(()));
    /// assert_eq!(tx.try_send(3), Err(TrySendError::Full(3)));
    /// drop(rx);
    /// assert_eq!(tx.try_send(4), Err(TrySendError::Closed(4)));
    /// ```
    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        if self.core_.is_closed_() {
            return Result::Err(TrySendError::Closed(item));
        }

        match self.core_.try_push_try_(item) {
            Result::Ok(()) => {
                self.core_.notify_cons_();
                Result::Ok(())
            }
            Result::Err(item) => {
                if self.core_.is_closed_() {
                    Result::Err(TrySendError::Closed(item))
                } else {
                    Result::Err(TrySendError::Full(item))
                }
            }
        }
    }

    /// 异步发送一个元素；满时进入本方向等待队列。
    ///
    /// 返回的 [`SendAsync`] 可以直接 `.await`，也可以先
    /// `.may_cancel_with(token)` 换成可取消版本。
    ///
    /// # Errors
    ///
    /// - [`SendError::Closed`]：接收侧已关闭，元素原样返回；
    /// - [`SendError::Cancelled`]：等待被取消且元素尚未入队，元素原样返回。
    ///
    /// # Examples
    ///
    /// ```
    /// use futures_lite::future::block_on;
    /// use mpmc_chan::{MpmcBuf, MpmcChan};
    ///
    /// let mut buf = MpmcBuf::<u8, 2>::new();
    /// let (tx, rx) = MpmcChan::new(&mut buf);
    /// assert_eq!(block_on(async { tx.send_async(7).await }), Ok(()));
    /// assert_eq!(rx.try_recv(), Ok(7));
    /// ```
    pub fn send_async<'f>(&'f self, item: T) -> SendAsync<'a, 'f, 'f, T, B, C, N>
    where
        'a: 'f,
    {
        SendAsync::new(self, item)
    }

    /// 关闭发送方向：此后所有发送返回 `Closed`；接收侧仍可 drain 已入队元素。
    pub fn close(&self) {
        self.core_.tx_closed_.store(true, Ordering::Release);
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

impl<'a, T, const N: usize, B, C> Clone for Sender<'a, T, N, B, C>
where
    B: BorrowMut<MpmcBuf<T, N>>,
    C: TrConfig + 'a,
{
    fn clone(&self) -> Self {
        self.core_.senders_.fetch_add(1, Ordering::Relaxed);
        Sender {
            core_: Arc::clone(&self.core_),
        }
    }
}

impl<'a, T, const N: usize, B, C> Drop for Sender<'a, T, N, B, C>
where
    B: BorrowMut<MpmcBuf<T, N>>,
    C: TrConfig + 'a,
{
    fn drop(&mut self) {
        let old = self.core_.senders_.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(old > 0, "Sender 句柄计数下溢");
        if old == 1 {
            self.core_.tx_closed_.store(true, Ordering::Release);
            self.core_.notify_all_();
        }
    }
}

/// 异步发送流程；由 `gen_mcf2` 生成 `SendAsync` / `SendFuture` 两件套。
#[gen_may_cancel_future(Send, pub, new(pub(crate)))]
async fn send_async_<'a, 'f, T, B, const N: usize, Cfg, C>(
    sender: &'f Sender<'a, T, N, B, Cfg>,
    item: T,
    cancel: C,
) -> Result<(), SendError<T>>
where
    'a: 'f,
    B: BorrowMut<MpmcBuf<T, N>>,
    Cfg: TrConfig + 'a,
    C: TrCancellationToken,
{
    if cancel.is_cancelled() {
        return Result::Err(SendError::Cancelled(item));
    }
    if sender.core_.is_closed_() {
        return Result::Err(SendError::Closed(item));
    }

    // 快路径：无锁 CAS 一次 Vyukov push。
    let mut item = item;
    match sender.core_.try_push_fast_(item) {
        Result::Ok(()) => {
            sender.core_.notify_cons_();
            return Result::Ok(());
        }
        Result::Err(value) => item = value,
    }

    // P1：快路径失败后先做有界自旋，抓对向刚释放的槽位，避免立即进入慢路径。
    // 每轮都是直接无锁尝试，不再需要读锁；预算有界，每 `CANCEL_CHECK_EVERY`
    // 次检查取消/关闭，保持取消可观测。
    {
        metric_!(SPIN_PUSH_ENTER);
        for spin in 0..Cfg::FAST_SPIN {
            if spin % CANCEL_CHECK_EVERY == 0 {
                if cancel.is_cancelled() {
                    metric_!(PUSH_SLOW_CANCEL);
                    return Result::Err(SendError::Cancelled(item));
                }
                if sender.core_.is_closed_() {
                    metric_!(PUSH_SLOW_CLOSED);
                    return Result::Err(SendError::Closed(item));
                }
            }
            match sender.core_.try_push_fast_(item) {
                Result::Ok(()) => {
                    metric_!(SPIN_PUSH_OK);
                    sender.core_.notify_cons_();
                    return Result::Ok(());
                }
                Result::Err(value) => item = value,
            }
            core::hint::spin_loop();
        }
    }

    metric_!(PUSH_SLOW_ENTER);

    // 慢路径：进入生产方向等待队列。节点由 `WaitGuard` 持有，future 被取消/
    // 丢弃时自动归还 SlabList。
    let mut guard: Option<WaitGuard<'_, Cfg::ProdWaitSlab>> = Option::None;
    loop {
        if cancel.is_cancelled() {
            metric_!(PUSH_SLOW_CANCEL);
            return Result::Err(SendError::Cancelled(item));
        }
        if sender.core_.is_closed_() {
            metric_!(PUSH_SLOW_CLOSED);
            return Result::Err(SendError::Closed(item));
        }

        match sender.core_.buf_.borrow().try_push_(item) {
            Result::Ok(()) => {
                if let Option::Some(mut wait) = guard.take() {
                    wait.finish_();
                }
                metric_!(PUSH_SLOW_OK);
                sender.core_.notify_cons_();
                return Result::Ok(());
            }
            Result::Err(value) => item = value,
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
                            &sender.core_.prod_wait_,
                            cx.waker().clone(),
                        ));
                        Poll::Ready(())
                    }
                    Option::Some(wait) => {
                        let poll = wait.poll_(cx);
                        finished = poll.is_ready();
                        match poll {
                            Poll::Ready(()) => metric_!(PUSH_SLOW_WAKE),
                            Poll::Pending => metric_!(PUSH_SLOW_PARK),
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
            metric_!(PUSH_SLOW_CANCEL);
            return Result::Err(SendError::Cancelled(item));
        }
    }
}
