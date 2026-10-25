//! 消费端句柄与异步接收。

use alloc::sync::Arc;
use core::{
    alloc::AllocatorClone,
    borrow::BorrowMut,
    sync::atomic::Ordering,
    task::Poll,
};


#[cfg(not(feature = "hybrid"))]
use super::util_::CANCEL_CHECK_EVERY;

use abs_cancel::{ok_or::XtOkOr, TrCancellationToken};
use gen_mcf2::gen_may_cancel_future;

use crate::{MpmcBuf, buf_::Pop_};

use super::{
    config_::{DefaultConfig, TrConfig},
    core_::{MpmcChan, TryPop_},
    error_::{RecvError, TryRecvError},
    metrics_::metric_,
};

/// 消费端句柄。
///
/// `Receiver` 可以被 [`Clone`]；每个句柄持有一个 `Arc<MpmcChan>`。最后一个
/// `Receiver` 被 Drop 时会把接收方向标记为关闭，并唤醒两个方向的 park 者。
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
    C: TrConfig,
    C::ProdAlloc: AllocatorClone,
    C::ConsAlloc: AllocatorClone,
{
    core_: Arc<MpmcChan<'a, T, N, B, C>, C::ChanAlloc>,
}

impl<'a, T, const N: usize, B, C> Receiver<'a, T, N, B, C>
where
    B: BorrowMut<MpmcBuf<T, N>>,
    C: TrConfig,
    C::ProdAlloc: AllocatorClone,
    C::ConsAlloc: AllocatorClone,
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
    /// - [`TryRecvError::Empty`]：环空、头槽已预留但生产者尚未发布，或消费方向锁
    ///   当前被慢写者/短临界区占用；
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
        match self.core_.try_pop_locked_() {
            TryPop_::Value(value) => {
                self.core_.not_full_.notify_();
                Result::Ok(value)
            }
            // 方向锁被占用属于 would-block，不能据此断言「已关闭且无元素」：
            // 持锁的慢消费者可能马上把元素 drain 出来。
            TryPop_::Busy | TryPop_::Pending => Result::Err(TryRecvError::Empty),
            TryPop_::Empty => {
                if self.core_.is_closed_() {
                    Result::Err(TryRecvError::Closed)
                } else {
                    Result::Err(TryRecvError::Empty)
                }
            }
        }
    }

    /// 异步接收一个元素；空时进入本方向慢路径等待。
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
        self.core_.notify_both_();
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

impl<T, const N: usize, B, C> Clone for Receiver<'_, T, N, B, C>
where
    B: BorrowMut<MpmcBuf<T, N>>,
    C: TrConfig,
    C::ProdAlloc: AllocatorClone,
    C::ConsAlloc: AllocatorClone,
{
    fn clone(&self) -> Self {
        self.core_.receivers_.fetch_add(1, Ordering::Relaxed);
        Receiver {
            core_: Arc::clone(&self.core_),
        }
    }
}

impl<T, const N: usize, B, C> Drop for Receiver<'_, T, N, B, C>
where
    B: BorrowMut<MpmcBuf<T, N>>,
    C: TrConfig,
    C::ProdAlloc: AllocatorClone,
    C::ConsAlloc: AllocatorClone,
{
    fn drop(&mut self) {
        let old = self.core_.receivers_.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(old > 0, "Receiver 句柄计数下溢");
        if old == 1 {
            self.core_.rx_closed_.store(true, Ordering::Release);
            self.core_.notify_both_();
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
    Cfg: TrConfig,
    Cfg::ProdAlloc: AllocatorClone,
    Cfg::ConsAlloc: AllocatorClone,
    C: TrCancellationToken,
{
    if cancel.is_cancelled() {
        return Result::Err(RecvError::Cancelled);
    }

    // 快路径：读锁 + 一次 Vyukov pop。关闭后仍要先尝试 drain。
    match receiver.core_.try_pop_fast_() {
        TryPop_::Value(value) => {
            receiver.core_.not_full_.notify_();
            return Result::Ok(value);
        }
        TryPop_::Empty => {
            if receiver.core_.is_closed_() {
                return Result::Err(RecvError::Closed);
            }
        }
        // 方向锁被占用，或头槽已预留但生产者尚未发布：即使已关闭也要走慢路径，
        // 等待在途元素发布/排在持锁的慢消费者之后 drain。
        TryPop_::Busy | TryPop_::Pending => {}
    }

    // P1：方案 C 的快路径失败后先做有界自旋，抓生产者刚发布的元素，避免排队成
    // 慢写者。每轮仍走 `try_pop_fast_` 的读锁，不插队到慢写者队列前面；
    // `Pending` + 已关闭仍要继续自旋/慢路径，等对向发布在途元素，不能判 Closed。
    //
    // 方案 D 不编译这段：`try_pop_fast_` 首次失败就会置拥塞位，自旋没有可成功
    // 窗口，直接走慢路径更省。
    #[cfg(not(feature = "hybrid"))]
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
                    receiver.core_.not_full_.notify_();
                    return Result::Ok(value);
                }
                TryPop_::Empty => {
                    if receiver.core_.is_closed_() {
                        metric_!(POP_SLOW_CLOSED);
                        return Result::Err(RecvError::Closed);
                    }
                }
                // 方向锁忙 / 头槽预留未发布：继续自旋，等待对向推进。
                TryPop_::Busy | TryPop_::Pending => {}
            }
            core::hint::spin_loop();
        }
    }

    // 方案 D：进入慢路径前先置拥塞位，挡住后续异步快路径。
    #[cfg(feature = "hybrid")]
    receiver.core_.set_cons_congested_();

    metric_!(POP_SLOW_ENTER);

    // 慢路径：排队为消费者写者。
    let mut sess = receiver.core_.cons_gate_.acquire_session();
    // 默认构建保持原始 `write_async`；只有统计等锁 park 的 metrics 构建才加
    // 一层 poll_fn，避免为计数给正式路径增加间接调用。
    #[cfg(mpmc_metrics)]
    let wait_result = {
        let child = cancel.child_token();
        let mut wait = core::pin::pin!(core::future::IntoFuture::into_future(
            sess.write_async()
        ));
        child
            .cancellation()
            .ok_or(core::future::poll_fn(|cx| {
                let poll = core::future::Future::poll(wait.as_mut(), cx);
                match poll {
                    Poll::Pending => metric_!(POP_SLOW_LOCK_PARK),
                    Poll::Ready(_) => metric_!(POP_SLOW_LOCK_READY),
                }
                poll
            }))
            .await
            .err()
    };
    #[cfg(not(mpmc_metrics))]
    let wait_result = {
        let child = cancel.child_token();
        child
            .cancellation()
            .ok_or(core::future::IntoFuture::into_future(sess.write_async()))
            .await
            .err()
    };
    let Some(result) = wait_result else {
        metric_!(POP_SLOW_CANCEL);
        return Result::Err(RecvError::Cancelled);
    };
    let guard = match result {
        Result::Ok(guard) => guard,
        Result::Err(_) => {
            metric_!(POP_SLOW_CANCEL);
            return Result::Err(RecvError::Cancelled);
        }
    };

    loop {
        match receiver.core_.buf_.borrow().try_pop_state_() {
            Pop_::Value(value) => {
                drop(guard);
                // 方案 D：只有拿到写锁（说明没有其他慢消费者排队）才清拥塞位。
                #[cfg(feature = "hybrid")]
                receiver.core_.clear_cons_congested_();
                metric_!(POP_SLOW_OK);
                receiver.core_.not_full_.notify_();
                return Result::Ok(value);
            }
            // 头槽已预留但尚未发布：等待生产者发布，不要在这里判 Closed。
            Pop_::Pending => {}
            Pop_::Empty => {
                // 先尝试 drain，再判断关闭：关闭后仍允许把已入队元素取完。
                if receiver.core_.is_closed_() {
                    metric_!(POP_SLOW_CLOSED);
                    return Result::Err(RecvError::Closed);
                }
            }
        }

        let child = cancel.child_token();
        let notified = child
            .cancellation()
            .ok_or(core::future::poll_fn(|cx| {
                let poll = receiver.core_.not_empty_.poll_wait_(cx);
                match poll {
                    Poll::Pending => metric_!(POP_SLOW_PARK),
                    Poll::Ready(()) => metric_!(POP_SLOW_WAKE),
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
