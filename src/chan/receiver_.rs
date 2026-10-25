//! 消费端句柄与异步接收。

use alloc::sync::Arc;
use core::sync::atomic::Ordering;

use abs_cancel::TrCancellationToken;
use gen_mcf2::gen_may_cancel_future;

use crate::buf_::Pop_;

use super::{
    core_::{MpmcChan, TryPop_},
    error_::{RecvError, TryRecvError},
    util_::race_cancel_into_,
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
/// let buf = MpmcBuf::<u32, 2>::new();
/// let (tx, rx) = MpmcChan::new(&buf).expect("首次构造应成功");
/// tx.try_send(1).unwrap();
/// assert_eq!(rx.try_recv(), Ok(1));
/// ```
pub struct Receiver<'a, T, const N: usize> {
    core_: Arc<MpmcChan<'a, T, N>>,
}

impl<'a, T, const N: usize> Receiver<'a, T, N> {
    /// 内部构造：增加接收端句柄计数。
    pub(super) fn new_(core_: Arc<MpmcChan<'a, T, N>>) -> Self {
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
    /// let buf = MpmcBuf::<u8, 2>::new();
    /// let (tx, rx) = MpmcChan::new(&buf).unwrap();
    /// assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    /// tx.try_send(1).unwrap();
    /// assert_eq!(rx.try_recv(), Ok(1));
    /// drop(tx);
    /// assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
    /// ```
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self.core_.try_pop_fast_() {
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
    /// let buf = MpmcBuf::<u8, 2>::new();
    /// let (tx, rx) = MpmcChan::new(&buf).unwrap();
    /// tx.try_send(7).unwrap();
    /// assert_eq!(block_on(async { rx.recv_async().await }), Ok(7));
    /// ```
    pub fn recv_async<'f>(&'f self) -> RecvAsync<'a, 'f, 'f, T, N>
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

impl<T, const N: usize> Clone for Receiver<'_, T, N> {
    fn clone(&self) -> Self {
        self.core_.receivers_.fetch_add(1, Ordering::Relaxed);
        Receiver {
            core_: Arc::clone(&self.core_),
        }
    }
}

impl<T, const N: usize> Drop for Receiver<'_, T, N> {
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
async fn recv_async_<'a, 'f, T, const N: usize, C>(
    receiver: &'f Receiver<'a, T, N>,
    cancel: C,
) -> Result<T, RecvError>
where
    'a: 'f,
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

    // 慢路径：排队为消费者写者。
    let mut sess = receiver.core_.cons_gate_.acquire_session();
    let Some(result) = race_cancel_into_(&cancel, sess.write_async()).await else {
        return Result::Err(RecvError::Cancelled);
    };
    let guard = match result {
        Result::Ok(guard) => guard,
        Result::Err(_) => return Result::Err(RecvError::Cancelled),
    };

    loop {
        match receiver.core_.buf_.try_pop_state_() {
            Pop_::Value(value) => {
                drop(guard);
                receiver.core_.not_full_.notify_();
                return Result::Ok(value);
            }
            // 头槽已预留但尚未发布：等待生产者发布，不要在这里判 Closed。
            Pop_::Pending => {}
            Pop_::Empty => {
                // 先尝试 drain，再判断关闭：关闭后仍允许把已入队元素取完。
                if receiver.core_.is_closed_() {
                    return Result::Err(RecvError::Closed);
                }
            }
        }

        let notified = race_cancel_into_(
            &cancel,
            core::future::poll_fn(|cx| receiver.core_.not_empty_.poll_wait_(cx)),
        )
        .await;
        if notified.is_none() {
            return Result::Err(RecvError::Cancelled);
        }
    }
}
