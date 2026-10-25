//! 生产端句柄与异步发送。

use alloc::sync::Arc;
use core::sync::atomic::Ordering;

use abs_cancel::TrCancellationToken;
use gen_mcf2::gen_may_cancel_future;

use super::{
    core_::MpmcChan,
    error_::{SendError, TrySendError},
    util_::race_cancel_into_,
};

/// 生产端句柄。
///
/// `Sender` 可以被 [`Clone`]；每个句柄持有一个 `Arc<MpmcChan>`。最后一个 `Sender`
/// 被 Drop 时会把发送方向标记为关闭，并唤醒两个方向的 park 者。
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
pub struct Sender<'a, T, const N: usize> {
    core_: Arc<MpmcChan<'a, T, N>>,
}

impl<'a, T, const N: usize> Sender<'a, T, N> {
    /// 内部构造：增加发送端句柄计数。
    pub(super) fn new_(core_: Arc<MpmcChan<'a, T, N>>) -> Self {
        core_.senders_.fetch_add(1, Ordering::Relaxed);
        Sender { core_ }
    }

    /// 尝试立即发送一个元素。
    ///
    /// # Errors
    ///
    /// - [`TrySendError::Full`]：环满、目标槽尚未回收，或生产方向锁当前被
    ///   慢写者/短临界区占用；元素原样返回。
    /// - [`TrySendError::Closed`]：接收侧已关闭，或发送侧已显式关闭；元素原样返回。
    ///
    /// # Examples
    ///
    /// ```
    /// use mpmc_chan::{MpmcBuf, MpmcChan, TrySendError};
    ///
    /// let buf = MpmcBuf::<u8, 2>::new();
    /// let (tx, rx) = MpmcChan::new(&buf).unwrap();
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

        match self.core_.try_push_fast_(item) {
            Result::Ok(()) => {
                self.core_.not_empty_.notify_();
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

    /// 异步发送一个元素；满时进入本方向慢路径等待。
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
    /// let buf = MpmcBuf::<u8, 2>::new();
    /// let (tx, rx) = MpmcChan::new(&buf).unwrap();
    /// assert_eq!(block_on(async { tx.send_async(7).await }), Ok(()));
    /// assert_eq!(rx.try_recv(), Ok(7));
    /// ```
    pub fn send_async<'f>(&'f self, item: T) -> SendAsync<'a, 'f, 'f, T, N>
    where
        'a: 'f,
    {
        SendAsync::new(self, item)
    }

    /// 关闭发送方向：此后所有发送返回 `Closed`；接收侧仍可 drain 已入队元素。
    pub fn close(&self) {
        self.core_.tx_closed_.store(true, Ordering::Release);
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

impl<T, const N: usize> Clone for Sender<'_, T, N> {
    fn clone(&self) -> Self {
        self.core_.senders_.fetch_add(1, Ordering::Relaxed);
        Sender {
            core_: Arc::clone(&self.core_),
        }
    }
}

impl<T, const N: usize> Drop for Sender<'_, T, N> {
    fn drop(&mut self) {
        let old = self.core_.senders_.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(old > 0, "Sender 句柄计数下溢");
        if old == 1 {
            self.core_.tx_closed_.store(true, Ordering::Release);
            self.core_.notify_both_();
        }
    }
}

/// 异步发送流程；由 `gen_mcf2` 生成 `SendAsync` / `SendFuture` 两件套。
#[gen_may_cancel_future(Send, pub, new(pub(crate)))]
async fn send_async_<'a, 'f, T, const N: usize, C>(
    sender: &'f Sender<'a, T, N>,
    item: T,
    cancel: C,
) -> Result<(), SendError<T>>
where
    'a: 'f,
    C: TrCancellationToken,
{
    if cancel.is_cancelled() {
        return Result::Err(SendError::Cancelled(item));
    }
    if sender.core_.is_closed_() {
        return Result::Err(SendError::Closed(item));
    }

    // 快路径：读锁 + 一次 Vyukov push。
    let mut item = item;
    match sender.core_.try_push_fast_(item) {
        Result::Ok(()) => {
            sender.core_.not_empty_.notify_();
            return Result::Ok(());
        }
        Result::Err(value) => item = value,
    }

    // 慢路径：排队为写者，拿到写锁后如果仍满就持锁 park。
    let mut sess = sender.core_.prod_gate_.acquire_session();
    let Some(result) = race_cancel_into_(&cancel, sess.write_async()).await else {
        return Result::Err(SendError::Cancelled(item));
    };
    let guard = match result {
        Result::Ok(guard) => guard,
        Result::Err(_) => return Result::Err(SendError::Cancelled(item)),
    };

    loop {
        if sender.core_.is_closed_() {
            return Result::Err(SendError::Closed(item));
        }

        match sender.core_.buf_.try_push_(item) {
            Result::Ok(()) => {
                drop(guard);
                sender.core_.not_empty_.notify_();
                return Result::Ok(());
            }
            Result::Err(value) => item = value,
        }

        // 环仍满（或目标槽尚未回收）：登记 not_full 并 park。
        // 取消时 race_cancel_into_ 丢弃等待 future，guard 在返回时释放。
        let notified = race_cancel_into_(
            &cancel,
            core::future::poll_fn(|cx| sender.core_.not_full_.poll_wait_(cx)),
        )
        .await;
        if notified.is_none() {
            return Result::Err(SendError::Cancelled(item));
        }
    }
}
