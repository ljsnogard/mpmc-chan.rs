//! 通道共享内核：两把方向锁、两个单等待者槽、关闭标志与句柄计数。

use alloc::sync::Arc;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use atomic_sync::rwlock::cooperative_arena::CooperativeRwLockOwned;

use crate::{MpmcBuf, buf_::Pop_};

use super::{error_::ChanBuildError, receiver_::Receiver, sender_::Sender, waker_::WakerSlot};

/// 快路径出队结果。
///
/// 不能把「方向锁被占用」与「环空」混成一个 `None`：关闭后仍要 drain 的语义下，
/// 前者应继续走慢路径，后者才可以返回 `Closed`。
pub(super) enum TryPop_<T> {
    /// 取到一个元素。
    Value(T),
    /// 拿到了消费读锁，但环里当前没有可读元素（真空）。
    Empty,
    /// 拿到了消费读锁，头槽已预留但生产者尚未发布；应等待而不是判定关闭。
    Pending,
    /// 消费方向锁当前被慢写者/短临界区占用，快路径没有进入。
    Busy,
}

/// 双锁 MPMC 通道的共享内核。
///
/// 用户通常不直接持有它：[`MpmcChan::new`] 返回一对
/// [`Sender`] / [`Receiver`] 句柄，句柄内部各持一个 `Arc<MpmcChan>`。
///
/// # 锁拓扑
///
/// - `prod_gate_`：只由生产者获取。快路径取读锁；慢路径取写锁并在满时持锁 park。
/// - `cons_gate_`：只由消费者获取，结构对称。
///
/// 两把锁互不嵌套；发送路径不取消费锁，接收路径不取生产锁。
///
/// # 生命周期
///
/// 内核借用 `&'a MpmcBuf`；所有句柄与 Future 都带 `'a`，因此 buffer 一定比
/// channel 活得久。buffer 的独占由 [`MpmcBuf`] 内部的 `claimed` 原子位保证。
///
/// # Examples
///
/// ```
/// use mpmc_chan::{MpmcBuf, MpmcChan};
///
/// static BUF: MpmcBuf<u32, 4> = MpmcBuf::new();
///
/// let (tx, rx) = MpmcChan::new(&BUF).expect("buffer 已被占用");
/// tx.try_send(1).unwrap();
/// assert_eq!(rx.try_recv(), Ok(1));
/// ```
pub struct MpmcChan<'a, T, const N: usize> {
    pub(super) buf_: &'a MpmcBuf<T, N>,
    pub(super) prod_gate_: CooperativeRwLockOwned<()>,
    pub(super) cons_gate_: CooperativeRwLockOwned<()>,
    pub(super) not_full_: WakerSlot,
    pub(super) not_empty_: WakerSlot,
    pub(super) senders_: AtomicUsize,
    pub(super) receivers_: AtomicUsize,
    pub(super) tx_closed_: AtomicBool,
    pub(super) rx_closed_: AtomicBool,
}

impl<'a, T, const N: usize> MpmcChan<'a, T, N> {
    /// 借用 `buf` 构造一对通道句柄。
    ///
    /// 成功时会对 buffer 做运行时 claim + reset；失败时返回
    /// [`ChanBuildError::BufferInUse`]，不会触碰 buffer 状态。
    ///
    /// # Errors
    ///
    /// 当同一个 `MpmcBuf` 已经有一个存活的 channel 时返回 `BufferInUse`。
    ///
    /// # Examples
    ///
    /// ```
    /// use mpmc_chan::{ChanBuildError, MpmcBuf, MpmcChan};
    ///
    /// let buf = MpmcBuf::<u8, 2>::new();
    /// let (tx, rx) = MpmcChan::new(&buf).expect("首次构造应成功");
    /// assert!(matches!(
    ///     MpmcChan::new(&buf),
    ///     Err(ChanBuildError::BufferInUse)
    /// ));
    /// drop(tx);
    /// drop(rx);
    /// ```
    // 该构造器按设计返回一对句柄，而不是 `Self`；`new` 这个名字对使用方最直观。
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        buf: &'a MpmcBuf<T, N>,
    ) -> Result<(Sender<'a, T, N>, Receiver<'a, T, N>), ChanBuildError> {
        if !buf.claim_() {
            return Result::Err(ChanBuildError::BufferInUse);
        }
        buf.reset_();

        let core = Arc::new(MpmcChan {
            buf_: buf,
            prod_gate_: CooperativeRwLockOwned::<()>::new_owned(()),
            cons_gate_: CooperativeRwLockOwned::<()>::new_owned(()),
            not_full_: WakerSlot::new_(),
            not_empty_: WakerSlot::new_(),
            senders_: AtomicUsize::new(0),
            receivers_: AtomicUsize::new(0),
            tx_closed_: AtomicBool::new(false),
            rx_closed_: AtomicBool::new(false),
        });

        let tx = Sender::new_(Arc::clone(&core));
        let rx = Receiver::new_(Arc::clone(&core));
        Result::Ok((tx, rx))
    }

    /// 通道是否已经关闭（任一方向显式 close，或对应方向的最后一个句柄消失）。
    #[inline]
    pub(super) fn is_closed_(&self) -> bool {
        self.tx_closed_.load(Ordering::Acquire) || self.rx_closed_.load(Ordering::Acquire)
    }

    /// 当前可观测到的元素数（近似值）。
    #[inline]
    pub(super) fn len_(&self) -> usize {
        self.buf_.len_()
    }

    /// 容量 `N`。
    #[inline]
    pub(super) fn capacity_(&self) -> usize {
        self.buf_.capacity()
    }

    /// 通知两个方向的 park 者。任一方向关闭或状态可能变化时调用。
    #[inline]
    pub(super) fn notify_both_(&self) {
        self.not_full_.notify_();
        self.not_empty_.notify_();
    }

    /// 生产者快路径：取生产读锁，尝试一次入队。
    ///
    /// 失败时把元素原样还回，由调用方决定走慢路径还是返回 would-block。
    #[inline]
    pub(super) fn try_push_fast_(&self, item: T) -> Result<(), T> {
        let mut sess = self.prod_gate_.acquire_session();
        let Result::Ok(_guard) = sess.try_read() else {
            return Result::Err(item);
        };
        self.buf_.try_push_(item)
    }

    /// 消费者快路径：取消费读锁，尝试一次出队。
    ///
    /// 返回 [`TryPop_::Busy`] 表示方向锁没拿到，调用方应走慢路径（即使通道已关闭，
    /// 也要让慢路径去 drain 可能存在的元素）。
    #[inline]
    pub(super) fn try_pop_fast_(&self) -> TryPop_<T> {
        let mut sess = self.cons_gate_.acquire_session();
        let Result::Ok(_guard) = sess.try_read() else {
            return TryPop_::Busy;
        };
        match self.buf_.try_pop_state_() {
            Pop_::Value(value) => TryPop_::Value(value),
            Pop_::Empty => TryPop_::Empty,
            Pop_::Pending => TryPop_::Pending,
        }
    }
}

impl<T, const N: usize> Drop for MpmcChan<'_, T, N> {
    fn drop(&mut self) {
        // 走到这里说明所有 Sender/Receiver/Future 都已经消失，不可能有在途操作。
        // drop_remaining_ 会 drain 已发布未消费的元素并 reset；随后释放 claim。
        self.buf_.drop_remaining_();
        self.buf_.release_();
    }
}
