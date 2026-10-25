//! 通道共享内核：两把方向锁、两个单等待者槽、关闭标志与句柄计数。

use alloc::sync::Arc;
use core::{
    borrow::BorrowMut,
    marker::PhantomData,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use atomic_sync::rwlock::cooperative_arena::CooperativeRwLockOwned;

use crate::{MpmcBuf, buf_::Pop_};

use super::{receiver_::Receiver, sender_::Sender, waker_::WakerSlot};

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
/// # Buffer 所有权
///
/// `B` 是 buffer 的所有权载体，必须实现 `BorrowMut<MpmcBuf<T, N>>`：
///
/// - owned `MpmcBuf` / `Box<MpmcBuf>`：channel 拥有 buffer；
/// - `&'a mut MpmcBuf`：channel 借用 buffer。
///
/// 两种情况都由 Rust 的所有权/独占借用保证「一个 buffer 只有一个 channel」，
/// 不需要运行时 claim 标志。不可变 `static BUF: MpmcBuf` 无法安全地产生 `&mut`，
/// 需要 `static mut` 或 `Box::leak` 一类手段；这是 `BorrowMut` 方案的代价。
///
/// # Examples
///
/// ```
/// use mpmc_chan::{MpmcBuf, MpmcChan};
///
/// let mut buf = MpmcBuf::<u32, 4>::new();
/// let (tx, rx) = MpmcChan::new(&mut buf);
/// tx.try_send(1).unwrap();
/// assert_eq!(rx.try_recv(), Ok(1));
/// ```
pub struct MpmcChan<'a, T, const N: usize, B = &'a mut MpmcBuf<T, N>>
where
    B: BorrowMut<MpmcBuf<T, N>>,
{
    pub(super) buf_: B,
    pub(super) prod_gate_: CooperativeRwLockOwned<()>,
    pub(super) cons_gate_: CooperativeRwLockOwned<()>,
    pub(super) not_full_: WakerSlot,
    pub(super) not_empty_: WakerSlot,
    pub(super) senders_: AtomicUsize,
    pub(super) receivers_: AtomicUsize,
    pub(super) tx_closed_: AtomicBool,
    pub(super) rx_closed_: AtomicBool,
    /// 让 `'a` 在 owned buffer（例如 `Box<MpmcBuf>`）时也有使用点。
    pub(super) _lt_: PhantomData<&'a MpmcBuf<T, N>>,
}

impl<'a, T, const N: usize, B> MpmcChan<'a, T, N, B>
where
    B: BorrowMut<MpmcBuf<T, N>>,
{
    /// 用 buffer 构造一对通道句柄。
    ///
    /// `buf` 可以是 owned `MpmcBuf`、`Box<MpmcBuf>`，也可以是 `&mut MpmcBuf`。
    /// 构造与 Drop 都会 drain + reset buffer。独占性由所有权/`&mut` 借用保证，
    /// 因此不存在运行时「buffer 已被占用」这一类错误。
    ///
    /// # Examples
    ///
    /// ```
    /// use mpmc_chan::{MpmcBuf, MpmcChan};
    ///
    /// let mut buf = MpmcBuf::<u8, 2>::new();
    /// let (tx, rx) = MpmcChan::new(&mut buf);
    /// tx.try_send(1).unwrap();
    /// assert_eq!(rx.try_recv(), Ok(1));
    /// ```
    // 该构造器按设计返回一对句柄，而不是 `Self`；`new` 这个名字对使用方最直观。
    #[allow(clippy::new_ret_no_self)]
    pub fn new(mut buf: B) -> (Sender<'a, T, N, B>, Receiver<'a, T, N, B>) {
        buf.borrow_mut().drop_remaining_();

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
            _lt_: PhantomData,
        });

        let tx = Sender::new_(Arc::clone(&core));
        let rx = Receiver::new_(Arc::clone(&core));
        (tx, rx)
    }

    /// 通道是否已经关闭（任一方向显式 close，或对应方向的最后一个句柄消失）。
    #[inline]
    pub(super) fn is_closed_(&self) -> bool {
        self.tx_closed_.load(Ordering::Acquire) || self.rx_closed_.load(Ordering::Acquire)
    }

    /// 当前可观测到的元素数（近似值）。
    #[inline]
    pub(super) fn len_(&self) -> usize {
        self.buf_.borrow().len_()
    }

    /// 容量 `N`。
    #[inline]
    pub(super) fn capacity_(&self) -> usize {
        self.buf_.borrow().capacity()
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
        self.buf_.borrow().try_push_(item)
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
        match self.buf_.borrow().try_pop_state_() {
            Pop_::Value(value) => TryPop_::Value(value),
            Pop_::Empty => TryPop_::Empty,
            Pop_::Pending => TryPop_::Pending,
        }
    }
}

impl<T, const N: usize, B> Drop for MpmcChan<'_, T, N, B>
where
    B: BorrowMut<MpmcBuf<T, N>>,
{
    fn drop(&mut self) {
        // 走到这里说明所有 Sender/Receiver/Future 都已经消失，不可能有在途操作。
        // drop_remaining_ 会 drain 已发布未消费的元素并 reset。
        self.buf_.borrow_mut().drop_remaining_();
    }
}
