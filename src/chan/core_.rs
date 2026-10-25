//! 通道共享内核：两把方向锁、两个单等待者槽、关闭标志与句柄计数。

use alloc::sync::Arc;
use core::{
    borrow::BorrowMut,
    marker::PhantomData,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use atomic_sync::rwlock::cooperative_arena::CooperativeRwLockOwned;

use crate::{MpmcBuf, buf_::Pop_};

use super::{
    metrics_::metric_, receiver_::Receiver, sender_::Sender, waker_::WakerSlot,
};

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
    /// 消费方向锁当前被慢写者/短临界区占用（方案 C），或拥塞位为真（方案 D）；
    /// 调用方应走慢路径。
    Busy,
}

/// 读锁下的入队结果，供 fast / try 两条路径共用并分别计数。
enum Push_<T> {
    /// 入队成功。
    Ok,
    /// 拿到读锁但环满。
    Full(T),
    /// 方向读锁未拿到。
    Busy(T),
}

/// 双锁 MPMC 通道的共享内核。
///
/// 用户通常不直接持有它：[`MpmcChan::new`] 返回一对
/// [`Sender`] / [`Receiver`] 句柄，句柄内部各持一个 `Arc<MpmcChan>`。
///
/// # 锁拓扑
///
/// - `prod_gate_`：只由生产者获取。默认方案 C 下快路径取读锁；方案 D 下
///   `send_async` 快路径不取锁、只看拥塞位，`try_send` 仍取读锁。慢路径取写锁并在
///   满时持锁 park。
/// - `cons_gate_`：只由消费者获取，结构对称。
///
/// 两把锁互不嵌套；发送路径不取消费锁，接收路径不取生产锁。
///
/// # 方案 D（`hybrid` feature）
///
/// - `send_async` / `recv_async` 快路径只读拥塞位；失败就置位并进入慢路径；
/// - 慢路径成功后在释放方向写锁后，用一次 `try_write` 检查「是否还有慢者排队」，
///   只有队列为空才清拥塞位；
/// - `try_send` / `try_recv` 仍走方向读锁，因此不会被拥塞位卡死，也不插队到
///   慢写者队列前面。
///
/// 方案 D 的公平性是近似的：快路径在拥塞位被置位前已经在途的 CAS 仍可能插队。
/// 它适合用基准验证是否值得，不应在未测之前替换默认方案 C。
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
    /// 方案 D 的生产侧拥塞位；默认方案 C 下不编译。
    #[cfg(feature = "hybrid")]
    pub(super) prod_congested_: AtomicBool,
    /// 方案 D 的消费侧拥塞位。
    #[cfg(feature = "hybrid")]
    pub(super) cons_congested_: AtomicBool,
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
            #[cfg(feature = "hybrid")]
            prod_congested_: AtomicBool::new(false),
            #[cfg(feature = "hybrid")]
            cons_congested_: AtomicBool::new(false),
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

    /// 取生产读锁并尝试一次入队；不计数，由 fast / try 调用方分别记账。
    #[inline]
    fn try_push_read_(&self, item: T) -> Push_<T> {
        let mut sess = self.prod_gate_.acquire_session();
        let Result::Ok(_guard) = sess.try_read() else {
            return Push_::Busy(item);
        };
        match self.buf_.borrow().try_push_(item) {
            Result::Ok(()) => Push_::Ok,
            Result::Err(item) => Push_::Full(item),
        }
    }

    /// 取消费读锁并尝试一次出队；不计数，由 fast / try 调用方分别记账。
    #[inline]
    fn try_pop_read_(&self) -> TryPop_<T> {
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

    /// `try_send` 使用的非阻塞快路径：取生产读锁，尝试一次入队。
    ///
    /// 这条路径在方案 C / D 下都保留，因为它尊重方向写锁的排队：
    /// 有慢生产者持有/等待写锁时，`try_send` 返回 would-block，而不是插队。
    #[inline]
    pub(super) fn try_push_locked_(&self, item: T) -> Result<(), T> {
        metric_!(PUSH_TRY_ATTEMPT);
        match self.try_push_read_(item) {
            Push_::Ok => {
                metric_!(PUSH_TRY_OK);
                Result::Ok(())
            }
            Push_::Full(item) => {
                metric_!(PUSH_TRY_FULL);
                Result::Err(item)
            }
            Push_::Busy(item) => {
                metric_!(PUSH_TRY_BUSY);
                Result::Err(item)
            }
        }
    }

    /// `try_recv` 使用的非阻塞快路径：取消费读锁，尝试一次出队。
    ///
    /// 返回 [`TryPop_::Busy`] 表示方向锁没拿到，调用方应返回 would-block。
    #[inline]
    pub(super) fn try_pop_locked_(&self) -> TryPop_<T> {
        metric_!(POP_TRY_ATTEMPT);
        match self.try_pop_read_() {
            TryPop_::Value(value) => {
                metric_!(POP_TRY_OK);
                TryPop_::Value(value)
            }
            TryPop_::Empty => {
                metric_!(POP_TRY_EMPTY);
                TryPop_::Empty
            }
            TryPop_::Pending => {
                metric_!(POP_TRY_PENDING);
                TryPop_::Pending
            }
            TryPop_::Busy => {
                metric_!(POP_TRY_BUSY);
                TryPop_::Busy
            }
        }
    }

    /// 方案 C：异步快路径与 `try_*` 一样走读锁。
    #[cfg(not(feature = "hybrid"))]
    #[inline]
    pub(super) fn try_push_fast_(&self, item: T) -> Result<(), T> {
        metric_!(PUSH_FAST_ATTEMPT);
        match self.try_push_read_(item) {
            Push_::Ok => {
                metric_!(PUSH_FAST_OK);
                Result::Ok(())
            }
            Push_::Full(item) => {
                metric_!(PUSH_FAST_FULL);
                Result::Err(item)
            }
            Push_::Busy(item) => {
                metric_!(PUSH_FAST_BUSY);
                Result::Err(item)
            }
        }
    }

    /// 方案 C：异步快路径与 `try_*` 一样走读锁。
    #[cfg(not(feature = "hybrid"))]
    #[inline]
    pub(super) fn try_pop_fast_(&self) -> TryPop_<T> {
        metric_!(POP_FAST_ATTEMPT);
        match self.try_pop_read_() {
            TryPop_::Value(value) => {
                metric_!(POP_FAST_OK);
                TryPop_::Value(value)
            }
            TryPop_::Empty => {
                metric_!(POP_FAST_EMPTY);
                TryPop_::Empty
            }
            TryPop_::Pending => {
                metric_!(POP_FAST_PENDING);
                TryPop_::Pending
            }
            TryPop_::Busy => {
                metric_!(POP_FAST_BUSY);
                TryPop_::Busy
            }
        }
    }

    /// 方案 D：异步生产者快路径不取方向锁，只看拥塞位。
    ///
    /// 一旦失败就置拥塞位；后续异步生产者会进入慢路径排队。`try_send` 不走这里，
    /// 因此不会被这个 hint 永久挡住。
    #[cfg(feature = "hybrid")]
    #[inline]
    pub(super) fn try_push_fast_(&self, item: T) -> Result<(), T> {
        metric_!(PUSH_FAST_ATTEMPT);
        if self.prod_congested_.load(Ordering::Acquire) {
            metric_!(PUSH_FAST_BUSY);
            return Result::Err(item);
        }
        match self.buf_.borrow().try_push_(item) {
            Result::Ok(()) => {
                metric_!(PUSH_FAST_OK);
                Result::Ok(())
            }
            Result::Err(item) => {
                metric_!(PUSH_FAST_FULL);
                self.set_prod_congested_();
                Result::Err(item)
            }
        }
    }

    /// 方案 D：异步消费者快路径不取方向锁，只看拥塞位。
    #[cfg(feature = "hybrid")]
    #[inline]
    pub(super) fn try_pop_fast_(&self) -> TryPop_<T> {
        metric_!(POP_FAST_ATTEMPT);
        if self.cons_congested_.load(Ordering::Acquire) {
            metric_!(POP_FAST_BUSY);
            return TryPop_::Busy;
        }
        match self.buf_.borrow().try_pop_state_() {
            Pop_::Value(value) => {
                metric_!(POP_FAST_OK);
                TryPop_::Value(value)
            }
            Pop_::Empty => {
                metric_!(POP_FAST_EMPTY);
                self.set_cons_congested_();
                TryPop_::Empty
            }
            Pop_::Pending => {
                metric_!(POP_FAST_PENDING);
                self.set_cons_congested_();
                TryPop_::Pending
            }
        }
    }

    /// 方案 D：慢路径开始时置生产拥塞位，挡住后续异步快路径。
    #[cfg(feature = "hybrid")]
    #[inline]
    pub(super) fn set_prod_congested_(&self) {
        #[cfg(mpmc_metrics)]
        {
            if !self.prod_congested_.swap(true, Ordering::Release) {
                metric_!(CONGESTED_SET_PROD);
            }
        }
        #[cfg(not(mpmc_metrics))]
        self.prod_congested_.store(true, Ordering::Release);
    }

    /// 方案 D：慢路径成功后尝试清生产拥塞位。
    ///
    /// 只有拿到方向写锁才清：`try_write` 要求队列为空，因此还有慢生产者排队/持锁时
    /// 会失败，拥塞位保持。这样不会让新快路径插到已排队的慢生产者前面。
    #[cfg(feature = "hybrid")]
    #[inline]
    pub(super) fn clear_prod_congested_(&self) {
        if !self.prod_congested_.load(Ordering::Acquire) {
            return;
        }
        let mut sess = self.prod_gate_.acquire_session();
        if let Result::Ok(_guard) = sess.try_write() {
            self.prod_congested_.store(false, Ordering::Release);
            metric_!(CONGESTED_CLEAR_PROD);
        } else {
            metric_!(CONGESTED_CLEAR_FAIL_PROD);
        }
    }

    /// 方案 D：慢路径开始时置消费拥塞位。
    #[cfg(feature = "hybrid")]
    #[inline]
    pub(super) fn set_cons_congested_(&self) {
        #[cfg(mpmc_metrics)]
        {
            if !self.cons_congested_.swap(true, Ordering::Release) {
                metric_!(CONGESTED_SET_CONS);
            }
        }
        #[cfg(not(mpmc_metrics))]
        self.cons_congested_.store(true, Ordering::Release);
    }

    /// 方案 D：慢路径成功后尝试清消费拥塞位。
    #[cfg(feature = "hybrid")]
    #[inline]
    pub(super) fn clear_cons_congested_(&self) {
        if !self.cons_congested_.load(Ordering::Acquire) {
            return;
        }
        let mut sess = self.cons_gate_.acquire_session();
        if let Result::Ok(_guard) = sess.try_write() {
            self.cons_congested_.store(false, Ordering::Release);
            metric_!(CONGESTED_CLEAR_CONS);
        } else {
            metric_!(CONGESTED_CLEAR_FAIL_CONS);
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
