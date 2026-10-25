//! 通道共享内核：两个方向等待队列、关闭标志与句柄计数。
//!
//! 与旧版双锁实现的根本区别：内核里**没有读写锁**。快路径直接操作 Vyukov 环；
//! 慢路径失败后进入本方向的 [`WaitQueue`]，由队列节点保存 waker 与排队顺序。
//! 生产者与消费者仍然完全独立：生产队列只在消费者腾出空间时被唤醒，消费队列
//! 只在生产者发布元素时被唤醒，因此同一方向大量等待不会阻塞对向。

use alloc::{alloc::Global, sync::Arc};
use core::{
    borrow::BorrowMut,
    marker::PhantomData,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use crate::{MpmcBuf, buf_::Pop_};

use super::{
    config_::{DefaultConfig, TrConfig},
    metrics_::metric_,
    receiver_::Receiver,
    sender_::Sender,
    wait_queue_::WaitQueue,
};

/// 快路径出队结果。
///
/// 不能把「环里没有可读元素」与「头槽已预留但生产者尚未发布」混成一个 `None`：
/// 关闭后仍要 drain 的语义下，后者应继续等待发布，而不是直接返回 `Closed`。
pub(super) enum TryPop_<T> {
    /// 取到一个元素。
    Value(T),
    /// 确实为空。
    Empty,
    /// 头槽已预留但生产者尚未发布；应等待而不是判定关闭。
    Pending,
}

/// 自建等待队列 MPMC 通道的共享内核。
///
/// 用户通常不直接持有它：[`MpmcChan::new`] 返回一对 [`Sender`] / [`Receiver`]
/// 句柄，句柄内部各持一个 `Arc<MpmcChan>`。
///
/// # 等待模型
///
/// - `prod_wait_`：生产者慢路径的 FIFO 等待队列。消费者每次成功出队后唤醒队首；
/// - `cons_wait_`：消费者慢路径的 FIFO 等待队列。生产者每次成功入队后唤醒队首；
/// - 队列节点由 SlabList 批量分配，包含 waker、状态与双向链接；
/// - 快路径不检查队列是否非空，始终直接尝试无锁环操作，因此等待队列不会像
///   读写锁的写者队列那样自我维持；代价是同方向 FIFO 是**近似公平**（快路径
///   仍可能插队），已在 crate 文档中写明。
///
/// # 调教（`C`）
///
/// 最后一个类型参数 `C: TrConfig` 决定外层 `Arc` 与等待节点 SlabList 的
/// allocator（`C::ChanAlloc`）、两个方向等待队列各自的 SlabList 配置类型
/// （`C::ProdWaitSlab` / `C::ConsWaitSlab`）与 async 快路径自旋预算
/// （`C::FAST_SPIN`），默认为 [`DefaultConfig`]。chunk 大小由配置类型携带的
/// 编译期常量固定。
///
/// # Buffer 所有权
///
/// `B` 是 buffer 的所有权载体，必须实现 `BorrowMut<MpmcBuf<T, N>>`：
///
/// - owned `MpmcBuf` / `Box<MpmcBuf>`：channel 拥有 buffer；
/// - `&'a mut MpmcBuf`：channel 借用 buffer。
///
/// 两种情况都由 Rust 的所有权/独占借用保证「一个 buffer 只有一个 channel」，
/// 不需要运行时 claim 标志。
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
pub struct MpmcChan<'a, T, const N: usize, B = &'a mut MpmcBuf<T, N>, C = DefaultConfig>
where
    B: BorrowMut<MpmcBuf<T, N>>,
    C: TrConfig + 'a,
{
    pub(super) buf_: B,
    pub(super) prod_wait_: WaitQueue<C::ProdWaitSlab>,
    pub(super) cons_wait_: WaitQueue<C::ConsWaitSlab>,
    pub(super) senders_: AtomicUsize,
    pub(super) receivers_: AtomicUsize,
    pub(super) tx_closed_: AtomicBool,
    pub(super) rx_closed_: AtomicBool,
    /// 让 `'a` 在 owned buffer（例如 `Box<MpmcBuf>`）时也有使用点。
    pub(super) _lt_: PhantomData<&'a MpmcBuf<T, N>>,
}

impl<'a, T, const N: usize, B> MpmcChan<'a, T, N, B, DefaultConfig>
where
    B: BorrowMut<MpmcBuf<T, N>>,
{
    /// 用 buffer 构造一对通道句柄（默认 [`DefaultConfig`]）。
    ///
    /// `buf` 可以是 owned `MpmcBuf`、`Box<MpmcBuf>`，也可以是 `&mut MpmcBuf`。
    /// 构造与 Drop 都会 drain + reset buffer。独占性由所有权/`&mut` 借用保证，
    /// 因此不存在运行时「buffer 已被占用」这一类错误。
    ///
    /// 要换 allocator、等待队列 chunk 大小或自旋预算，使用 [`Self::new_in`] 并
    /// 显式指定配置类型 `MpmcChan<..., C>`。
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
    pub fn new(
        buf: B,
    ) -> (
        Sender<'a, T, N, B, DefaultConfig>,
        Receiver<'a, T, N, B, DefaultConfig>,
    ) {
        Self::new_in(buf, Global)
    }
}

impl<'a, T, const N: usize, B, C> MpmcChan<'a, T, N, B, C>
where
    B: BorrowMut<MpmcBuf<T, N>>,
    C: TrConfig + 'a,
{
    /// 用 buffer 与自定义 allocator 构造通道。
    ///
    /// 两个方向的等待队列会各自克隆一份 `chan_alloc` 来分配 SlabList chunk；
    /// 外层 `Arc` 使用原值。默认配置可以直接用 [`Self::new`]。
    ///
    /// # Panics
    ///
    /// 当 `C::ProdWaitSlab` / `C::ConsWaitSlab` 的编译期 `CHUNK_SIZE` 无法容纳
    /// 至少一个等待节点时 panic；这是配置错误，应在配置类型中修正。
    ///
    /// # Examples
    ///
    /// ```
    /// use mpmc_chan::{DefaultConfig, MpmcBuf, MpmcChan};
    /// use std::alloc::Global;
    ///
    /// let mut buf = MpmcBuf::<u8, 2>::new();
    /// let (tx, rx) = MpmcChan::<u8, 2, _, DefaultConfig>::new_in(&mut buf, Global);
    /// tx.try_send(1).unwrap();
    /// assert_eq!(rx.try_recv(), Ok(1));
    /// ```
    pub fn new_in(
        mut buf: B,
        chan_alloc: C::ChanAlloc,
    ) -> (Sender<'a, T, N, B, C>, Receiver<'a, T, N, B, C>) {
        buf.borrow_mut().drop_remaining_();

        let prod_wait_ = WaitQueue::<C::ProdWaitSlab>::new_(chan_alloc.clone());
        let cons_wait_ = WaitQueue::<C::ConsWaitSlab>::new_(chan_alloc.clone());

        let core = Arc::new_in(
            MpmcChan {
                buf_: buf,
                prod_wait_,
                cons_wait_,
                senders_: AtomicUsize::new(0),
                receivers_: AtomicUsize::new(0),
                tx_closed_: AtomicBool::new(false),
                rx_closed_: AtomicBool::new(false),
                _lt_: PhantomData,
            },
            chan_alloc,
        );

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

    /// 唤醒两个方向全部等待者；关闭或即将关闭时调用。
    #[inline]
    pub(super) fn notify_all_(&self) {
        self.prod_wait_.notify_all_();
        self.cons_wait_.notify_all_();
    }

    /// 唤醒生产方向队首：消费者腾出了一个位置。
    #[inline]
    pub(super) fn notify_prod_(&self) {
        self.prod_wait_.notify_one_();
    }

    /// 唤醒消费方向队首：生产者发布了一个元素。
    #[inline]
    pub(super) fn notify_cons_(&self) {
        self.cons_wait_.notify_one_();
    }

    /// 直接尝试一次入队；不计数，由 fast / try 调用方分别记账。
    #[inline]
    fn try_push_now_(&self, item: T) -> Result<(), T> {
        self.buf_.borrow().try_push_(item)
    }

    /// 直接尝试一次出队，并区分空/未发布；不计数。
    #[inline]
    fn try_pop_now_(&self) -> TryPop_<T> {
        match self.buf_.borrow().try_pop_state_() {
            Pop_::Value(value) => TryPop_::Value(value),
            Pop_::Empty => TryPop_::Empty,
            Pop_::Pending => TryPop_::Pending,
        }
    }

    /// `try_send` 使用的非阻塞路径：直接尝试一次入队。
    ///
    /// 不再有方向锁，因此只有「环满」会失败；慢等待者不会让 `try_send` 变成
    /// would-block（这是与旧版的语义差异，见 crate 文档的公平性说明）。
    #[inline]
    pub(super) fn try_push_try_(&self, item: T) -> Result<(), T> {
        metric_!(PUSH_TRY_ATTEMPT);
        match self.try_push_now_(item) {
            Result::Ok(()) => {
                metric_!(PUSH_TRY_OK);
                Result::Ok(())
            }
            Result::Err(item) => {
                metric_!(PUSH_TRY_FULL);
                Result::Err(item)
            }
        }
    }

    /// `try_recv` 使用的非阻塞路径：直接尝试一次出队。
    #[inline]
    pub(super) fn try_pop_try_(&self) -> TryPop_<T> {
        metric_!(POP_TRY_ATTEMPT);
        match self.try_pop_now_() {
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
        }
    }

    /// 异步快路径：直接尝试一次入队。
    #[inline]
    pub(super) fn try_push_fast_(&self, item: T) -> Result<(), T> {
        metric_!(PUSH_FAST_ATTEMPT);
        match self.try_push_now_(item) {
            Result::Ok(()) => {
                metric_!(PUSH_FAST_OK);
                Result::Ok(())
            }
            Result::Err(item) => {
                metric_!(PUSH_FAST_FULL);
                Result::Err(item)
            }
        }
    }

    /// 异步快路径：直接尝试一次出队。
    #[inline]
    pub(super) fn try_pop_fast_(&self) -> TryPop_<T> {
        metric_!(POP_FAST_ATTEMPT);
        match self.try_pop_now_() {
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
        }
    }
}

impl<'a, T, const N: usize, B, C> Drop for MpmcChan<'a, T, N, B, C>
where
    B: BorrowMut<MpmcBuf<T, N>>,
    C: TrConfig + 'a,
{
    fn drop(&mut self) {
        // 走到这里说明所有 Sender/Receiver/Future 都已经消失，不可能有在途操作。
        // drop_remaining_ 会 drain 已发布未消费的元素并 reset。
        self.buf_.borrow_mut().drop_remaining_();
    }
}
