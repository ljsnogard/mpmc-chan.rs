//! 每方向一个的异步等待队列。
//!
//! 这是替代方向 `RwLock` 的核心结构：不再用读写锁把慢路径排成写者队列，而是
//! 让每个 park 的 future 在队列里拥有一个**等待节点**。节点由
//! [`SlabList`](crate::slab_list_::SlabList) 批量分配，内部保存 waker、状态和
//! 双向链接；唤醒时从队首摘除节点并锁外 `wake`。
//!
//! # 语义
//!
//! - **同方向 FIFO**：`notify_one_` 只摘除队首节点，因此等待者按入队顺序获得
//!   唤醒机会；
//! - **无写者优先**：快路径不检查本队列，始终直接尝试无锁环操作。因此队列
//!   只承载“确实无法完成”的操作，不会像读写锁那样一入队就挡住所有快路径；
//! - **取消安全**：等待节点由 [`WaitGuard`] 持有，future 被丢弃时 `Drop` 会把
//!   仍在队列中的节点摘除并归还 SlabList；
//! - **无每轮堆分配**：除第一次申请 Chunk 外，入队/唤醒/释放都只在 SlabList
//!   的 chunk 内做索引操作。
//!
//! # 丢失唤醒的防护
//!
//! 通知方可能在等待方入队**之前**观察到条件变化。为此等待方在入队后必须再次
//! 检查条件（由 `sender_` / `receiver_` 的慢路径循环完成）；若条件已满足就
//! 直接完成并释放节点。这样即使通知发生在入队窗口内被“错过”，复检也能兜住。
//!
//! # 锁
//!
//! 队列状态由本模块自带的短自旋锁保护；锁内只做链表/状态更新和 waker 的
//! 取出，`Waker::wake` 一律在锁外调用。另有一个原子 `waiting_` 计数，
//! 让热路径上的 `notify_one_` 在无人等待时只做一次原子读、不取锁。

use alloc::vec::Vec;
use core::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll, Waker},
};

use crate::{
    chan::TrSlabConfig,
    slab_list_::{SlabList, SlotPtr},
};

/// 一个短临界区自旋锁。
///
/// 队列状态里包含 [`SlabList`] 的裸指针，因此不能用要求 `T: Sync` 的通用自旋
/// 互斥锁；这里只需要 `T: Send`：临界区访问由锁串行化，数据可以跨执行流移交。
struct SpinLock<T> {
    locked_: AtomicUsize,
    data_: UnsafeCell<T>,
}

// SAFETY: `data_` 只在持有锁（`locked_ == 1`）时经 `SpinLockGuard` 访问，同一
// 时刻至多一个执行流能取得锁，因此共享访问必然串行；`T: Send` 保证临界区数据
// 可以跨执行流移交。
unsafe impl<T: Send> Send for SpinLock<T> {}

// SAFETY: 理由同上；锁本身提供跨线程的互斥访问。
unsafe impl<T: Send> Sync for SpinLock<T> {}

impl<T> SpinLock<T> {
    const fn new_(data: T) -> Self {
        SpinLock {
            locked_: AtomicUsize::new(0),
            data_: UnsafeCell::new(data),
        }
    }

    /// 自旋直到取得锁。
    fn lock_(&self) -> SpinLockGuard<'_, T> {
        while self
            .locked_
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            core::hint::spin_loop();
        }
        SpinLockGuard { lock_: self }
    }

    fn unlock_(&self) {
        self.locked_.store(0, Ordering::Release);
    }
}

/// 自旋锁守卫；析构时释放锁。
struct SpinLockGuard<'a, T> {
    lock_: &'a SpinLock<T>,
}

impl<T> Deref for SpinLockGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: 构造守卫的前提是已持有锁，且在守卫析构前不会释放，因此这里
        // 是该临界区的唯一访问路径。
        unsafe { &*self.lock_.data_.get() }
    }
}

impl<T> DerefMut for SpinLockGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: 同 `deref`。
        unsafe { &mut *self.lock_.data_.get() }
    }
}

impl<T> Drop for SpinLockGuard<'_, T> {
    fn drop(&mut self) {
        self.lock_.unlock_()
    }
}

/// 等待节点的状态。
///
/// 只在队列锁内修改，因此不需要原子操作。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WaitState {
    /// 仍在队列中等待通知。
    Waiting,
    /// 已被通知方摘除并置位；等待者下一次 poll 会观察到它。
    Notified,
    /// 等待者已经完成/取消，节点正在或已经被归还 SlabList。
    Done,
}

/// 等待队列节点。
///
/// 节点由 SlabList 分配，地址在存活期间稳定；队列链接和状态都只在队列锁内
/// 读写。
struct WaitNode {
    /// 当前注册的 waker；被通知时取走，在锁外唤醒。
    waker_: Option<Waker>,
    /// 节点状态。
    state_: WaitState,
    /// 前驱链接（队首为 `None`）。
    prev_: Option<SlotPtr<WaitNode>>,
    /// 后继链接（队尾为 `None`）。
    next_: Option<SlotPtr<WaitNode>>,
}

// SAFETY: 节点只含 `Waker` 与两个槽位句柄。节点的所有字段访问都在队列锁内
// 完成；跨线程移动节点（或它的句柄）不会产生别名访问。
unsafe impl Send for WaitNode {}

/// 队列状态：队首/队尾、长度与节点池。
struct QueueState<C: TrSlabConfig> {
    head_: Option<SlotPtr<WaitNode>>,
    tail_: Option<SlotPtr<WaitNode>>,
    len_: usize,
    slab_: SlabList<WaitNode, C>,
}

/// 单方向的异步等待队列。
///
/// `A` 是等待节点 Chunk 的分配器类型，通常与通道的 `ChanAlloc` 相同。
pub(super) struct WaitQueue<C: TrSlabConfig> {
    state_: SpinLock<QueueState<C>>,
    /// 队列中等待者的快速计数，供 `notify_one_` 在无人等待时跳过取锁。
    waiting_: AtomicUsize,
}

impl<C: TrSlabConfig> WaitQueue<C> {
    /// 建立空队列。
    ///
    /// `alloc` 是配置类型 `C` 指定的 allocator 实例；chunk 大小由 `C` 的
    /// 编译期常量 `CHUNK_SIZE` 固定。
    ///
    /// # Panics
    ///
    /// 当 `C::CHUNK_SIZE` 无法容纳至少一个等待节点时 panic；这是配置错误
    /// （SlabList chunk 过小），应在配置类型中修正。
    pub(super) fn new_(alloc: C::Allocator) -> Self {
        let slab_ = SlabList::new_(alloc).unwrap_or_else(|error| {
            panic!(
                "等待队列的 SlabList chunk 配置非法（{} 字节）：{error}",
                C::CHUNK_SIZE
            )
        });
        WaitQueue {
            state_: SpinLock::new_(QueueState {
                head_: Option::None,
                tail_: Option::None,
                len_: 0,
                slab_,
            }),
            waiting_: AtomicUsize::new(0),
        }
    }

    /// 把 waker 放入队尾，返回等待节点句柄。
    ///
    /// 调用方随后必须再次检查条件；若条件已经满足，应通过
    /// [`WaitGuard::finish_`]（或直接丢弃 guard）归还节点。
    fn enqueue_(&self, waker: Waker) -> SlotPtr<WaitNode> {
        let mut guard = self.state_.lock_();
        let state = &mut *guard;
        let prev = state.tail_;
        let node = state.slab_.insert_(WaitNode {
            waker_: Option::Some(waker),
            state_: WaitState::Waiting,
            prev_: prev,
            next_: Option::None,
        });
        match prev {
            Option::Some(mut tail) => tail.next_ = Option::Some(node),
            Option::None => state.head_ = Option::Some(node),
        }
        state.tail_ = Option::Some(node);
        state.len_ += 1;
        self.waiting_.fetch_add(1, Ordering::AcqRel);
        node
    }

    /// 注册/更新 waker，并检查节点是否已被通知。
    ///
    /// 返回 `Ready` 表示“节点已被通知”，调用方应重新检查条件；返回 `Pending`
    /// 表示 waker 已登记、可以安全返回。
    fn poll_wait_(&self, mut node: SlotPtr<WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        let guard = self.state_.lock_();
        // 锁内只需要保证状态与 waker 的访问互斥；不直接解引用 QueueState。
        let _ = &*guard;
        match node.state_ {
            WaitState::Notified => {
                node.state_ = WaitState::Done;
                Poll::Ready(())
            }
            WaitState::Waiting => {
                let stale = match node.waker_.as_ref() {
                    Option::Some(old) => !old.will_wake(cx.waker()),
                    Option::None => true,
                };
                if stale {
                    // 旧 waker 的析构发生在锁内；这与原 `WakerSlot` 的替换策略
                    // 一致，临界区只涉及一次 `Waker` 的替换。
                    node.waker_ = Option::Some(cx.waker().clone());
                }
                Poll::Pending
            }
            WaitState::Done => Poll::Ready(()),
        }
    }

    /// 唤醒队首等待者；无人等待时直接返回，不取锁。
    pub(super) fn notify_one_(&self) {
        if self.waiting_.load(Ordering::Acquire) == 0 {
            return;
        }

        let waker = {
            let mut guard = self.state_.lock_();
            let state = &mut *guard;
            let Option::Some(mut node) = state.head_ else {
                return;
            };
            state.head_ = node.next_;
            match state.head_ {
                Option::Some(mut next) => next.prev_ = Option::None,
                Option::None => state.tail_ = Option::None,
            }
            node.prev_ = Option::None;
            node.next_ = Option::None;
            node.state_ = WaitState::Notified;
            state.len_ -= 1;
            self.waiting_.fetch_sub(1, Ordering::AcqRel);
            node.waker_.take()
        };

        if let Option::Some(waker) = waker {
            waker.wake();
        }
    }

    /// 唤醒队列中的全部等待者（关闭路径使用）。
    pub(super) fn notify_all_(&self) {
        let wakers = {
            let mut guard = self.state_.lock_();
            let state = &mut *guard;
            let mut wakers = Vec::new();

            while let Option::Some(mut node) = state.head_ {
                let next = node.next_;
                state.head_ = next;
                match next {
                    Option::Some(mut next) => next.prev_ = Option::None,
                    Option::None => state.tail_ = Option::None,
                }
                node.prev_ = Option::None;
                node.next_ = Option::None;
                node.state_ = WaitState::Notified;
                if let Option::Some(waker) = node.waker_.take() {
                    wakers.push(waker);
                }
                state.len_ -= 1;
                self.waiting_.fetch_sub(1, Ordering::AcqRel);
            }

            wakers
        };

        for waker in wakers {
            waker.wake();
        }
    }

    /// 归还等待节点：若它还在队列中就先摘除，然后释放槽位。
    ///
    /// 槽位释放后 `node` 句柄立即失效，调用方不得再使用它。
    fn release_(&self, mut node: SlotPtr<WaitNode>) {
        let value = {
            let mut guard = self.state_.lock_();
            let state = &mut *guard;

            if node.state_ == WaitState::Waiting {
                let prev = node.prev_;
                let next = node.next_;
                match prev {
                    Option::Some(mut prev) => prev.next_ = next,
                    Option::None => state.head_ = next,
                }
                match next {
                    Option::Some(mut next) => next.prev_ = prev,
                    Option::None => state.tail_ = prev,
                }
                state.len_ -= 1;
                self.waiting_.fetch_sub(1, Ordering::AcqRel);
            }

            node.state_ = WaitState::Done;
            node.prev_ = Option::None;
            node.next_ = Option::None;
            state.slab_.remove_(node)
        };

        // 节点内可能还留有取消时未取走的 waker；在锁外析构。
        drop(value);
    }

    /// 队列中等待者的数量（仅供测试观测）。
    #[cfg(test)]
    fn len_(&self) -> usize {
        self.state_.lock_().len_
    }

    /// SlabList 中仍然存活的节点数量（仅供测试观测泄漏）。
    #[cfg(test)]
    fn occupied_len_(&self) -> usize {
        self.state_.lock_().slab_.occupied_len_()
    }
}

/// 一个已入队的等待节点守卫。
///
/// `WaitGuard` 保证无论 future 是正常完成、取消还是被直接丢弃，节点都会归还
/// SlabList；这正是自建队列替代 `RwLock` 写者队列后仍能保持取消安全的关键。
pub(super) struct WaitGuard<'q, C: TrSlabConfig> {
    queue_: &'q WaitQueue<C>,
    node_: Option<SlotPtr<WaitNode>>,
}

impl<'q, C: TrSlabConfig> WaitGuard<'q, C> {
    /// 入队并返回守卫。
    pub(super) fn arm_(queue: &'q WaitQueue<C>, waker: Waker) -> Self {
        let node = queue.enqueue_(waker);
        WaitGuard {
            queue_: queue,
            node_: Option::Some(node),
        }
    }

    /// 注册 waker；被通知时自动归还节点并返回 `Ready`。
    pub(super) fn poll_(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let Option::Some(node) = self.node_ else {
            return Poll::Ready(());
        };
        match self.queue_.poll_wait_(node, cx) {
            Poll::Ready(()) => {
                self.finish_();
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }

    /// 归还节点（幂等）。
    pub(super) fn finish_(&mut self) {
        if let Option::Some(node) = self.node_.take() {
            self.queue_.release_(node);
        }
    }

    /// 当前是否仍持有节点（仅供测试观测）。
    #[cfg(test)]
    pub(super) fn is_armed_(&self) -> bool {
        self.node_.is_some()
    }
}

impl<C: TrSlabConfig> Drop for WaitGuard<'_, C> {
    fn drop(&mut self) {
        self.finish_();
    }
}

#[cfg(test)]
mod tests_ {
    use alloc::alloc::Global;
    use core::task::{Context, Poll, Waker};

    use super::{WaitGuard, WaitQueue};
    use crate::chan::SlabListConfig;

    /// 测试目标：验证队列 FIFO 唤醒顺序与节点回收。
    /// 手段：入队 a/b 两个等待者，连续两次 `notify_one_`，每次检查两个 guard 的 poll。
    /// 判断：第一次 a 为 Ready、b 为 Pending；第二次 b 为 Ready；队列与 SlabList 都为空。
    /// Test goal: verify FIFO wake order and node reclamation.
    /// Method: enqueue a/b, call `notify_one_` twice, poll both guards after each call.
    /// Assertion: a is Ready and b is Pending first; then b is Ready; queue and slab become empty.
    #[test]
    fn notify_one_is_fifo_() {
        let queue = WaitQueue::<SlabListConfig<4096>>::new_(Global);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        let mut a = WaitGuard::arm_(&queue, waker.clone());
        let mut b = WaitGuard::arm_(&queue, waker.clone());
        assert_eq!(queue.len_(), 2);

        queue.notify_one_();
        assert_eq!(a.poll_(&mut cx), Poll::Ready(()));
        assert_eq!(b.poll_(&mut cx), Poll::Pending);
        assert!(!a.is_armed_());
        assert!(b.is_armed_());

        queue.notify_one_();
        assert_eq!(b.poll_(&mut cx), Poll::Ready(()));
        assert_eq!(queue.len_(), 0);
        assert_eq!(queue.occupied_len_(), 0);
    }

    /// 测试目标：验证取消（直接丢弃 guard）会把仍在队列中的节点摘除并归还。
    /// 手段：入队后直接 drop guard，检查队列长度与 SlabList 存活节点数。
    /// 判断：drop 后长度与存活节点数都为 0。
    /// Test goal: verify dropping a guard cancels and reclaims a queued node.
    /// Method: enqueue, drop the guard, inspect queue length and live node count.
    /// Assertion: both become zero.
    #[test]
    fn drop_cancels_and_reclaims_() {
        let queue = WaitQueue::<SlabListConfig<4096>>::new_(Global);
        let guard = WaitGuard::arm_(&queue, Waker::noop().clone());
        assert_eq!(queue.len_(), 1);

        drop(guard);
        assert_eq!(queue.len_(), 0);
        assert_eq!(queue.occupied_len_(), 0);
    }

    /// 测试目标：验证 `notify_all_` 会唤醒全部等待者。
    /// 手段：入队三个等待者，调用 `notify_all_`，逐个 poll。
    /// 判断：三个 guard 都返回 Ready；队列与 SlabList 都为空。
    /// Test goal: verify `notify_all_` wakes every waiter.
    /// Method: enqueue three waiters, call `notify_all_`, poll each guard.
    /// Assertion: all three are Ready; queue and slab become empty.
    #[test]
    fn notify_all_wakes_everyone_() {
        let queue = WaitQueue::<SlabListConfig<4096>>::new_(Global);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        let mut a = WaitGuard::arm_(&queue, waker.clone());
        let mut b = WaitGuard::arm_(&queue, waker.clone());
        let mut c = WaitGuard::arm_(&queue, waker.clone());
        assert_eq!(queue.len_(), 3);

        queue.notify_all_();
        assert_eq!(a.poll_(&mut cx), Poll::Ready(()));
        assert_eq!(b.poll_(&mut cx), Poll::Ready(()));
        assert_eq!(c.poll_(&mut cx), Poll::Ready(()));
        assert_eq!(queue.len_(), 0);
        assert_eq!(queue.occupied_len_(), 0);
    }

    /// 测试目标：验证已入队的通知先于 poll 发生时不丢唤醒。
    /// 手段：入队后先 `notify_one_`，再 poll guard。
    /// 判断：poll 直接返回 Ready（节点状态已是 Notified）。
    /// Test goal: verify a notification that happens before poll is not lost.
    /// Method: enqueue, call `notify_one_`, then poll the guard.
    /// Assertion: poll returns Ready immediately.
    #[test]
    fn notify_before_poll_is_not_lost_() {
        let queue = WaitQueue::<SlabListConfig<4096>>::new_(Global);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        let mut a = WaitGuard::arm_(&queue, waker.clone());
        queue.notify_one_();
        assert_eq!(a.poll_(&mut cx), Poll::Ready(()));
        assert_eq!(queue.occupied_len_(), 0);
    }

    /// 测试目标：验证 `notify_one_` 在空队列上直接返回且不破坏状态。
    /// 手段：对空队列连续调用两次 `notify_one_`。
    /// 判断：长度保持 0，没有 panic，SlabList 仍为空。
    /// Test goal: verify `notify_one_` on an empty queue is a no-op.
    /// Method: call `notify_one_` twice on an empty queue.
    /// Assertion: length stays zero, no panic, slab stays empty.
    #[test]
    fn notify_empty_queue_() {
        let queue = WaitQueue::<SlabListConfig<4096>>::new_(Global);
        queue.notify_one_();
        queue.notify_one_();
        assert_eq!(queue.len_(), 0);
        assert_eq!(queue.occupied_len_(), 0);
    }
}
