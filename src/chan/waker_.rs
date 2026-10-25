//! 每方向一个单等待者通知槽。
//!
//! 结构：一个持久 `pending` 位、一个 `waiter` 快速位，以及一个只保存单个
//! `Option<Waker>` 的 [`SpinningMutexOwned`]。协议是「先置 pending，再决定是否
//! 取 waker；等待方登记后必须复检」。
//!
//! # 为什么复用 `SpinningMutexOwned`
//!
//! 之前这里有一个自研的 bool 自旋锁。它能工作，但项目里已经有经过测试的
//! 自旋互斥锁，且 `smux_v1` 的 `NotifySlot_` 就是用同一把锁保护 `Option<Waker>`。
//! 复用减少了一份 unsafe 与独立维护的锁实现。
//!
//! # 为什么还需要 `waiter` 快速位
//!
//! 第一版没有 `has_waiter` 优化，`notify_` 在每个 push/pop 上都会被调用。如果
//! 每次都去取 `SpinningMutexOwned`，就把一把通用互斥锁放到了热路径上。`waiter`
//! 位让 `notify_` 在没人 park 时只做两次 SeqCst 原子操作，不进入 mutex。
//!
//! `pending` 与 `waiter` 都用 `SeqCst`：这是一个经典的 wait/notify 竞争——通知方
//! 先置 `pending` 再查 `waiter`，等待方先置 `waiter` 再复检 `pending`。只用
//! Acquire/Release 不足以排除 store-load 重排，弱内存序平台上会丢唤醒。
//!
//! # 内存序
//!
//! `Option<Waker>` 的发布/读取由 mutex 的 `AcqRel` 成功序保证：加锁是 Acquire，
//! 解锁是 Release。这里**不能**用 `LocksOrderings`，因为它的成功序是 `Acquire`，
//! 而 `SpinningMutex` 的加锁与 `MutexGuard::drop` 的解锁共用同一个
//! `SUCC_ORDERING`；解锁没有 Release 就无法把等待方写入的 waker 发布给通知方。

use core::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::{Context, Poll, Waker},
};

use atomic_sync::{
    mutex::preemptive::{MsbAsMutexSignal, SpinningMutexOwned},
    x_deps::atomex::TrCmpxchOrderings,
};

/// 只保护一个 `Option<Waker>` 的互斥锁内存序。
///
/// `SUCC_ORDERING = AcqRel` 同时覆盖两个方向：加锁需要 Acquire，解锁需要
/// Release。`LocksOrderings` 的成功序是 Acquire，不能用于解锁发布。
#[derive(Clone, Copy, Debug, Default)]
struct WakerOrderings_;

impl TrCmpxchOrderings for WakerOrderings_ {
    const SUCC_ORDERING: Ordering = Ordering::AcqRel;
    const FAIL_ORDERING: Ordering = Ordering::Acquire;
    const LOAD_ORDERING: Ordering = Ordering::Acquire;
}

/// 保存单个 waker 的自旋互斥锁。
type WakerMutex_ =
    SpinningMutexOwned<Option<Waker>, AtomicUsize, MsbAsMutexSignal<usize>, WakerOrderings_>;

/// 单等待者通知槽。
///
/// 只允许一个 park 者使用；调用方（方向写锁）保证这一点。第二个等待者会覆盖
/// 第一个的 waker，因此不要把同一个槽共享给多个 park 者。
pub(super) struct WakerSlot {
    pending_: AtomicBool,
    waiter_: AtomicBool,
    waker_: WakerMutex_,
}

impl WakerSlot {
    /// 空槽（未通知、无等待者）。
    pub(super) fn new_() -> Self {
        WakerSlot {
            pending_: AtomicBool::new(false),
            waiter_: AtomicBool::new(false),
            waker_: WakerMutex_::new_owned(Option::None),
        }
    }

    /// 在槽锁下访问 waker。
    ///
    /// 这里使用 `SpinningMutexOwned` 的不可取消 `wait()`：临界区只有一次
    /// `Option<Waker>::take` / 覆盖，不会 yield，也不会等待条件变量。
    fn with_waker_<R>(&self, f: impl FnOnce(&mut Option<Waker>) -> R) -> R {
        let mut session = self.waker_.lock_session();
        let mut guard = match session.lock().wait() {
            Result::Ok(guard) => guard,
            // `wait()` 使用 NonCancellableToken；按锁的契约不会取消/重试。
            Result::Err(_) => unreachable!("不可取消的自旋锁不应返回错误"),
        };
        f(&mut guard)
    }

    /// 报告「状态可能变了」：先置持久位；只有确实有 park 者时才取 waker 唤醒。
    pub(super) fn notify_(&self) {
        self.pending_.store(true, Ordering::SeqCst);
        // 用 swap 消费 waiter：既读取又清位。即使这次取不到 waker（等待方正处于
        // 「置 waiter 之后、登记 waker 之前」），pending 也已置位，等待方复检时
        // 会立即返回 Ready。
        if self.waiter_.swap(false, Ordering::SeqCst) {
            let waker = self.with_waker_(|slot| slot.take());
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }

    /// 等待下一次通知。
    ///
    /// 调用方应在外层循环里先检查条件；本函数只负责「消费 pending / 登记 /
    /// 再复检 pending」。返回 `Ready` 后必须重新检查条件，不能假定条件已成立。
    pub(super) fn poll_wait_(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.pending_.swap(false, Ordering::SeqCst) {
            self.waiter_.store(false, Ordering::SeqCst);
            return Poll::Ready(());
        }

        // 先置 waiter，再登记 waker，最后复检 pending。顺序不能颠倒：通知方
        // 是「先 pending、再查 waiter」，两侧都用 SeqCst。
        self.waiter_.store(true, Ordering::SeqCst);
        self.with_waker_(|slot| {
            let stale = match slot.as_ref() {
                Option::Some(old) => !old.will_wake(cx.waker()),
                Option::None => true,
            };
            if stale {
                *slot = Option::Some(cx.waker().clone());
            }
        });

        if self.pending_.swap(false, Ordering::SeqCst) {
            self.waiter_.store(false, Ordering::SeqCst);
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests_ {
    use core::{
        sync::atomic::Ordering,
        task::{Context, Poll, Waker},
    };

    use super::WakerSlot;

    /// 测试目标：验证无通知时 park、通知后唤醒，以及持久 pending 不丢。
    /// 手段：用 noop waker 首次 poll 得到 Pending；notify 后再次 poll 得到 Ready；
    ///       再在未 poll 的情况下 notify 一次，验证下一次 poll 直接 Ready。
    /// 判断：第一次 Pending、第二次 Ready、第三次（先 notify 再 poll）Ready。
    /// Test goal: verify park, wake, and persistent pending semantics.
    /// Method: first poll with a noop waker -> Pending; notify then poll -> Ready;
    ///         notify without polling, then poll -> Ready.
    /// Assertion: Pending, Ready, Ready.
    #[test]
    fn pending_wake_() {
        let slot = WakerSlot::new_();
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        assert_eq!(slot.poll_wait_(&mut cx), Poll::Pending);
        slot.notify_();
        assert_eq!(slot.poll_wait_(&mut cx), Poll::Ready(()));

        slot.notify_();
        assert_eq!(slot.poll_wait_(&mut cx), Poll::Ready(()));
    }

    /// 测试目标：验证 notify 之后 waiter 位被消费，后续 notify 不重复取锁。
    /// 手段：先 poll 到 Pending（置 waiter），notify 一次（swap 清 waiter），
    ///       再直接检查 waiter 位。
    /// 判断：notify 前 waiter=true；notify 后 waiter=false。
    /// Test goal: verify notify consumes the waiter hint.
    /// Method: poll to Pending (sets waiter), notify once (swaps it clear), inspect.
    /// Assertion: waiter is true before notify and false after.
    #[test]
    fn notify_consumes_waiter_() {
        let slot = WakerSlot::new_();
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        assert_eq!(slot.poll_wait_(&mut cx), Poll::Pending);
        assert!(slot.waiter_.load(Ordering::SeqCst));
        slot.notify_();
        assert!(!slot.waiter_.load(Ordering::SeqCst));
    }
}
