//! 每方向一个单等待者通知槽。
//!
//! 与 `smux_v1` 的 `NotifySlot_` 同构：一个持久 `pending` 位 + 一个只保存单个
//! `Waker` 的短自旋锁。协议是「先置 pending，再取 waker；等待方登记后必须复检」。
//! `wake()` 一律在槽锁之外调用，避免单线程执行器上同步重入造成自死锁。

use core::{
    cell::UnsafeCell,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll, Waker},
};

/// 只保护一个 `Option<Waker>` 的极短自旋锁。
struct SpinLock<T> {
    locked_: AtomicBool,
    data_: UnsafeCell<T>,
}

impl<T> SpinLock<T> {
    const fn new_(data: T) -> Self {
        SpinLock {
            locked_: AtomicBool::new(false),
            data_: UnsafeCell::new(data),
        }
    }

    #[inline]
    fn with_<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        while self
            .locked_
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            core::hint::spin_loop();
        }

        // 即使闭包 panic，也要释放锁；否则一次 `Waker::clone` 的 panic 会把
        // 整个方向的 notify 永久卡死。
        let _unlock = Unlock_(&self.locked_);
        // SAFETY: 自旋锁已被本线程独占；闭包在 `_unlock` 释放之前执行。
        f(unsafe { &mut *self.data_.get() })
    }
}

/// 作用域结束时释放自旋锁的 RAII 守卫。
struct Unlock_<'a>(&'a AtomicBool);

impl Drop for Unlock_<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

// SAFETY: 锁保证同一时刻只有一个线程访问 T；T: Send 即可跨线程移动。
unsafe impl<T: Send> Sync for SpinLock<T> {}

/// 单等待者通知槽。
pub(super) struct WakerSlot {
    pending_: AtomicBool,
    waker_: SpinLock<Option<Waker>>,
}

impl WakerSlot {
    /// 空槽（未通知、无等待者）。
    pub(super) const fn new_() -> Self {
        WakerSlot {
            pending_: AtomicBool::new(false),
            waker_: SpinLock::new_(Option::None),
        }
    }

    /// 报告「状态可能变了」：先置持久位，再取走 waker 并在锁外唤醒。
    pub(super) fn notify_(&self) {
        self.pending_.store(true, Ordering::Release);
        let waker = self.waker_.with_(|slot| slot.take());
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// 等待下一次通知。
    ///
    /// 调用方应在外层循环里先检查条件；本函数只负责「消费 pending / 登记 /
    /// 再复检 pending」。返回 `Ready` 后必须重新检查条件，不能假定条件已成立。
    pub(super) fn poll_wait_(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.pending_.swap(false, Ordering::AcqRel) {
            return Poll::Ready(());
        }

        self.waker_.with_(|slot| {
            let stale = match slot.as_ref() {
                Option::Some(old) => !old.will_wake(cx.waker()),
                Option::None => true,
            };
            if stale {
                *slot = Option::Some(cx.waker().clone());
            }
        });

        if self.pending_.swap(false, Ordering::AcqRel) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
