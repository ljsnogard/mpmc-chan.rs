//! 基准专用计数点（仅在 `--cfg mpmc_metrics` 下编译）。
//!
//! # 为什么不是 Cargo feature
//!
//! `mpmc_metrics` 是构建期 `--cfg` 开关，不进入 `Cargo.toml` 的 feature 清单，
//! 因此不改变使用方的依赖/配置面；默认构建既不编译本模块，也不产生任何计数
//! 指令。
//!
//! # 使用限制
//!
//! 计数器是进程级全局原子，只用于统计「快路径 / 慢路径 / park / wake」的**比例**。
//! 打开后每次计数都是共享 cache line 上的 RMW，会显著放大高竞争场景的开销，
//! 因此**不能用带计数的构建测吞吐**；性能数字必须在默认构建（无 `mpmc_metrics`）
//! 下测量，两者对照使用。
//!
//! bench 的读取入口是 [`reset`] / [`snapshot`] / [`names`]，它们被标记为
//! `#[doc(hidden)]`，不进入公开文档。

use core::sync::atomic::{AtomicUsize, Ordering};

/// 生产快路径：尝试次数（含首次与自旋重试）。
pub const PUSH_FAST_ATTEMPT: usize = 0;
/// 生产快路径：成功入队。
pub const PUSH_FAST_OK: usize = 1;
/// 生产快路径：拿到读锁但环满。
pub const PUSH_FAST_FULL: usize = 2;
/// 生产快路径：方向读锁未拿到（方案 C），或拥塞位为真（方案 D）。
pub const PUSH_FAST_BUSY: usize = 3;
/// 消费快路径：尝试次数。
pub const POP_FAST_ATTEMPT: usize = 4;
/// 消费快路径：成功出队。
pub const POP_FAST_OK: usize = 5;
/// 消费快路径：拿到消费读锁但环空。
pub const POP_FAST_EMPTY: usize = 6;
/// 消费快路径：头槽已预留但生产者尚未发布。
pub const POP_FAST_PENDING: usize = 7;
/// 消费快路径：方向读锁未拿到（方案 C），或拥塞位为真（方案 D）。
pub const POP_FAST_BUSY: usize = 8;
/// `try_send`：尝试次数。
pub const PUSH_TRY_ATTEMPT: usize = 9;
/// `try_send`：成功。
pub const PUSH_TRY_OK: usize = 10;
/// `try_send`：环满。
pub const PUSH_TRY_FULL: usize = 11;
/// `try_send`：方向读锁忙。
pub const PUSH_TRY_BUSY: usize = 12;
/// `try_recv`：尝试次数。
pub const POP_TRY_ATTEMPT: usize = 13;
/// `try_recv`：成功。
pub const POP_TRY_OK: usize = 14;
/// `try_recv`：环空。
pub const POP_TRY_EMPTY: usize = 15;
/// `try_recv`：头槽预留未发布。
pub const POP_TRY_PENDING: usize = 16;
/// `try_recv`：方向读锁忙。
pub const POP_TRY_BUSY: usize = 17;
/// 生产慢路径：进入 `write_async` 排队。
pub const PUSH_SLOW_ENTER: usize = 18;
/// 生产慢路径：在写锁内 park 一次。
pub const PUSH_SLOW_PARK: usize = 19;
/// 生产慢路径：从 park 被唤醒（含立即消费已置位的 pending）。
pub const PUSH_SLOW_WAKE: usize = 20;
/// 生产慢路径：成功入队。
pub const PUSH_SLOW_OK: usize = 21;
/// 生产慢路径：取消。
pub const PUSH_SLOW_CANCEL: usize = 22;
/// 生产慢路径：关闭。
pub const PUSH_SLOW_CLOSED: usize = 23;
/// 消费慢路径：进入 `write_async` 排队。
pub const POP_SLOW_ENTER: usize = 24;
/// 消费慢路径：在写锁内 park 一次。
pub const POP_SLOW_PARK: usize = 25;
/// 消费慢路径：从 park 被唤醒。
pub const POP_SLOW_WAKE: usize = 26;
/// 消费慢路径：成功出队。
pub const POP_SLOW_OK: usize = 27;
/// 消费慢路径：取消。
pub const POP_SLOW_CANCEL: usize = 28;
/// 消费慢路径：关闭且已无元素。
pub const POP_SLOW_CLOSED: usize = 29;
/// 方案 D：生产拥塞位 false -> true。
pub const CONGESTED_SET_PROD: usize = 30;
/// 方案 D：消费拥塞位 false -> true。
pub const CONGESTED_SET_CONS: usize = 31;
/// 方案 D：生产拥塞位成功清除。
pub const CONGESTED_CLEAR_PROD: usize = 32;
/// 方案 D：消费拥塞位成功清除。
pub const CONGESTED_CLEAR_CONS: usize = 33;
/// 方案 D：生产清位时 `try_write` 失败（仍有慢写者排队）。
pub const CONGESTED_CLEAR_FAIL_PROD: usize = 34;
/// 方案 D：消费清位时 `try_write` 失败。
pub const CONGESTED_CLEAR_FAIL_CONS: usize = 35;
/// waker：notify 命中 waiter（有 park 者）。
pub const WAKER_NOTIFY_HIT: usize = 36;
/// waker：notify 未命中 waiter（无 park 者，只置 pending）。
pub const WAKER_NOTIFY_MISS: usize = 37;
/// waker：notify 命中 waiter，但槽里没有 waker。
pub const WAKER_NOTIFY_NO_WAKER: usize = 38;
/// waker：`poll_wait_` 首次检查就消费到 pending（Ready）。
pub const WAKER_POLL_READY: usize = 39;
/// P1 自旋：生产侧进入自旋。
pub const SPIN_PUSH_ENTER: usize = 40;
/// P1 自旋：生产侧自旋中入队成功。
pub const SPIN_PUSH_OK: usize = 41;
/// P1 自旋：生产侧因拥塞位已为真而跳过自旋（方案 D）。
pub const SPIN_PUSH_SKIP: usize = 42;
/// P1 自旋：消费侧进入自旋。
pub const SPIN_POP_ENTER: usize = 43;
/// P1 自旋：消费侧自旋中出队成功。
pub const SPIN_POP_OK: usize = 44;
/// P1 自旋：消费侧因拥塞位已为真而跳过自旋（方案 D）。
pub const SPIN_POP_SKIP: usize = 45;
/// 生产慢路径：`write_async` 等待写锁时被 poll 为 Pending（等锁 park）。
pub const PUSH_SLOW_LOCK_PARK: usize = 46;
/// 生产慢路径：`write_async` 等待写锁时立即 Ready。
pub const PUSH_SLOW_LOCK_READY: usize = 47;
/// 消费慢路径：`write_async` 等待写锁时被 poll 为 Pending。
pub const POP_SLOW_LOCK_PARK: usize = 48;
/// 消费慢路径：`write_async` 等待写锁时立即 Ready。
pub const POP_SLOW_LOCK_READY: usize = 49;
/// 计数器总数。
pub const STAT_COUNT: usize = 50;

/// 全部计数器；索引由上面的常量给出。
static STATS_: [AtomicUsize; STAT_COUNT] = [const { AtomicUsize::new(0) }; STAT_COUNT];

/// 计一次数（仅 `mpmc_metrics` 构建存在）。
#[inline]
pub(crate) fn inc_(idx: usize) {
    STATS_[idx].fetch_add(1, Ordering::Relaxed);
}

/// 清零全部计数器；bench 在每个测量轮次前调用。
#[doc(hidden)]
pub fn reset() {
    for stat in &STATS_ {
        stat.store(0, Ordering::Relaxed);
    }
}

/// 读取当前快照；bench 在每个测量轮次后调用。
#[doc(hidden)]
pub fn snapshot() -> [usize; STAT_COUNT] {
    let mut out = [0usize; STAT_COUNT];
    for (dst, src) in out.iter_mut().zip(STATS_.iter()) {
        *dst = src.load(Ordering::Relaxed);
    }
    out
}

/// 计数器名字，下标与 [`snapshot`] 对齐。
#[doc(hidden)]
pub fn names() -> [&'static str; STAT_COUNT] {
    [
        "push_fast_attempt",
        "push_fast_ok",
        "push_fast_full",
        "push_fast_busy",
        "pop_fast_attempt",
        "pop_fast_ok",
        "pop_fast_empty",
        "pop_fast_pending",
        "pop_fast_busy",
        "push_try_attempt",
        "push_try_ok",
        "push_try_full",
        "push_try_busy",
        "pop_try_attempt",
        "pop_try_ok",
        "pop_try_empty",
        "pop_try_pending",
        "pop_try_busy",
        "push_slow_enter",
        "push_slow_park",
        "push_slow_wake",
        "push_slow_ok",
        "push_slow_cancel",
        "push_slow_closed",
        "pop_slow_enter",
        "pop_slow_park",
        "pop_slow_wake",
        "pop_slow_ok",
        "pop_slow_cancel",
        "pop_slow_closed",
        "congested_set_prod",
        "congested_set_cons",
        "congested_clear_prod",
        "congested_clear_cons",
        "congested_clear_fail_prod",
        "congested_clear_fail_cons",
        "waker_notify_hit",
        "waker_notify_miss",
        "waker_notify_no_waker",
        "waker_poll_ready",
        "spin_push_enter",
        "spin_push_ok",
        "spin_push_skip",
        "spin_pop_enter",
        "spin_pop_ok",
        "spin_pop_skip",
        "push_slow_lock_park",
        "push_slow_lock_ready",
        "pop_slow_lock_park",
        "pop_slow_lock_ready",
    ]
}
