//! 基于数组、运行时无关、`no_std` 的有界 MPMC 通道。
//!
//! 本 crate 的并发骨架是「生产侧 / 消费侧各一把
//! [`CooperativeRwLock`](atomic_sync::rwlock::cooperative_arena::CooperativeRwLockOwned)」：
//!
//! - 快路径取本方向读锁，然后在 [`MpmcBuf`] 的 Vyukov 序列环上 CAS 领取位置；
//! - 慢路径放弃读锁，改为本方向写锁；拿到写锁后如果仍满/空，就**持写锁 park**，
//!   对向使用另一把锁，因此仍能推进；
//! - 每方向只有一个 park 者，所以只需要一个单等待者 waker 槽。
//!
//! 底层缓冲 [`MpmcBuf`] 由调用方提供（可以是 `static`、`Box` 或栈上值）。channel
//! 构造时通过 [`MpmcBuf`] 内部的一个原子位完成独占 claim；同一个 buffer 同时只能
//! 对应一个 channel，重复构造返回 [`ChanBuildError::BufferInUse`]。
//!
//! # 示例
//!
//! ```
//! use mpmc_chan::{MpmcBuf, MpmcChan};
//!
//! static BUF: MpmcBuf<u32, 4> = MpmcBuf::new();
//!
//! let (tx, rx) = MpmcChan::new(&BUF).expect("buffer 已被占用");
//! tx.try_send(7).unwrap();
//! assert_eq!(rx.try_recv(), Ok(7));
//! ```
//!
//! # 分配
//!
//! 环本身不分配；但 `CooperativeRwLock` 内部用 `Arc` + Arena 承载等待队列，
//! 因此本 crate 需要一个全局分配器（或给锁注入自定义分配器）。第一次出现
//! park 者时会申请 Arena chunk，之后稳态不再分配。

#![no_std]
#![feature(impl_trait_in_assoc_type)]

extern crate alloc;

#[cfg(test)]
extern crate std;

mod buf_;
pub mod chan;

pub use buf_::MpmcBuf;
pub use chan::{
    ChanBuildError, MpmcChan, Receiver, RecvAsync, RecvError, RecvFuture, SendAsync, SendError,
    SendFuture, Sender, TryRecvError, TrySendError,
};

/// 本 crate 依赖的外部 crate 的统一重导出，避免使用方版本漂移。
///
/// 其中 `abs_cancel::TrMayCancel` 是调用 `.may_cancel_with(token)` 所需的 trait。
pub mod x_deps {
    pub use abs_cancel;
    pub use atomic_sync;
}
