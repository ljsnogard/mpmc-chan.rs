//! 基于数组、运行时无关、`no_std` 的有界 MPMC 通道。
//!
//! 本 crate 的并发骨架是「生产侧 / 消费侧各一把
//! [`CooperativeRwLock`](atomic_sync::rwlock::cooperative_arena::CooperativeRwLockOwned)」：
//!
//! - 默认（方案 C）快路径取本方向读锁，然后在 [`MpmcBuf`] 的 Vyukov 序列环上
//!   CAS 领取位置；
//! - 启用 `hybrid` feature（方案 D）后，`send_async` / `recv_async` 的快路径改为
//!   「本方向拥塞位 + 无锁 CAS」，不再取读锁；`try_send` / `try_recv` 仍走读锁，
//!   避免一次失败把拥塞位永久挂住，并保留非阻塞入口对慢写者排队的尊重；
//! - 慢路径放弃快路径，改为本方向写锁；拿到写锁后如果仍满/空，就**持写锁 park**，
//!   对向使用另一把锁，因此仍能推进；
//! - 每方向只有一个 park 者，所以只需要一个单等待者 waker 槽。
//!
//! 底层缓冲 [`MpmcBuf`] 由调用方提供，channel 通过 `BorrowMut<MpmcBuf>` 接收：
//! owned `MpmcBuf` / `Box<MpmcBuf>`，或 `&mut MpmcBuf`。独占性由所有权/`&mut`
//! 借用保证，不需要运行时 claim。不可变 `static BUF` 不能安全产生 `&mut`，需要
//! `static mut` / `Box::leak` 一类手段。
//!
//! # 调教
//!
//! 外层 `Arc`、生产方向锁、消费方向锁各自的 allocator 与 async 快路径自旋
//! 预算由 [`TrConfig`] 决定，默认 [`DefaultConfig`] = 三处全局分配器 + 512 次
//! 自旋。`MpmcChan` 的最后一个类型参数是配置类型；自定义配置用
//! `MpmcChan::<..., C>::new_in(buf, chan_alloc, prod_alloc, cons_alloc)` 构造。
//! 容量 `N` 与 buffer 载体 `B` 仍是独立参数。
//!
//! # 示例
//!
//! ```
//! use mpmc_chan::{MpmcBuf, MpmcChan};
//!
//! let mut buf = MpmcBuf::<u32, 4>::new();
//! let (tx, rx) = MpmcChan::new(&mut buf);
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
#![feature(allocator_ext, impl_trait_in_assoc_type)]

extern crate alloc;

#[cfg(test)]
extern crate std;

mod buf_;
pub mod chan;

/// 基准专用计数入口；仅在 `RUSTFLAGS="--cfg mpmc_metrics"` 时编译。
///
/// 默认构建完全不包含该模块。它不属于公开 API，也不提供稳定性承诺；仅供
/// `benches/mpmc_compare.rs` 读取快/慢路径比例。
#[cfg(mpmc_metrics)]
#[doc(hidden)]
#[path = "metrics_.rs"]
pub mod bench_metrics;

pub use buf_::MpmcBuf;
pub use chan::{
    DefaultConfig, MpmcChan, Receiver, RecvAsync, RecvError, RecvFuture, SendAsync, SendError,
    SendFuture, Sender, TrConfig, TryRecvError, TrySendError,
};

/// 本 crate 依赖的外部 crate 的统一重导出，避免使用方版本漂移。
///
/// 其中 `abs_cancel::TrMayCancel` 是调用 `.may_cancel_with(token)` 所需的 trait。
pub mod x_deps {
    pub use abs_cancel;
    pub use atomic_sync;
}
