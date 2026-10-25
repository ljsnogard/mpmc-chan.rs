//! 基于数组、运行时无关、`no_std` 的有界 MPMC 通道。
//!
//! 本 crate 的并发骨架是「生产侧 / 消费侧各一个自建异步等待队列」：
//!
//! - 快路径不再取任何方向锁，直接在一个 [`MpmcBuf`] 的 Vyukov 序列环上 CAS；
//! - 快路径失败后先做有界自旋（[`TrConfig::FAST_SPIN`]），仍失败则进入本方向
//!   的等待队列；
//! - 等待节点由 [`SlabList`] 按 Chunk 批量分配，节点内保存 waker、状态与 FIFO
//!   链接；唤醒时从队首摘除并锁外 `wake`；
//! - 生产者与消费者使用**互相独立**的等待队列：消费者腾出空间只唤醒生产队首，
//!   生产者发布元素只唤醒消费队首，因此一个方向积压不会阻塞对向；
//! - 通道关闭时两个队列的全部等待者都会被唤醒并返回 `Closed` / `Cancelled`。
//!
//! 与旧版“双 `CooperativeRwLock`”实现相比，慢路径不再排队为写者、也不再持写锁
//! park。读写锁的写者队列一旦形成会挡住同方向所有快路径，导致高竞争下几乎所有
//! 操作都退化成慢路径；自建队列只在操作确实无法完成时才被使用。代价是同方向
//! FIFO 变成**近似公平**：快路径仍可能插队到已排队者之前，慢路径等待者被唤醒后
//! 也会重新与快路径竞争。语义细节见 [`Sender::send_async`] / [`Receiver::recv_async`]。
//!
//! 底层缓冲 [`MpmcBuf`] 由调用方提供，channel 通过 `BorrowMut<MpmcBuf>` 接收：
//! owned `MpmcBuf` / `Box<MpmcBuf>`，或 `&mut MpmcBuf`。独占性由所有权/`&mut`
//! 借用保证，不需要运行时 claim。不可变 `static BUF` 不能安全产生 `&mut`，需要
//! `static mut` / `Box::leak` 一类手段。
//!
//! # 调教
//!
//! 外层 `Arc` 与等待节点 SlabList 的 allocator（`C::ChanAlloc`）、生产/消费两个
//! 等待队列各自的 SlabList 配置类型（`C::ProdWaitSlab` / `C::ConsWaitSlab`）与
//! async 快路径自旋预算（`C::FAST_SPIN`）由 [`TrConfig`] 决定。SlabList 的 chunk
//! 大小是类型级编译期常量（[`SlabListConfig`]），不会在运行期被任意字节数改变：
//! 固定大小保证分配对齐一致、长期运行不产生碎片，且槽位数始终受 `u16` 索引与
//! `MAX_SLOTS_PER_CHUNK` 约束。默认 [`DefaultConfig`] = 全局分配器 + 两个
//! 16 KiB chunk + 512 次自旋。自定义配置用
//! `MpmcChan::<..., C>::new_in(buf, chan_alloc)` 构造。容量 `N` 与 buffer 载体
//! `B` 仍是独立参数。
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
//! 环本身不分配；等待队列的节点由 SlabList 承载，第一次出现 park 者时申请
//! chunk，之后稳态复用 chunk 内的槽位，不再逐节点堆分配。

#![no_std]
#![feature(allocator_ext, impl_trait_in_assoc_type)]

extern crate alloc;

#[cfg(test)]
extern crate std;

mod buf_;
pub mod chan;
mod slab_list_;

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
    DEFAULT_CHUNK_BYTES, DefaultConfig, MAX_SLOTS_PER_CHUNK, MpmcChan, Receiver, RecvAsync,
    RecvError, RecvFuture, SendAsync, SendError, SendFuture, Sender, SlabListConfig, TrConfig,
    TrSlabConfig, TryRecvError, TrySendError, WaitSlab4K, WaitSlab16K, WaitSlab64K,
};

/// 本 crate 依赖的外部 crate 的统一重导出，避免使用方版本漂移。
///
/// 其中 `abs_cancel::TrMayCancel` 是调用 `.may_cancel_with(token)` 所需的 trait。
pub mod x_deps {
    pub use abs_cancel;
}
