//! 通道调教：方向锁 allocator 与快路径自旋预算。

use alloc::alloc::Global;
use core::alloc::{Allocator, AllocatorClone};

/// 通道调教参数。
///
/// 把「生产/消费两把方向锁等待队列各自使用的 allocator」和「async 快路径失败
/// 后的有界自旋预算」收进一个类型。调用者只需要在 [`crate::MpmcChan`] 的最后
/// 一个类型参数上选一个实现，默认是 [`DefaultConfig`]；不需要逐个传 allocator，
/// 也不需要改库内常量。
///
/// 容量 `N` 与 buffer 载体 `B` 仍是独立类型参数：前者是 `MpmcBuf` 的编译期
/// 容量，后者是所有权载体（owned / `&mut` / `Box`），都不属于「调教」。
///
/// # 稳定性
///
/// 本 trait 是公开配置接口；新增关联项属于 breaking 变更。当前只有
/// [`Self::Alloc`] 与 [`Self::FAST_SPIN`]。
///
/// # Examples
///
/// ```
/// use mpmc_chan::{DefaultConfig, TrConfig};
///
/// assert_eq!(DefaultConfig::FAST_SPIN, 512);
/// ```
pub trait TrConfig {
    /// 外层 `Arc<MpmcChan>` 使用的 allocator。
    ///
    /// `Arc` 的多个引用共享同一块分配，克隆出来的 allocator 必须与分配时的
    /// allocator 等价（能用任一方释放另一方分配的内存），因此这里使用
    /// `AllocatorClone` 这个 unsafe 等价性标记，而不是裸的 `Allocator + Clone`。
    type ChanAlloc: AllocatorClone + Send + Sync;

    /// 生产方向锁等待队列（Arena）使用的 allocator。
    ///
    /// 锁本身对 allocator 的基础要求是 `Allocator + Clone`（内部会克隆
    /// allocator 交给 Arena 节点）。注意 `cooperative_arena` 的锁实现内部还有
    /// `Arc`，所以 `MpmcChan` 在具体类型上会额外要求 `AllocatorClone`；这里保留
    /// 基础约束，避免把 `AllocatorClone` 当成 `Allocator + Clone` 的别名。
    type ProdAlloc: Allocator + Clone + Send + Sync;

    /// 消费方向锁等待队列（Arena）使用的 allocator。
    ///
    /// 与 [`Self::ProdAlloc`] 独立，允许两个方向使用不同 arena/allocator；
    /// 默认配置两侧都是 `Global`。同样在 `MpmcChan` 的具体类型上额外要求
    /// `AllocatorClone`。
    type ConsAlloc: Allocator + Clone + Send + Sync;

    /// async 快路径失败后、进入慢路径前的有界自旋次数（仅方案 C）。
    ///
    /// 必须足够小以保证取消/关闭可观测（实现每 16 次检查一次），又要足够大以
    /// 覆盖对向刚释放槽位的窗口。默认 [`DefaultConfig`] 取 512。
    const FAST_SPIN: usize;
}

/// 默认调教：全局分配器 + 512 次快路径自旋。
///
/// 与引入 [`TrConfig`] 之前的实现等价；不显式指定配置类型时使用它。
///
/// # Examples
///
/// ```
/// use mpmc_chan::{DefaultConfig, MpmcBuf, MpmcChan, TrConfig};
///
/// let mut buf = MpmcBuf::<u32, 4>::new();
/// let (tx, rx) = MpmcChan::<u32, 4, _, DefaultConfig>::new(&mut buf);
/// tx.try_send(1).unwrap();
/// assert_eq!(rx.try_recv(), Ok(1));
/// assert_eq!(DefaultConfig::FAST_SPIN, 512);
/// ```
pub struct DefaultConfig;

impl TrConfig for DefaultConfig {
    type ChanAlloc = Global;
    type ProdAlloc = Global;
    type ConsAlloc = Global;
    const FAST_SPIN: usize = 512;
}
