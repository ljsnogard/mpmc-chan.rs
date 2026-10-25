//! 通道调教：方向锁 allocator 与快路径自旋预算。

use alloc::alloc::Global;
use core::alloc::AllocatorClone;

/// 通道调教参数。
///
/// 把「两把方向锁等待队列使用的 allocator」和「async 快路径失败后的有界自旋
/// 预算」收进一个类型。调用者只需要在 [`crate::MpmcChan`] 的最后一个类型参数上
/// 选一个实现，默认是 [`DefaultConfig`]；不需要逐个传 allocator，也不需要改
/// 库内常量。
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
    /// 两把方向锁等待队列（Arena）使用的 allocator。
    ///
    /// 只影响 `cooperative_arena` 读写锁内部的等待节点/Arena；`MpmcChan` 外层的
    /// `Arc` 仍使用全局分配器。
    ///
    /// 要求 `Send + Sync`，这样只要 `T: Send`，`MpmcChan` 就保持 `Send + Sync`。
    /// 不要求 `Default`：带状态 allocator 用 [`crate::MpmcChan::new_in`] 传入；
    /// 只有默认配置的 [`crate::MpmcChan::new`] 依赖 `Global: Default`。
    type Alloc: AllocatorClone + Send + Sync;

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
    type Alloc = Global;
    const FAST_SPIN: usize = 512;
}
