//! 通道调教：外层 Arc allocator、两把方向锁的具体类型与快路径自旋预算。

use alloc::alloc::Global;
use core::{alloc::AllocatorClone, sync::atomic::AtomicUsize};

use atomic_sync::{
    rwlock::cooperative_arena::CooperativeRwLockOwned,
    x_deps::{abs_sync::async_rwlock::TrAsyncRwLock, atomex::StrictOrderings},
};

/// 通道调教参数。
///
/// 把「外层 `Arc` 的 allocator」「生产/消费两把方向锁的具体类型」和「async 快
/// 路径失败后的有界自旋预算」收进一个类型。调用者只需要在 [`crate::MpmcChan`]
/// 的最后一个类型参数上选一个实现，默认是 [`DefaultConfig`]。
///
/// 容量 `N` 与 buffer 载体 `B` 仍是独立类型参数：前者是 `MpmcBuf` 的编译期
/// 容量，后者是所有权载体（owned / `&mut` / `Box`），都不属于「调教」。
///
/// 两把方向锁通过 GAT 指定具体类型，因此可以替换为其他
/// [`TrAsyncRwLock`] 实现（包括借用型锁）；实现者必须满足
/// [`Self::ProdGate`] / [`Self::ConsGate`] 文档中的公平性契约。
///
/// # 稳定性
///
/// 本 trait 是公开配置接口；新增/修改关联项属于 breaking 变更。
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

    /// 生产方向锁的具体类型（GAT；允许借用型锁）。
    ///
    /// # 公平性契约
    ///
    /// [`crate::MpmcChan`] 的慢路径与 D 的拥塞位清位依赖以下语义，实现者必须
    /// 保证：
    ///
    /// - 写者入队后，同方向新的 `try_read` 必须失败（写者优先），快路径不能
    ///   插队到已排队的慢写者前面；
    /// - 写者按 FIFO 获得写锁，WriterGuard Drop 必须唤醒/交接下一个写者；
    /// - 写者队列非空时 `try_write` 必须失败（D 用 `try_write` 成功来判断
    ///   「没有慢写者排队」）；
    /// - 不允许读者在写者排队时无限插队，否则慢写者可能饿死。
    ///
    /// [`TrAsyncRwLock`] 只描述方法签名，不表达上述语义；不满足时 C 的防饿死
    /// 论证和 D 的清位/公平性论证失效。默认的 `CooperativeRwLockOwned` 满足。
    type ProdGate<'a>: TrAsyncRwLock<Target = ()> + Send + Sync
    where
        Self: 'a;

    /// 消费方向锁的具体类型（GAT；允许借用型锁）。
    ///
    /// # 公平性契约
    ///
    /// 与 [`Self::ProdGate`] 相同的契约：写者优先（写者排队时 `try_read` 失败）、
    /// 写者 FIFO、WriterGuard Drop 交接下一个写者、写者队列非空时 `try_write`
    /// 失败、不允许读者无限插队。消费方向的慢路径和 D 的消费拥塞位清位同样依赖
    /// 这些语义。
    type ConsGate<'a>: TrAsyncRwLock<Target = ()> + Send + Sync
    where
        Self: 'a;

    /// async 快路径失败后、进入慢路径前的有界自旋次数（仅方案 C）。
    ///
    /// 必须足够小以保证取消/关闭可观测（实现每 16 次检查一次），又要足够大以
    /// 覆盖对向刚释放槽位的窗口。默认 [`DefaultConfig`] 取 512。
    const FAST_SPIN: usize;
}

/// 默认调教：全局 `Arc` 分配器、两把 `CooperativeRwLockOwned`、512 次自旋。
///
/// 两把方向锁都是 `CooperativeRwLockOwned<(), AtomicUsize, StrictOrderings, Global>`，
/// 满足 [`TrConfig::ProdGate`] / [`TrConfig::ConsGate`] 的公平性契约；不显式指定
/// 配置类型时使用它。
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

    type ProdGate<'a>
        = CooperativeRwLockOwned<(), AtomicUsize, StrictOrderings, Global>
    where
        Self: 'a;

    type ConsGate<'a>
        = CooperativeRwLockOwned<(), AtomicUsize, StrictOrderings, Global>
    where
        Self: 'a;

    const FAST_SPIN: usize = 512;
}
