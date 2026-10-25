//! 通道调教：外层 Arc allocator、等待队列 SlabList 的**类型级** chunk 配置与
//! 快路径自旋预算。
//!
//! 与旧版最大的区别是：配置不再指定两把方向锁的具体类型，而是指定两个等待
//! 队列各自的 SlabList 配置类型。chunk 大小是类型级 const，编译期固定，不会
//! 在运行期被任意字节数改变——这正是为了避免长期运行时的内存碎片，以及保证
//! 单 chunk 槽位数始终能被 `u16` 索引与 [`MAX_SLOTS_PER_CHUNK`] 约束住。

use alloc::alloc::Global;
use core::{alloc::Allocator, alloc::AllocatorClone, marker::PhantomData};

/// 默认单 Chunk 字节数：16 KiB。
///
/// 等待节点通常很小（一个 waker、两个链接、一个状态），16 KiB 已能容纳数百个
/// 节点；通道配置可以按需选择其他预设。
pub const DEFAULT_CHUNK_BYTES: usize = 16 * 1024;

/// 单个 Chunk 允许的最大槽位数：2^15。
///
/// 每个槽尾部的 `self_idx` 使用最高位作为 occupied 标记，因此槽号只能使用
/// 低 15 位。固定 chunk 字节数在编译期/构造期就会对照该上限校验，不会出现
/// 运行期“超过 `u16` 阈值”的配置。
pub const MAX_SLOTS_PER_CHUNK: usize = 1 << 15;

/// SlabList 的类型级配置。
///
/// `SlabList<T, C>` 的 `T` 与 `C` 是两个独立的泛型参数：`T` 决定槽位中的
/// 元素类型，`C` 决定单 Chunk 的**编译期固定字节数**和分配 Chunk 所用的
/// allocator 类型。
///
/// 配置本身不持有 allocator 实例；`SlabList` 在构造时接收一个
/// `C::Allocator` 并自己持有，分配/释放 Chunk 时直接使用它。
///
/// # Examples
///
/// ```
/// use mpmc_chan::{SlabListConfig, TrSlabConfig};
///
/// assert_eq!(SlabListConfig::<4096>::CHUNK_SIZE, 4096);
/// ```
pub trait TrSlabConfig {
    /// 分配 Chunk 内存所使用的 allocator 类型。
    type Allocator: Allocator;

    /// 单个 Chunk 的总内存大小（字节），编译期固定。
    const CHUNK_SIZE: usize;
}

/// 内置的类型级 SlabList 配置。
///
/// 通过 const 泛型参数指定单 Chunk 字节数，通过类型参数 `A` 指定 allocator
/// 类型；默认 allocator 为 [`Global`]。这个类型本身不需要实例化，只作为
/// `SlabList<T, C>` / [`TrConfig::ProdWaitSlab`] 的类型参数使用：
///
/// ```text
/// SlabList<T, SlabListConfig<CHUNK_SIZE, A>>
/// ```
///
/// # Examples
///
/// ```
/// use mpmc_chan::{SlabListConfig, TrSlabConfig};
/// use std::alloc::Global;
///
/// type Slab = SlabListConfig<4096, Global>;
/// assert_eq!(Slab::CHUNK_SIZE, 4096);
/// ```
pub struct SlabListConfig<const CHUNK_SIZE: usize, A = Global> {
    _marker_: PhantomData<A>,
}

impl<const CHUNK_SIZE: usize, A: Allocator> TrSlabConfig for SlabListConfig<CHUNK_SIZE, A> {
    type Allocator = A;

    const CHUNK_SIZE: usize = CHUNK_SIZE;
}

/// 预设的等待队列 chunk 配置：4 KiB。
///
/// 适合等待者很少、希望每个方向只占少量内存的通道。
pub type WaitSlab4K<A = Global> = SlabListConfig<{ 4 * 1024 }, A>;

/// 预设的等待队列 chunk 配置：16 KiB。
///
/// [`DefaultConfig`] 使用该值；对常见的等待者数量是内存与批量分配之间的折中。
pub type WaitSlab16K<A = Global> = SlabListConfig<{ 16 * 1024 }, A>;

/// 预设的等待队列 chunk 配置：64 KiB。
///
/// 适合可能同时出现大量等待者的通道，可减少申请新 chunk 的次数。
pub type WaitSlab64K<A = Global> = SlabListConfig<{ 64 * 1024 }, A>;

/// 通道调教参数。
///
/// 把「外层 `Arc` 的 allocator」「生产/消费两个等待队列各自的 SlabList 配置
/// 类型」和「async 快路径失败后的有界自旋预算」收进一个类型。调用者只需要在
/// [`crate::MpmcChan`] 的最后一个类型参数上选一个实现，默认是 [`DefaultConfig`]。
///
/// 容量 `N` 与 buffer 载体 `B` 仍是独立类型参数：前者是 `MpmcBuf` 的编译期
/// 容量，后者是所有权载体（owned / `&mut` / `Box`），都不属于「调教」。
///
/// # 稳定性
///
/// 本 trait 是公开配置接口；新增/修改关联项属于 breaking 变更。
///
/// # Examples
///
/// ```
/// use mpmc_chan::{DefaultConfig, TrConfig, TrSlabConfig};
///
/// assert_eq!(DefaultConfig::FAST_SPIN, 512);
/// assert_eq!(
///     <DefaultConfig as TrConfig>::ProdWaitSlab::CHUNK_SIZE,
///     16 * 1024,
/// );
/// ```
pub trait TrConfig {
    /// 外层 `Arc<MpmcChan>` 使用的 allocator。
    ///
    /// `Arc` 的多个引用共享同一块分配，克隆出来的 allocator 必须与分配时的
    /// allocator 等价（能用任一方释放另一方分配的内存），因此这里使用
    /// `AllocatorClone` 这个 unsafe 等价性标记，而不是裸的 `Allocator + Clone`。
    ///
    /// 该类型同时是等待节点 SlabList 的 allocator：`ProdWaitSlab` /
    /// `ConsWaitSlab` 的 `Allocator` 都必须等于它。
    type ChanAlloc: AllocatorClone + Send + Sync;

    /// 生产方向等待队列的 SlabList 配置类型。
    ///
    /// chunk 大小由该类型携带的 const 泛型参数在编译期固定；可从
    /// [`WaitSlab4K`]、[`WaitSlab16K`]、[`WaitSlab64K`] 中选择，或用
    /// [`SlabListConfig`] 自定义一个编译期常量。
    type ProdWaitSlab: TrSlabConfig<Allocator = Self::ChanAlloc>;

    /// 消费方向等待队列的 SlabList 配置类型。
    ///
    /// 约束与 [`Self::ProdWaitSlab`] 相同。
    type ConsWaitSlab: TrSlabConfig<Allocator = Self::ChanAlloc>;

    /// async 快路径失败后、进入等待队列前的有界自旋次数。
    ///
    /// 必须足够小以保证取消/关闭可观测（实现每 16 次检查一次），又要足够大以
    /// 覆盖对向刚释放槽位的窗口。默认 [`DefaultConfig`] 取 512。
    const FAST_SPIN: usize;
}

/// 默认调教：全局分配器、两个 16 KiB 等待队列 chunk、512 次自旋。
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

    type ProdWaitSlab = WaitSlab16K<Global>;
    type ConsWaitSlab = WaitSlab16K<Global>;

    const FAST_SPIN: usize = 512;
}
