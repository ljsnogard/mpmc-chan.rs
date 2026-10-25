//! SlabList：按 Chunk 批量分配、直接返回元素句柄的 arena。
//!
//! 本模块从 `bplus_tree_cmp` 的 `slab_list_` 移植而来，并按 `mpmc-chan` 的需求
//! 做了一点调整：
//!
//! - arena 自己持有 allocator 实例，`insert_` / `remove_` / `clear_` 不再需要
//!   额外借入 allocator；
//! - 单 Chunk 大小保留类型级配置 [`TrSlabConfig::CHUNK_SIZE`](crate::TrSlabConfig)，
//!   在编译期固定。这样每次 chunk 分配都使用同一布局与对齐，长期运行不会产生
//!   碎片；槽位数也能在构造时对照 `MAX_SLOTS_PER_CHUNK` 校验，避免 `u16` 索引
//!   溢出。
//!
//! 模块内部结构：
//!
//! - `chunk_`：Chunk 头、槽位布局与底层内存操作；
//! - `error_`：创建阶段的错误类型；
//! - `ptr_`：指向槽位元素的 [`SlotPtr`]；
//! - `list_`：Chunk 链表、槽位分配/归还与析构。
//!
//! 本模块是 crate 私有实现，只向 `chan::wait_queue_` 导出 [`SlabList`] 与
//! [`SlotPtr`]；错误类型 `SlabListError` 只在模块内部使用。
//!
//! # 为什么等待队列需要它
//!
//! 等待节点必须能长期保存 waker 与队列链接，并在取消/唤醒后回收复用。逐节点
//! 堆分配会让慢路径每次 park 都触发一次分配；按 Chunk 批量分配后，除申请新
//! Chunk 外，节点分配/回收只在 chunk 内做 `u16` 索引操作。
//!
//! # 安全边界
//!
//! [`SlotPtr`] 是无生命周期的裸指针句柄，类型系统无法约束它的有效期；调用方
//! 必须保证句柄来自仍然存活的同一 arena、没有重复归还、且不产生别名 `&mut`。

mod chunk_;
mod error_;
mod list_;
mod ptr_;

pub(crate) use list_::SlabList;
pub(crate) use ptr_::SlotPtr;
