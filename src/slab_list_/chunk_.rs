//! SlabList 的 Chunk 内存布局与底层操作。
//!
//! 一个 Chunk 使用一次 `alloc` 分配，布局如下：
//!
//! ```text
//! +----------------------+  <- Chunk 基址（也是 ChunkHeader 的地址）
//! | ChunkHeader          |
//! |  prev / next         |
//! |  free_head           |
//! |  free_count          |
//! |  capacity            |
//! +----------------------+  <- slot_offset（按 Slot<T> 对齐）
//! | Slot<T> #0           |
//! |  value: MaybeUninit  |
//! |  self_idx: u16       |
//! |  next_free: u16      |
//! +----------------------+
//! | Slot<T> #1           |
//! | ...                  |
//! +----------------------+
//! ```
//!
//! `Slot<T>` 的 `value_` 位于槽位起始地址，因此指向 `T` 的指针同时也是
//! 槽位基址。`self_idx_` 的低 15 位保存槽号，最高位保存 occupied 标记；
//! `next_free_` 保存空闲链表中的下一个槽号，`u16::MAX` 表示链表结束。
//!
//! # 地址稳定性
//!
//! Chunk 由分配器整块申请，之后不会搬移；槽位只要还存活，地址就不会改变。
//! 因此 `SlotPtr` 可以长期保存，直接用作等待队列节点之间的链接。

use core::{
    alloc::{Allocator, Layout},
    mem::{MaybeUninit, align_of, size_of},
    ptr::{self, NonNull},
};

use alloc::alloc::handle_alloc_error;

use crate::chan::MAX_SLOTS_PER_CHUNK;

use super::error_::SlabListError;

/// `self_idx_` 的 occupied 标记位。
pub(super) const OCCUPIED_BIT: u16 = 1 << 15;

/// 从 `self_idx_` 中提取槽号时使用的掩码。
pub(super) const SLOT_INDEX_MASK: u16 = OCCUPIED_BIT - 1;

/// 空闲链表结束标记。
pub(super) const NIL_SLOT: u16 = u16::MAX;

/// Chunk 头，位于每个 Chunk 分配内存的起始位置。
#[repr(C)]
pub(super) struct ChunkHeader {
    /// 前一个 Chunk；链表头部为 null。
    pub(super) prev_: *mut ChunkHeader,

    /// 后一个 Chunk；链表尾部为 null。
    pub(super) next_: *mut ChunkHeader,

    /// 空闲链表头部的槽号；没有空闲槽时为 [`NIL_SLOT`]。
    pub(super) free_head_: u16,

    /// 当前空闲槽数量。
    pub(super) free_count_: u16,

    /// 本 Chunk 的槽位总数。
    pub(super) capacity_: u16,
}

/// 一个槽位：元素数据在前，两个 u16 元数据在后。
#[repr(C)]
pub(super) struct Slot<T> {
    /// 尚未初始化或正在使用的元素。
    pub(super) value_: MaybeUninit<T>,

    /// 低 15 位是槽号，最高位是 occupied 标记。
    pub(super) self_idx_: u16,

    /// 空闲链表中的下一个槽号；[`NIL_SLOT`] 表示没有下一个空闲槽。
    pub(super) next_free_: u16,
}

/// 由 Chunk 字节数与具体元素类型 `T` 计算出的 Chunk 布局。
///
/// 该值在 `SlabList::new_` 时计算一次并缓存；之后分配/归还槽位只读缓存。
#[derive(Clone, Copy, Debug)]
pub(super) struct ChunkLayout {
    /// 单 Chunk 的分配布局。
    pub(super) layout_: Layout,

    /// 槽位数组相对于 Chunk 基址的偏移。
    pub(super) slot_offset_: usize,

    /// 每个 Chunk 的槽位数。
    pub(super) slots_per_chunk_: u16,
}

impl ChunkLayout {
    /// 根据单 Chunk 字节数与具体元素类型 `T` 计算布局。
    ///
    /// # Errors
    ///
    /// - Chunk 字节数不足以容纳 header、对齐填充和至少一个槽位时返回
    ///   [`SlabListError::ChunkTooSmall`]。
    /// - 槽位数超过 [`MAX_SLOTS_PER_CHUNK`] 时返回 [`SlabListError::TooManySlots`]。
    /// - 布局无法表示时返回 [`SlabListError::LayoutOverflow`]。
    pub(super) fn for_type_<T>(chunk_bytes: usize) -> Result<Self, SlabListError> {
        let slot_offset = slot_offset_::<T>().ok_or(SlabListError::LayoutOverflow)?;
        let stride = size_of::<Slot<T>>();
        debug_assert!(stride > 0, "Slot<T> 至少包含两个 u16 元数据");

        let available = chunk_bytes
            .checked_sub(slot_offset)
            .ok_or(SlabListError::ChunkTooSmall)?;
        let slots = available / stride;

        if slots == 0 {
            return Err(SlabListError::ChunkTooSmall);
        }
        if slots > MAX_SLOTS_PER_CHUNK {
            return Err(SlabListError::TooManySlots);
        }

        let layout = Layout::from_size_align(chunk_bytes, chunk_align_::<T>())
            .map_err(|_| SlabListError::LayoutOverflow)?;

        Ok(ChunkLayout {
            layout_: layout,
            slot_offset_: slot_offset,
            slots_per_chunk_: slots as u16,
        })
    }

    /// 返回单 Chunk 的分配布局。
    pub(super) const fn layout_(&self) -> Layout {
        self.layout_
    }

    /// 返回槽位数组相对于 Chunk 基址的偏移。
    pub(super) const fn slot_offset_(&self) -> usize {
        self.slot_offset_
    }

    /// 返回每个 Chunk 的槽位数。
    #[cfg(test)]
    pub(super) const fn slots_per_chunk_(&self) -> usize {
        self.slots_per_chunk_ as usize
    }
}

/// 把 `value` 向上对齐到 `align`。
///
/// `align` 必须是 2 的幂。返回值使用 `Option` 表达加法溢出。
const fn align_up_(value: usize, align: usize) -> Option<usize> {
    let mask = align - 1;
    match value.checked_add(mask) {
        Some(padded) => Some(padded & !mask),
        None => None,
    }
}

/// 返回槽位数组相对于 Chunk 基址的偏移。
///
/// 偏移会按 `Slot<T>` 的对齐要求向上取整。Chunk 分配本身按
/// `max(align_of::<ChunkHeader>(), align_of::<Slot<T>>())` 对齐，
/// 因此基址加上该偏移后仍然满足 `Slot<T>` 的对齐要求。
pub(super) const fn slot_offset_<T>() -> Option<usize> {
    align_up_(size_of::<ChunkHeader>(), align_of::<Slot<T>>())
}

/// 返回 Chunk 分配所需的整体对齐。
fn chunk_align_<T>() -> usize {
    align_of::<ChunkHeader>().max(align_of::<Slot<T>>())
}

/// 返回 Chunk 中槽位数组的起始地址。
///
/// # Safety
///
/// - `chunk` 必须指向一个按 [`ChunkLayout::for_type_`] 分配并初始化的 Chunk。
/// - `slot_offset` 必须等于 [`ChunkLayout::slot_offset_`]，且该 Chunk 至少有一个槽位。
pub(super) unsafe fn slots_base_<T>(chunk: *mut ChunkHeader, slot_offset: usize) -> *mut Slot<T> {
    // SAFETY: 调用者保证 chunk 指向有效的 Chunk 分配，slot_offset 在分配范围内
    // 且按 Slot<T> 对齐。Chunk 基址本身按 Slot<T> 对齐，因此相加后仍然对齐。
    unsafe { (chunk as *mut u8).add(slot_offset).cast::<Slot<T>>() }
}

/// 返回 Chunk 中第 `idx` 个槽位的地址。
///
/// # Safety
///
/// - `chunk`、`slot_offset` 的要求同 [`slots_base_`]。
/// - `idx` 必须小于该 Chunk 的 `capacity_`。
pub(super) unsafe fn slot_ptr_at_<T>(
    chunk: *mut ChunkHeader,
    slot_offset: usize,
    idx: u16,
) -> *mut Slot<T> {
    // SAFETY: 调用者保证 idx 在槽位范围内。
    unsafe { slots_base_::<T>(chunk, slot_offset).add(idx as usize) }
}

/// 返回槽位中 `self_idx_` 字段的地址。
///
/// # Safety
///
/// `slot` 必须指向一个已经开始初始化、且元数据仍然有效的 `Slot<T>`。
pub(super) unsafe fn slot_self_idx_ptr_<T>(slot: *mut Slot<T>) -> *mut u16 {
    // SAFETY: 调用者保证 slot 有效；addr_of_mut! 不会创建对字段的引用。
    unsafe { ptr::addr_of_mut!((*slot).self_idx_) }
}

/// 由槽位地址与槽号反推出所属 Chunk 的基址。
///
/// 槽位在 Chunk 内连续排列，因此：
///
/// ```text
/// slot0 = slot - idx * size_of::<Slot<T>>()
/// chunk = slot0 - slot_offset
/// ```
///
/// # Safety
///
/// - `slot` 必须指向一个有效 Chunk 中的有效槽位。
/// - `idx` 必须等于该槽位 `self_idx_` 的低 15 位。
/// - `slot_offset` 必须等于 [`slot_offset_::<T>`]。
pub(super) unsafe fn chunk_of_slot_<T>(
    slot: *mut Slot<T>,
    idx: u16,
    slot_offset: usize,
) -> *mut ChunkHeader {
    // SAFETY: 调用者保证 idx 是当前槽位在 Chunk 内的真实下标，
    // 因此 slot.sub(idx) 仍位于同一 Chunk 的槽位数组中。
    let slot0 = unsafe { slot.sub(idx as usize) };
    // SAFETY: slot0 是槽位数组起始地址，减去 slot_offset 得到 Chunk 基址。
    unsafe { (slot0 as *mut u8).sub(slot_offset).cast::<ChunkHeader>() }
}

/// 使用 allocator 分配并初始化一个 Chunk。
///
/// 初始化后的 Chunk 拥有一个包含全部槽位的空闲链表，链表顺序为
/// `0 -> 1 -> ... -> capacity - 1 -> NIL`。
///
/// # Safety
///
/// - `chunk_layout` 必须来自 [`ChunkLayout::for_type_::<T>`] 的成功计算。
/// - `allocator` 必须能够分配 `chunk_layout.layout_` 描述的布局。
///
/// 分配失败时按全局 allocator 的错误处理器处理（通常是 abort）。
pub(super) unsafe fn alloc_chunk_<T, A>(allocator: &A, chunk_layout: ChunkLayout) -> *mut ChunkHeader
where
    A: Allocator,
{
    let layout = chunk_layout.layout_;

    // SAFETY: 调用者保证 allocator 与 layout 匹配。
    // allocate 返回的指针按 layout.align 对齐，因而同时满足 ChunkHeader
    // 与 Slot<T> 的对齐要求。
    let allocation = match allocator.allocate(layout) {
        Ok(allocation) => allocation,
        Err(_) => handle_alloc_error(layout),
    };
    let base = allocation.as_ptr() as *mut u8;

    let chunk = base.cast::<ChunkHeader>();

    // SAFETY:
    // - base 指向一块至少 layout.size() 字节的有效内存。
    // - slot_offset + slots * size_of::<Slot<T>>() <= layout.size()，
    //   这个不等式由 ChunkLayout::for_type_ 的计算保证。
    // - base 按 chunk_align_::<T>() 对齐，slot_offset 按 Slot<T> 对齐，
    //   因此槽位数组的每个元素都满足 Slot<T> 的对齐要求。
    unsafe {
        chunk.write(ChunkHeader {
            prev_: ptr::null_mut(),
            next_: ptr::null_mut(),
            free_head_: 0,
            free_count_: chunk_layout.slots_per_chunk_,
            capacity_: chunk_layout.slots_per_chunk_,
        });

        let slots_ptr = base.add(chunk_layout.slot_offset_).cast::<Slot<T>>();
        let capacity = chunk_layout.slots_per_chunk_ as usize;
        for i in 0..capacity {
            let next_free = if i + 1 < capacity {
                (i + 1) as u16
            } else {
                NIL_SLOT
            };
            slots_ptr.add(i).write(Slot {
                value_: MaybeUninit::uninit(),
                self_idx_: i as u16,
                next_free_: next_free,
            });
        }
    }

    chunk
}

/// 使用 allocator 释放一个由 [`alloc_chunk_`] 分配的 Chunk。
///
/// # Safety
///
/// - `chunk` 必须由同一个 allocator 的 `alloc_chunk_` 返回，且尚未被释放。
/// - `layout` 必须与分配时使用的 layout 完全一致。
pub(super) unsafe fn dealloc_chunk_<A>(allocator: &A, chunk: *mut ChunkHeader, layout: Layout)
where
    A: Allocator,
{
    // SAFETY: 调用者保证 chunk 与 layout 的匹配关系。
    unsafe {
        allocator.deallocate(NonNull::new_unchecked(chunk.cast::<u8>()), layout);
    }
}
