//! 基于 Chunk 链表的 SlabList arena。
//!
//! # 固定 chunk 大小
//!
//! 单 Chunk 字节数来自 [`TrSlabConfig::CHUNK_SIZE`]，是**类型级编译期常量**，
//! 不会在运行期被任意字节数改变。固定大小带来三点保证：
//!
//! - chunk 分配始终使用同一个布局与对齐，长期运行不会因为大小混杂产生碎片；
//! - 槽位数在构造时按 `CHUNK_SIZE` 计算一次，不超过 `MAX_SLOTS_PER_CHUNK`
//!   （2^15），因此 `self_idx` 的 `u16` 索引与 occupied 位永远不会溢出；
//! - 槽位布局与 chunk 反推偏移固定，`SlotPtr` 的 O(1) 反向定位不依赖运行时
//!   记录。

use alloc::alloc::Global;
use core::{
    marker::PhantomData,
    ptr::{self, NonNull},
};

use crate::chan::TrSlabConfig;

use super::{
    chunk_::{
        ChunkHeader, ChunkLayout, NIL_SLOT, OCCUPIED_BIT, SLOT_INDEX_MASK, Slot, alloc_chunk_,
        chunk_of_slot_, dealloc_chunk_, slot_ptr_at_, slot_self_idx_ptr_, slots_base_,
    },
    error_::SlabListError,
    ptr_::SlotPtr,
};

/// 一个按 Chunk 批量预留槽位的 arena。
///
/// 与逐元素分配的容器不同，`SlabList` 每次需要扩容时会一次性分配一个
/// Chunk；Chunk 的全部槽位共享同一片内存，Chunk 头本身也构建在这片内存上。
/// 单 Chunk 大小由配置类型 `C` 的编译期常量 `CHUNK_SIZE` 决定。
///
/// 多个 Chunk 通过 `prev_` / `next_` 指针组成双向链表：
///
/// - 有空闲槽位的 Chunk 尽量靠近链表头部；
/// - 已经写满的 Chunk 会被移到链表尾部；
/// - 某个 Chunk 归还槽位后，会被移到链表头部，以便后续插入优先复用它。
///
/// 这是“空闲最多的 Chunk 在头部”的近似策略，而不是按 `free_count` 严格排序。
/// 链表头部始终是可分配的 Chunk（如果还存在可分配 Chunk）。
///
/// 每个槽位的尾部有一对 `u16`：
///
/// - `self_idx`：低 15 位是槽号，最高位是 occupied 标记；
/// - `next_free`：空闲链表中的下一个槽号，`u16::MAX` 表示链表结束。
///
/// 因此槽位可以乱序归还，归还的槽位会压入所属 Chunk 的空闲链表头部。
///
/// # 线程安全
///
/// `SlabList` 不是 `Sync`：所有会改动池的操作都需要 `&mut self`，共享访问必须
/// 由外部锁（等待队列的自旋锁）串行化。它在 `T: Send, C::Allocator: Send` 时
/// 实现 `Send`。
pub(crate) struct SlabList<T, C: TrSlabConfig> {
    /// 链表头部；为空表示还没有分配任何 Chunk。
    head_: *mut ChunkHeader,

    /// 链表尾部。
    tail_: *mut ChunkHeader,

    /// 分配/释放 Chunk 用的分配器实例。
    alloc_: C::Allocator,

    /// 表明 `SlabList<T, C>` 拥有 `T`，并参与 drop check。
    _marker_: PhantomData<(Slot<T>, C)>,
}

// SAFETY: 池内所有指针都只指向 `alloc_` 申请、由本池独占的 Chunk。移动
// `SlabList`（包括跨线程移动）移动的是裸指针与分配器所有权，chunk 内存本身
// 不动，因此句柄与链链接始终有效。`T: Send` 保证仍然存活的值可以随池迁移并在
// 新线程读取/析构；`C::Allocator: Send` 保证分配器状态可以迁移。`SlabList` 不是
// `Sync`（裸指针不自动实现 `Sync`），并发访问仍必须由外部锁串行化。
unsafe impl<T: Send, C: TrSlabConfig> Send for SlabList<T, C> where C::Allocator: Send {}

impl<T, C: TrSlabConfig> SlabList<T, C> {
    /// 创建一个空的 `SlabList`；Chunk 在第一次 `insert_` 时才惰性分配。
    ///
    /// `alloc` 由 arena 持有，之后分配/释放 Chunk 都使用它。Chunk 布局在构造时
    /// 按 `C::CHUNK_SIZE` 校验一次；因此后续路径不会因为非法大小失败。
    ///
    /// # Errors
    ///
    /// 当 `C::CHUNK_SIZE` 无法容纳至少一个槽位时返回
    /// [`SlabListError::ChunkTooSmall`]；当计算出的槽位数超过
    /// `MAX_SLOTS_PER_CHUNK` 时返回 [`SlabListError::TooManySlots`]；布局无法
    /// 表示时返回 [`SlabListError::LayoutOverflow`]。
    pub(crate) fn new_(alloc: C::Allocator) -> Result<Self, SlabListError> {
        // 只校验，不缓存：chunk 大小是类型级常量，布局每次可以廉价重算。
        ChunkLayout::for_type_::<T>(C::CHUNK_SIZE)?;
        Ok(SlabList {
            head_: ptr::null_mut(),
            tail_: ptr::null_mut(),
            alloc_: alloc,
            _marker_: PhantomData,
        })
    }

    /// 返回单 Chunk 的总内存大小（字节）。
    #[cfg(test)]
    pub(crate) const fn chunk_bytes_(&self) -> usize {
        C::CHUNK_SIZE
    }

    /// 返回每个 Chunk 的槽位数。
    ///
    /// 该值由 `C::CHUNK_SIZE` 和 `T` 的布局实时计算，不作为独立字段缓存。
    #[cfg(test)]
    pub(crate) fn slots_per_chunk_(&self) -> usize {
        self.chunk_layout_().slots_per_chunk_()
    }

    /// 从 `C::CHUNK_SIZE` 和 `T` 计算 Chunk 布局。
    ///
    /// `new_` 已经验证过配置，因此这里不会失败。
    fn chunk_layout_(&self) -> ChunkLayout {
        match ChunkLayout::for_type_::<T>(C::CHUNK_SIZE) {
            Ok(layout) => layout,
            Err(_) => unreachable!("SlabList 创建时已校验 Chunk 配置"),
        }
    }

    /// 在 arena 中写入一个元素，并返回指向它的句柄。
    ///
    /// 如果链表头部 Chunk 已满，会分配一个新 Chunk 并放到链表头部。元素写入后
    /// 地址保持稳定，直到它被 `remove_` 或 `clear_` 释放。
    ///
    /// # Panics
    ///
    /// 本方法不会主动 panic。底层分配失败时按全局 allocator 的错误处理器处理。
    pub(crate) fn insert_(&mut self, value: T) -> SlotPtr<T> {
        let chunk_layout = self.chunk_layout_();
        let chunk = self.ensure_free_chunk_(&chunk_layout);

        // SAFETY:
        // - chunk 由 ensure_free_chunk_ 返回，非空且 free_count_ > 0。
        // - free_head_ 是空闲链表头，因此是合法槽号。
        // - slot_ptr_at_ 的 idx 在 capacity_ 范围内。
        // - Slot<T> 的 value_ 位于槽位起始地址，slot.cast::<T>() 满足对齐，
        //   且该槽位当前未被占用，可以写入。
        unsafe {
            let idx = (*chunk).free_head_;
            debug_assert!(idx != NIL_SLOT);
            debug_assert!((idx as usize) < (*chunk).capacity_ as usize);

            let slot = slot_ptr_at_::<T>(chunk, chunk_layout.slot_offset_(), idx);
            let next_free = (*slot).next_free_;

            // 先弹出空闲链表头，再写入元素，避免元素初始化后仍被视为空闲。
            (*chunk).free_head_ = next_free;
            (*chunk).free_count_ -= 1;

            let value_ptr = slot.cast::<T>();
            value_ptr.write(value);
            (*slot).self_idx_ = OCCUPIED_BIT | idx;
            (*slot).next_free_ = NIL_SLOT;

            if (*chunk).free_count_ == 0 {
                self.move_to_tail_(chunk);
            }

            SlotPtr::from_raw_(NonNull::new_unchecked(value_ptr))
        }
    }

    /// 取回并归还一个槽位，返回其中原来的元素。
    ///
    /// 槽位会被压入所属 Chunk 的空闲链表头部；同时该 Chunk 会被移到整个 Chunk
    /// 链表的头部，以便后续 `insert_` 优先复用它。
    ///
    /// # Panics
    ///
    /// 如果 `ptr` 对应的槽位已经被归还（`self_idx` 的 occupied 位为 0），
    /// 本方法会 panic。这样可以在调试和测试中尽早发现重复归还。
    ///
    /// # 句柄有效性
    ///
    /// `ptr` 必须由本 arena 的 `insert_` 返回，并且在本 arena 存活期间保持有效。
    /// 传入其他 arena 的句柄、已经悬垂的句柄或伪造的句柄属于调用方逻辑错误，
    /// 可能触发未定义行为。
    pub(crate) fn remove_(&mut self, ptr: SlotPtr<T>) -> T {
        let chunk_layout = self.chunk_layout_();
        let slot_offset = chunk_layout.slot_offset_();

        // SAFETY:
        // - SlotPtr 的安全约定保证 ptr 指向本 arena 中一个已初始化的槽位。
        // - 槽位基址就是 T 的地址，因此可以 cast 回 Slot<T> 并读取元数据。
        unsafe {
            let slot = ptr.as_non_null_().as_ptr().cast::<Slot<T>>();
            let self_idx = ptr::read(slot_self_idx_ptr_(slot));

            assert!(
                self_idx & OCCUPIED_BIT != 0,
                "remove_ called with a freed or invalid SlotPtr"
            );

            let idx = self_idx & SLOT_INDEX_MASK;
            let chunk = chunk_of_slot_(slot, idx, slot_offset);

            debug_assert!((idx as usize) < (*chunk).capacity_ as usize);
            debug_assert!(self.contains_chunk_(chunk));
            debug_assert_eq!(slot_ptr_at_::<T>(chunk, slot_offset, idx), slot);

            // 先把元素移出，再修改槽位元数据；这样返回值不依赖空闲链表状态。
            let value = slot.cast::<T>().read();

            (*slot).self_idx_ = idx;
            (*slot).next_free_ = (*chunk).free_head_;
            (*chunk).free_head_ = idx;
            (*chunk).free_count_ += 1;

            self.move_to_head_(chunk);

            value
        }
    }

    /// 当前仍然存活的元素数量（仅供测试与诊断观测）。
    #[cfg(test)]
    pub(crate) fn occupied_len_(&self) -> usize {
        let mut total = 0;
        let mut current = self.head_;
        while !current.is_null() {
            // SAFETY: current 非空时指向有效 Chunk。
            unsafe {
                total += ((*current).capacity_ - (*current).free_count_) as usize;
                current = (*current).next_;
            }
        }
        total
    }

    /// 当前 Chunk 数量（仅供测试观测）。
    #[cfg(test)]
    pub(crate) fn chunk_count_(&self) -> usize {
        let mut count = 0;
        let mut current = self.head_;
        while !current.is_null() {
            count += 1;
            // SAFETY: current 非空时指向有效 Chunk。
            current = unsafe { (*current).next_ };
        }
        count
    }

    /// 析构全部存活元素、释放所有 Chunk，并恢复为空 arena。
    ///
    /// 释放前会先对仍处于 occupied 状态的槽位执行 `drop_in_place`；已经归还的
    /// 空闲槽位不会被重复析构。
    pub(crate) fn clear_(&mut self) {
        let chunk_layout = self.chunk_layout_();
        let slot_offset = chunk_layout.slot_offset_();
        let mut chunk = self.head_;

        while !chunk.is_null() {
            // SAFETY:
            // - chunk 是当前链表中尚未释放的节点。
            // - 每个槽位的元数据在分配 Chunk 时已经初始化，之后只会被读写而不会失效。
            // - capacity_ 与槽位数组的实际长度一致。
            // - 只有 occupied 位为 1 的槽位包含已初始化的 T，需要且只需要析构一次。
            unsafe {
                let next = (*chunk).next_;
                let capacity = (*chunk).capacity_ as usize;
                let slots = slots_base_::<T>(chunk, slot_offset);

                for i in 0..capacity {
                    let slot = slots.add(i);
                    let self_idx = ptr::read(slot_self_idx_ptr_(slot));
                    if self_idx & OCCUPIED_BIT != 0 {
                        ptr::drop_in_place(slot.cast::<T>());
                    }
                }

                dealloc_chunk_(&self.alloc_, chunk, chunk_layout.layout_());
                chunk = next;
            }
        }

        self.head_ = ptr::null_mut();
        self.tail_ = ptr::null_mut();
    }

    /// 确保链表头部指向一个有空闲槽位的 Chunk，并返回它。
    ///
    /// 根据链表的维护策略，如果头部 Chunk 已满，则所有 Chunk 都已满，因此直接
    /// 分配一个新 Chunk 放到头部即可。
    fn ensure_free_chunk_(&mut self, chunk_layout: &ChunkLayout) -> *mut ChunkHeader {
        let must_alloc = if self.head_.is_null() {
            true
        } else {
            // SAFETY: head_ 非空时指向有效 Chunk。
            unsafe { (*self.head_).free_count_ == 0 }
        };

        if must_alloc {
            self.alloc_chunk_front_(chunk_layout);
        }

        debug_assert!(!self.head_.is_null());
        // SAFETY: 上面保证 head_ 非空，且新分配的 Chunk 至少有一个空闲槽位。
        self.head_
    }

    /// 使用自身的 allocator 分配一个新 Chunk 并插入链表头部。
    fn alloc_chunk_front_(&mut self, chunk_layout: &ChunkLayout) -> *mut ChunkHeader {
        // SAFETY: chunk_layout 由 C::CHUNK_SIZE 与 T 在构造时校验，且 alloc_
        // 的类型就是 C::Allocator。
        let chunk = unsafe { alloc_chunk_::<T, C::Allocator>(&self.alloc_, *chunk_layout) };

        // SAFETY: chunk 是新分配的有效 Chunk；head_/tail_ 要么为空，
        // 要么指向链表中仍然有效的 Chunk。
        unsafe {
            (*chunk).prev_ = ptr::null_mut();
            (*chunk).next_ = self.head_;

            if self.head_.is_null() {
                self.tail_ = chunk;
            } else {
                (*self.head_).prev_ = chunk;
            }
        }

        self.head_ = chunk;
        if self.tail_.is_null() {
            self.tail_ = chunk;
        }

        chunk
    }

    /// 把一个 Chunk 移到链表头部。
    ///
    /// 调用者必须保证 `chunk` 属于本链表。
    fn move_to_head_(&mut self, chunk: *mut ChunkHeader) {
        if chunk == self.head_ {
            return;
        }

        // SAFETY: chunk 是链表中的有效节点；prev_/next_ 构成一致的双向链表。
        unsafe {
            let prev = (*chunk).prev_;
            let next = (*chunk).next_;

            debug_assert!(!prev.is_null(), "chunk list is corrupted");
            if !prev.is_null() {
                (*prev).next_ = next;
            }

            if next.is_null() {
                self.tail_ = prev;
            } else {
                (*next).prev_ = prev;
            }

            (*chunk).prev_ = ptr::null_mut();
            (*chunk).next_ = self.head_;

            if self.head_.is_null() {
                self.tail_ = chunk;
            } else {
                (*self.head_).prev_ = chunk;
            }

            self.head_ = chunk;
        }
    }

    /// 把一个 Chunk 移到链表尾部。
    ///
    /// 调用者必须保证 `chunk` 属于本链表。
    fn move_to_tail_(&mut self, chunk: *mut ChunkHeader) {
        if chunk == self.tail_ {
            return;
        }

        // SAFETY: chunk 是链表中的有效节点；prev_/next_ 构成一致的双向链表。
        unsafe {
            let prev = (*chunk).prev_;
            let next = (*chunk).next_;

            if prev.is_null() {
                self.head_ = next;
            } else {
                (*prev).next_ = next;
            }

            if next.is_null() {
                self.tail_ = prev;
            } else {
                (*next).prev_ = prev;
            }

            (*chunk).prev_ = self.tail_;
            (*chunk).next_ = ptr::null_mut();

            if self.tail_.is_null() {
                self.head_ = chunk;
            } else {
                (*self.tail_).next_ = chunk;
            }

            self.tail_ = chunk;
        }
    }

    /// 判断 `target` 是否在当前 Chunk 链表中。
    ///
    /// 该函数只用于 `debug_assert!` 的归属校验，不会进入 release 热路径。
    fn contains_chunk_(&self, target: *mut ChunkHeader) -> bool {
        let mut current = self.head_;
        while !current.is_null() {
            if current == target {
                return true;
            }
            // SAFETY: current 非空时指向有效 Chunk。
            current = unsafe { (*current).next_ };
        }
        false
    }
}

impl<T, C: TrSlabConfig> Drop for SlabList<T, C> {
    fn drop(&mut self) {
        // 自身持有 allocator，因此可以在 Drop 中完整清理；调用方无需再手动
        // `clear_`。
        self.clear_();
    }
}

impl<T> SlabList<T, crate::chan::WaitSlab16K<Global>> {
    /// 用默认的 16 KiB chunk 与 [`Global`] 分配器构造一个空 arena。
    ///
    /// # Errors
    ///
    /// 与 [`SlabList::new_`] 相同。
    #[cfg(test)]
    pub(crate) fn with_default_config_() -> Result<Self, SlabListError> {
        Self::new_(Global)
    }
}

#[cfg(test)]
mod tests_ {
    use alloc::alloc::Global;
    use core::{
        alloc::{AllocError, Allocator, Layout},
        cell::Cell,
        mem::size_of,
        ptr::NonNull,
    };
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        rc::Rc,
    };

    use crate::chan::{DEFAULT_CHUNK_BYTES, MAX_SLOTS_PER_CHUNK, SlabListConfig};

    use super::super::chunk_::{ChunkHeader, Slot};
    use super::*;

    /// 一个把分配/释放次数记录在 `Rc<Cell<_>>` 中的 allocator。
    struct CountingAlloc_ {
        allocs_: Rc<Cell<usize>>,
        deallocs_: Rc<Cell<usize>>,
    }

    // SAFETY: CountingAlloc_ 将所有分配/释放委托给 Global，并只额外记录次数；
    // 因此它满足 Allocator 对指针有效性、布局匹配和分配失败的要求。
    unsafe impl Allocator for CountingAlloc_ {
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            let allocation = Global.allocate(layout)?;
            self.allocs_.set(self.allocs_.get() + 1);
            Ok(allocation)
        }

        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
            self.deallocs_.set(self.deallocs_.get() + 1);
            // SAFETY: 调用方保证 ptr/layout 来自本分配器。
            unsafe { Global.deallocate(ptr, layout) }
        }
    }

    /// 恰好容纳两个 `Slot<u64>` 的 chunk 字节数。
    const TWO_SLOT_CHUNK: usize = size_of::<ChunkHeader>() + 2 * size_of::<Slot<u64>>();

    /// 超过单 chunk 槽位上限的 chunk 字节数。
    const TOO_MANY_SLOTS_CHUNK: usize =
        (MAX_SLOTS_PER_CHUNK + 1) * size_of::<Slot<u64>>() + 64;

    /// 测试目标：验证过小的编译期 chunk 配置在构造时被拒绝。
    /// 手段：用 `SlabListConfig<1>` 构造 `SlabList<u64, _>`。
    /// 判断：返回 `Err(SlabListError::ChunkTooSmall)`。
    /// Test goal: verify a too-small compile-time chunk configuration is rejected.
    /// Method: build `SlabList<u64, _>` with `SlabListConfig<1>`.
    /// Assertion: returns `Err(SlabListError::ChunkTooSmall)`.
    #[test]
    fn chunk_too_small_() {
        assert_eq!(
            SlabList::<u64, SlabListConfig<1>>::new_(Global).err(),
            Some(SlabListError::ChunkTooSmall)
        );
    }

    /// 测试目标：验证编译期 chunk 配置算出的槽位数超过上限时被拒绝。
    /// 手段：用足够容纳超过 `MAX_SLOTS_PER_CHUNK` 个 `Slot<u64>` 的常量构造。
    /// 判断：返回 `Err(SlabListError::TooManySlots)`。
    /// Test goal: verify a compile-time chunk with too many slots is rejected.
    /// Method: use a constant chunk size that can hold more than `MAX_SLOTS_PER_CHUNK` slots.
    /// Assertion: returns `Err(SlabListError::TooManySlots)`.
    #[test]
    fn too_many_slots_() {
        let error = SlabList::<u64, SlabListConfig<TOO_MANY_SLOTS_CHUNK>>::new_(Global).err();
        assert_eq!(error, Some(SlabListError::TooManySlots));
    }

    /// 测试目标：验证基本插入、`Deref`/`DerefMut` 与乱序归还。
    /// 手段：4 KiB chunk 的 arena 插入 1/2/3，修改其中一个，乱序 remove。
    /// 判断：句柄解引用得到原值；修改生效；remove 返回原值；存活数归零。
    /// Test goal: verify insert, Deref/DerefMut, and out-of-order removal.
    /// Method: insert 1/2/3 into a 4 KiB arena, mutate one, remove out of order.
    /// Assertion: deref yields the original values; mutation is visible; live count returns to zero.
    #[test]
    fn insert_deref_remove_() {
        let mut arena = SlabList::<u64, SlabListConfig<4096>>::new_(Global).unwrap();
        let a = arena.insert_(1);
        let mut b = arena.insert_(2);
        let c = arena.insert_(3);

        assert_eq!(*a, 1);
        assert_eq!(*b, 2);
        assert_eq!(*c, 3);

        *b = 20;
        assert_eq!(*b, 20);

        assert_eq!(arena.remove_(b), 20);
        assert_eq!(arena.remove_(a), 1);
        assert_eq!(*c, 3);
        assert_eq!(arena.remove_(c), 3);
        assert_eq!(arena.occupied_len_(), 0);
    }

    /// 测试目标：验证空闲链表 LIFO 复用最近归还的槽位。
    /// 手段：插入 a/b，归还 a，再插入 c。
    /// 判断：c 的地址与 a 相同。
    /// Test goal: verify LIFO reuse of the most recently freed slot.
    /// Method: insert a/b, free a, insert c.
    /// Assertion: c has the same address as a.
    #[test]
    fn free_list_lifo_() {
        let mut arena = SlabList::<u64, SlabListConfig<4096>>::new_(Global).unwrap();
        let a = arena.insert_(1);
        let _b = arena.insert_(2);
        let a_addr = a.as_ptr_();
        assert_eq!(arena.remove_(a), 1);
        let c = arena.insert_(3);
        assert_eq!(c.as_ptr_(), a_addr);
    }

    /// 测试目标：验证 Chunk 扩容、写满移尾、归还后回到头部并复用。
    /// 手段：用恰好容纳 2 个 `Slot<u64>` 的编译期 chunk 配置构造 arena，连续插入
    ///       3 个元素，再归还头部元素并插入第 4 个。
    /// 判断：每个 chunk 恰好 2 个槽位；插入第 3 个后 chunk 数量为 2；归还后插入
    ///       第 4 个不再新增 chunk。
    /// Test goal: verify chunk growth and reuse after returning a slot.
    /// Method: configure a chunk that holds exactly two `Slot<u64>` entries, insert three,
    ///         free one, then insert a fourth.
    /// Assertion: two slots per chunk; a second chunk appears for the third insert; the
    ///             fourth insert reuses a slot without allocating a third chunk.
    #[test]
    fn chunk_growth_and_reuse_() {
        let mut arena = SlabList::<u64, SlabListConfig<TWO_SLOT_CHUNK>>::new_(Global).unwrap();
        assert_eq!(arena.slots_per_chunk_(), 2);

        let a = arena.insert_(1);
        let _b = arena.insert_(2);
        assert_eq!(arena.chunk_count_(), 1);

        let c = arena.insert_(3);
        assert_eq!(arena.chunk_count_(), 2);

        assert_eq!(arena.remove_(a), 1);
        let d = arena.insert_(4);
        assert_eq!(arena.chunk_count_(), 2);
        assert_eq!(arena.remove_(d), 4);
        assert_eq!(arena.remove_(c), 3);
    }

    /// 测试目标：验证 Drop 只析构仍然存活的元素，不重复析构空闲槽位。
    /// 手段：用一个带 Drop 计数的类型插入 3 个元素，归还其中一个，然后 drop arena。
    /// 判断：Drop 计数为 3（归还时 1 次 + Drop 时 2 次）。
    /// Test goal: verify Drop only drops live elements, not freed slots.
    /// Method: insert three Drop-counting values, free one, then drop the arena.
    /// Assertion: the drop count is 3 (once on removal, twice on arena drop).
    #[test]
    fn drop_only_live_() {
        struct Counted_<'a>(&'a Cell<usize>);
        impl Drop for Counted_<'_> {
            fn drop(&mut self) {
                self.0.set(self.0.get() + 1);
            }
        }

        let drops = Cell::new(0);
        {
            let mut arena = SlabList::<Counted_<'_>, SlabListConfig<4096>>::new_(Global).unwrap();
            let a = arena.insert_(Counted_(&drops));
            let _b = arena.insert_(Counted_(&drops));
            let _c = arena.insert_(Counted_(&drops));
            drop(arena.remove_(a));
            assert_eq!(drops.get(), 1);
        }
        assert_eq!(drops.get(), 3);
    }

    /// 测试目标：验证 ZST 槽位仍有互不相同的地址且可归还。
    /// 手段：插入 3 个 `()`，比较句柄地址，然后逐个归还。
    /// 判断：地址互不相同；归还后存活数为 0。
    /// Test goal: verify ZST slots have distinct addresses and can be freed.
    /// Method: insert three `()` values, compare addresses, then free them.
    /// Assertion: addresses differ; occupied length returns to zero.
    #[test]
    fn zst_slots_() {
        let mut arena = SlabList::<(), SlabListConfig<256>>::new_(Global).unwrap();
        let a = arena.insert_(());
        let b = arena.insert_(());
        let c = arena.insert_(());
        assert_ne!(a.as_ptr_(), b.as_ptr_());
        assert_ne!(b.as_ptr_(), c.as_ptr_());
        assert_ne!(a.as_ptr_(), c.as_ptr_());
        arena.remove_(a);
        arena.remove_(b);
        arena.remove_(c);
        assert_eq!(arena.occupied_len_(), 0);
    }

    /// 测试目标：验证重复归还会 panic。
    /// 手段：插入后 remove 两次，第二次用 `catch_unwind` 捕获。
    /// 判断：第二次 remove panic。
    /// Test goal: verify double removal panics.
    /// Method: insert once, remove twice; catch the second panic.
    /// Assertion: the second remove panics.
    #[test]
    fn double_remove_panics_() {
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut arena = SlabList::<u64, SlabListConfig<4096>>::new_(Global).unwrap();
            let a = arena.insert_(1);
            assert_eq!(arena.remove_(a), 1);
            let _ = arena.remove_(a);
        }));
        assert!(result.is_err());
    }

    /// 测试目标：验证默认配置可用且 chunk 大小是编译期常量。
    /// 手段：调用 `with_default_config_` 并插入一个值。
    /// 判断：构造成功；`chunk_bytes_` 等于 `DEFAULT_CHUNK_BYTES`。
    /// Test goal: verify the default configuration is usable and its chunk size is a const.
    /// Method: call `with_default_config_` and insert one value.
    /// Assertion: construction succeeds; chunk size equals `DEFAULT_CHUNK_BYTES`.
    #[test]
    fn default_config_() {
        let mut arena =
            SlabList::<u64, crate::chan::WaitSlab16K<Global>>::with_default_config_().unwrap();
        assert_eq!(arena.chunk_bytes_(), DEFAULT_CHUNK_BYTES);
        let a = arena.insert_(7);
        assert_eq!(*a, 7);
    }

    /// 测试目标：验证 arena 的分配/释放都走外部 allocator。
    /// 手段：用 `CountingAlloc_` 构造，插入多个元素后 drop。
    /// 判断：分配次数与释放次数相等且大于 0。
    /// Test goal: verify chunk allocation/deallocation go through the external allocator.
    /// Method: build with `CountingAlloc_`, insert values, then drop.
    /// Assertion: allocation and deallocation counts are equal and non-zero.
    #[test]
    fn custom_allocator_() {
        let allocs = Rc::new(Cell::new(0));
        let deallocs = Rc::new(Cell::new(0));
        {
            let mut arena =
                SlabList::<u64, SlabListConfig<4096, CountingAlloc_>>::new_(CountingAlloc_ {
                    allocs_: Rc::clone(&allocs),
                    deallocs_: Rc::clone(&deallocs),
                })
                .unwrap();
            let _a = arena.insert_(1);
            let _b = arena.insert_(2);
            assert_eq!(allocs.get(), 1);
        }
        assert_eq!(allocs.get(), deallocs.get());
        assert!(allocs.get() > 0);
    }
}
