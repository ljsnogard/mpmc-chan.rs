//! 有界 MPMC 环的底层缓冲。
//!
//! 采用 Dmitry Vyukov 的有界序列环：每个槽位有一个单调序列号，生产/消费通过
//! CAS `tail` / `head` 领取绝对位置，再用序列号判断该位置是否已发布/已回收。
//!
//! # 为什么 `N >= 2`
//!
//! 消费者回收槽位时写入 `head + N`。若 `N == 1`，这个值等于 `head + 1`，与
//! 生产者发布的值相同，生产者将无法区分「消费者还没读」与「消费者已读完」，
//! 从而覆盖正在读取的值。因此 Vyukov 环要求 `N >= 2`；容量 1 不在本实现范围。
//!
//! # 独占与 reset
//!
//! channel 通过 **owned `MpmcBuf`** 或 **`&mut MpmcBuf`** 借用获得独占访问；不再
//! 需要运行时 claim 位。构造时 reset，Drop 时 drain + reset；`MpmcBuf` 本身不实现
//! `Drop`。不可变 `static BUF` 无法安全地产生 `&mut`，因此需要 `static mut` /
//! `Box::leak` 一类手段；详见 `MpmcChan` 的文档。

use core::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
};

/// 环中的一个槽位。
///
/// `seq_` 与 `value_` 放在同一个结构里；这是 Vyukov 算法的经典布局。
struct Slot<T> {
    seq_: AtomicUsize,
    value_: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Slot<T> {
    /// 构造一个空槽；`seq_` 的初值由 [`MpmcBuf::new`] 的循环写成下标。
    const fn new_() -> Self {
        Slot {
            seq_: AtomicUsize::new(0),
            value_: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

/// 出队状态。
///
/// 必须把「环里确实没有位置」（`Empty`）与「位置已预留但生产者尚未发布」
/// （`Pending`）区分开：关闭后 drain 的语义下，后者应继续等待发布，而不是直接
/// 返回 `Closed` 把在途元素留给 Drop。
pub(crate) enum Pop_<T> {
    /// 取到一个元素。
    Value(T),
    /// `head == tail`，确实为空。
    Empty,
    /// `head != tail`，但头槽尚未发布（生产者正在写）。
    Pending,
}

/// 有界 MPMC 环的底层缓冲。
///
/// 公开类型、私有字段：调用方只能构造它并交给 [`crate::MpmcChan`]，不能直接读写
/// 槽位数据。`N` 必须是 2 的幂且 `N >= 2`。
///
/// # Examples
///
/// ```
/// use mpmc_chan::MpmcBuf;
///
/// static BUF: MpmcBuf<u64, 8> = MpmcBuf::new();
/// assert_eq!(BUF.capacity(), 8);
/// ```
pub struct MpmcBuf<T, const N: usize> {
    slots_: [Slot<T>; N],
    head_: AtomicUsize,
    tail_: AtomicUsize,
}

impl<T, const N: usize> MpmcBuf<T, N> {
    /// 构造一个未占用的空 buffer；槽位序列号按 Vyukov 要求写成 `0..N`。
    ///
    /// # Panics
    ///
    /// 编译期要求 `N` 是 2 的幂且 `N >= 2`；不满足时编译失败，不会运行到 panic。
    pub const fn new() -> Self {
        const { assert!(N.is_power_of_two(), "N 必须是 2 的正整数次幂") };
        const { assert!(N >= 2, "Vyukov 环要求 N >= 2") };

        let mut slots = [const { Slot::new_() }; N];
        let mut i = 0;
        while i < N {
            slots[i].seq_ = AtomicUsize::new(i);
            i += 1;
        }

        MpmcBuf {
            slots_: slots,
            head_: AtomicUsize::new(0),
            tail_: AtomicUsize::new(0),
        }
    }

    /// 返回容量 `N`。
    #[inline]
    pub const fn capacity(&self) -> usize {
        N
    }

    /// 返回当前已领取但尚未消费的位置数（近似值）。
    ///
    /// 该值包含「生产者已 CAS `tail`、但尚未发布值」的位置，因此不是严格的
    /// 可读元素个数；仅用于诊断/启发式判断。
    #[inline]
    pub(crate) fn len_(&self) -> usize {
        let head = self.head_.load(Ordering::Acquire);
        let tail = self.tail_.load(Ordering::Acquire);
        tail.wrapping_sub(head)
    }

    /// 重置为空环：`head = tail = 0`，槽位 `seq = 0..N`。
    ///
    /// 调用者必须拥有该 buffer 的独占访问（owned 或 `&mut`），且保证没有并发的
    /// push/pop。
    #[inline]
    pub(crate) fn reset_(&self) {
        for (i, slot) in self.slots_.iter().enumerate() {
            slot.seq_.store(i, Ordering::Relaxed);
        }
        self.head_.store(0, Ordering::Relaxed);
        self.tail_.store(0, Ordering::Relaxed);
    }

    /// 尝试入队一个元素；满时把元素原样还回。
    ///
    /// 这是 Vyukov 序列环的生产者侧。`tail` 的 CAS 领取绝对位置；成功后才写入
    /// 值并用 `Release` 发布序列号。消费者通过 `Acquire` 读取序列号，从而看到
    /// 完整的值。
    pub(crate) fn try_push_(&self, value: T) -> Result<(), T> {
        let mut tail = self.tail_.load(Ordering::Relaxed);

        loop {
            let slot = &self.slots_[tail & (N - 1)];
            let seq = slot.seq_.load(Ordering::Acquire);
            let dif = seq.wrapping_sub(tail) as isize;

            if dif == 0 {
                match self.tail_.compare_exchange_weak(
                    tail,
                    tail.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: 本线程刚刚通过 tail CAS 独占了该位置；在 seq 发布
                        // 之前消费者不会读取 value_。写入是 MaybeUninit 的普通
                        // 初始化，不运行用户代码。
                        unsafe {
                            slot.value_.get().write(MaybeUninit::new(value));
                        }
                        // Release：发布 value_ 与「该位置已可读」。
                        slot.seq_.store(tail.wrapping_add(1), Ordering::Release);
                        return Ok(());
                    }
                    Err(actual) => tail = actual,
                }
            } else if dif < 0 {
                // 槽位还停留在上一圈：环满。
                return Err(value);
            } else {
                // 另一个生产者已经推进 tail；重读后再试。
                tail = self.tail_.load(Ordering::Relaxed);
            }
        }
    }

    /// 尝试出队一个元素；空时返回 `None`。
    ///
    /// 这是给测试/诊断用的简化入口；channel 内部使用 [`Self::try_pop_state_`]，
    /// 以区分「真空」与「已预留未发布」。
    #[cfg(test)]
    pub(crate) fn try_pop_(&self) -> Option<T> {
        match self.try_pop_state_() {
            Pop_::Value(value) => Option::Some(value),
            Pop_::Empty | Pop_::Pending => Option::None,
        }
    }

    /// 尝试出队，并区分空/未发布。
    pub(crate) fn try_pop_state_(&self) -> Pop_<T> {
        let mut head = self.head_.load(Ordering::Relaxed);

        loop {
            let slot = &self.slots_[head & (N - 1)];
            let seq = slot.seq_.load(Ordering::Acquire);
            let dif = seq.wrapping_sub(head.wrapping_add(1)) as isize;

            if dif == 0 {
                match self.head_.compare_exchange_weak(
                    head,
                    head.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: 本线程刚刚通过 head CAS 独占了该位置；seq 的
                        // Acquire 读取与生产者的 Release 发布配对，值已初始化。
                        let value = unsafe { slot.value_.get().read().assume_init() };
                        // Release：告诉生产者该槽位可以进入下一圈。
                        slot.seq_.store(head.wrapping_add(N), Ordering::Release);
                        return Pop_::Value(value);
                    }
                    Err(actual) => head = actual,
                }
            } else if dif < 0 {
                let tail = self.tail_.load(Ordering::Acquire);
                return if head == tail {
                    Pop_::Empty
                } else {
                    Pop_::Pending
                };
            } else {
                head = self.head_.load(Ordering::Relaxed);
            }
        }
    }

    /// 丢弃环内所有已发布但尚未消费的元素，并把 buffer 重置为空。
    ///
    /// 只能在 channel 构造/Drop（或测试）且保证没有并发 push/pop 时调用。
    pub(crate) fn drop_remaining_(&self) {
        let head = self.head_.load(Ordering::Relaxed);
        let tail = self.tail_.load(Ordering::Relaxed);
        let mut pos = head;

        while pos != tail {
            let slot = &self.slots_[pos & (N - 1)];
            // 只有「已发布未消费」的槽位持有初始化好的 T。
            if slot.seq_.load(Ordering::Relaxed) == pos.wrapping_add(1) {
                // SAFETY: 该位置已发布且尚未消费；Drop 路径没有并发操作，
                // 因此可以独占读取并析构。
                unsafe {
                    drop(slot.value_.get().read().assume_init());
                }
            }
            pos = pos.wrapping_add(1);
        }

        self.reset_();
    }
}

impl<T, const N: usize> Default for MpmcBuf<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: 移动 `MpmcBuf` 等同于移动其中可能已初始化的 T；要求 T: Send。
unsafe impl<T: Send, const N: usize> Send for MpmcBuf<T, N> {}

// SAFETY: `&MpmcBuf` 只通过 crate 内部的 Vyukov 协议读写槽位：任一时刻一个
// 槽位只属于一个生产者或一个消费者；channel 通过 owned `MpmcBuf` 或 `&mut`
// 借用保证一个 buffer 只有一个 channel。跨线程移动 T 要求 T: Send；T 不需要
// Sync，因为不会把 &T 共享给多个线程。
unsafe impl<T: Send, const N: usize> Sync for MpmcBuf<T, N> {}

#[cfg(test)]
mod tests_ {
    use alloc::{sync::Arc, vec::Vec};
    use core::sync::atomic::{AtomicUsize, Ordering};

    use super::MpmcBuf;

    /// 测试目标：验证空 buffer 的 push/pop 基本语义与 FIFO 顺序。
    /// 手段：容量 4 的 buffer 依次 push 1..=4，再依次 pop。
    /// 判断：push 全部成功；pop 顺序必须为 1,2,3,4；随后 pop 返回 None。
    /// Test goal: verify basic push/pop and FIFO order on an empty buffer.
    /// Method: push 1..=4 into a capacity-4 buffer, then pop repeatedly.
    /// Assertion: all pushes succeed; pops are exactly 1,2,3,4; then None.
    #[test]
    fn fifo_() {
        let buf = MpmcBuf::<u32, 4>::new();
        for v in 1..=4 {
            assert_eq!(buf.try_push_(v), Ok(()));
        }
        for v in 1..=4 {
            assert_eq!(buf.try_pop_(), Some(v));
        }
        assert!(buf.try_pop_().is_none());
    }

    /// 测试目标：验证满时 push 把元素原样还回，且腾出一个位置后可继续入队。
    /// 手段：填满容量 2 的 buffer，再 push 第三个元素；pop 一个后重新 push。
    /// 判断：第三次 push 返回 Err(3)（元素未被吞掉）；pop 得 1；随后 push(3) 成功。
    /// Test goal: verify push returns the value when full and succeeds after a pop.
    /// Method: fill a capacity-2 buffer, push a third value, pop one, push again.
    /// Assertion: the third push returns Err(3); pop yields 1; then push(3) succeeds.
    #[test]
    fn full_returns_value_() {
        let buf = MpmcBuf::<u32, 2>::new();
        assert_eq!(buf.try_push_(1), Ok(()));
        assert_eq!(buf.try_push_(2), Ok(()));
        assert_eq!(buf.try_push_(3), Err(3));
        assert_eq!(buf.try_pop_(), Some(1));
        assert_eq!(buf.try_push_(3), Ok(()));
        assert_eq!(buf.try_pop_(), Some(2));
        assert_eq!(buf.try_pop_(), Some(3));
    }

    /// 测试目标：验证 reset 清空空 buffer，drop_remaining_ 清空有元素的 buffer。
    /// 手段：空 buffer 先 reset；push 一个值后调用 drop_remaining_，再 pop。
    /// 判断：reset 后 pop 为 None；push 后 len=1；drop_remaining_ 后 pop 为 None。
    /// Test goal: verify reset on empty and drop_remaining_ on a non-empty buffer.
    /// Method: reset an empty buffer; push a value, call drop_remaining_, then pop.
    /// Assertion: pop is None after reset; len=1 after push; pop is None after drop_remaining_.
    #[test]
    fn reset_empty_() {
        let buf = MpmcBuf::<u32, 4>::new();
        buf.reset_();
        assert!(buf.try_pop_().is_none());

        assert_eq!(buf.try_push_(9), Ok(()));
        assert_eq!(buf.len_(), 1);
        buf.drop_remaining_();
        assert!(buf.try_pop_().is_none());
    }

    /// 测试目标：验证 `try_pop_state_` 能区分「已预留未发布」与「真空」。
    /// 手段：手动把 tail 从 0 推进到 1（模拟生产者已 CAS 领取位置但尚未写值），
    ///       先检查 Pending，再手动写入 value/seq，检查 Value；最后消费后检查 Empty。
    /// 判断：第一次为 Pending，第二次为 Value(7)，第三次为 Empty。
    /// Test goal: verify `try_pop_state_` distinguishes reserved-unpublished from truly empty.
    /// Method: manually advance tail 0 -> 1, check Pending; publish value/seq, check Value;
    ///         consume, then check Empty.
    /// Assertion: Pending, then Value(7), then Empty.
    #[test]
    fn pop_state_pending_empty_() {
        use core::mem::MaybeUninit;
        use core::sync::atomic::Ordering;

        use super::Pop_;

        let buf = MpmcBuf::<u32, 2>::new();
        assert_eq!(
            buf.tail_
                .compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed),
            Ok(0)
        );

        assert!(matches!(buf.try_pop_state_(), Pop_::Pending));

        // SAFETY: 测试单线程；模拟生产者自己写入预留位置并发布。
        unsafe {
            buf.slots_[0].value_.get().write(MaybeUninit::new(7));
        }
        buf.slots_[0].seq_.store(1, Ordering::Release);

        assert!(matches!(buf.try_pop_state_(), Pop_::Value(7)));
        assert!(matches!(buf.try_pop_state_(), Pop_::Empty));
    }

    /// 测试目标：验证 Drop 路径会析构所有已发布未消费的元素，并重置为空。
    /// 手段：用一个带 Drop 计数的类型 push 3 个元素，调用 drop_remaining_。
    /// 判断：Drop 计数变为 3；随后 pop 为 None；再次 push 可用。
    /// Test goal: verify drop_remaining_ drops published unconsumed values and resets.
    /// Method: push three Drop-counting values, call drop_remaining_.
    /// Assertion: drop count is 3; pop yields None; the buffer can be reused.
    /// Test goal / 判定与手段见上。
    #[test]
    fn drop_remaining_drops_values_() {
        struct Counted<'a>(&'a AtomicUsize);
        impl Drop for Counted<'_> {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = AtomicUsize::new(0);
        let buf = MpmcBuf::<Counted<'_>, 4>::new();
        for _ in 0..3 {
            assert!(buf.try_push_(Counted(&drops)).is_ok());
        }
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        buf.drop_remaining_();
        assert_eq!(drops.load(Ordering::Relaxed), 3);
        assert!(buf.try_pop_().is_none());
        assert!(buf.try_push_(Counted(&drops)).is_ok());
        buf.drop_remaining_();
        assert_eq!(drops.load(Ordering::Relaxed), 4);
    }

    /// 测试目标：验证 const 构造可用于 static，且容量/初始状态正确。
    /// 手段：定义 `static BUF: MpmcBuf<u64, 8> = MpmcBuf::new()`。
    /// 判断：capacity() == 8；初始 pop 为 None。
    /// Test goal: verify const construction works for a static buffer.
    /// Method: define a static MpmcBuf via MpmcBuf::new().
    /// Assertion: capacity() == 8; initial pop is None.
    #[test]
    fn const_static_() {
        static BUF: MpmcBuf<u64, 8> = MpmcBuf::new();
        assert_eq!(BUF.capacity(), 8);
        assert_eq!(BUF.try_pop_(), None);
    }

    /// 测试目标：多生产者/多消费者并发下不丢不重、总数正确。
    /// 手段：4 个生产者各 push 2000 个递增值，4 个消费者在总消费数达到 8000 前持续 pop；
    ///       满/空时 yield 后重试。
    /// 判断：所有线程正常结束；8 个生产者序列号的并集恰好覆盖 0..8000（用计数近似 + 总和校验）。
    /// Test goal: verify no loss/duplication under 4 producers and 4 consumers.
    /// Method: each of 4 producers pushes 2000 sequential values; 4 consumers pop until 8000 total.
    /// Assertion: all threads finish; the sum of received values equals the expected sum.
    #[test]
    fn concurrent_stress_() {
        const PRODUCERS: usize = 4;
        const PER_PRODUCER: usize = 2_000;
        const TOTAL: usize = PRODUCERS * PER_PRODUCER;

        let buf = Arc::new(MpmcBuf::<usize, 64>::new());
        let consumed = Arc::new(AtomicUsize::new(0));
        let sum = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for p in 0..PRODUCERS {
            let buf = Arc::clone(&buf);
            handles.push(std::thread::spawn(move || {
                for n in 0..PER_PRODUCER {
                    let value = p * PER_PRODUCER + n;
                    loop {
                        match buf.try_push_(value) {
                            Ok(()) => break,
                            Err(v) => {
                                assert_eq!(v, value);
                                std::thread::yield_now();
                            }
                        }
                    }
                }
            }));
        }

        for _ in 0..PRODUCERS {
            let buf = Arc::clone(&buf);
            let consumed = Arc::clone(&consumed);
            let sum = Arc::clone(&sum);
            handles.push(std::thread::spawn(move || {
                loop {
                    if consumed.load(Ordering::Acquire) >= TOTAL {
                        break;
                    }
                    match buf.try_pop_() {
                        Some(v) => {
                            sum.fetch_add(v, Ordering::Relaxed);
                            consumed.fetch_add(1, Ordering::Release);
                        }
                        None => std::thread::yield_now(),
                    }
                }
            }));
        }

        for h in handles {
            h.join().expect("stress thread panicked");
        }

        assert_eq!(consumed.load(Ordering::Acquire), TOTAL);
        let expected = (0..TOTAL).sum::<usize>();
        assert_eq!(sum.load(Ordering::Relaxed), expected);
    }
}
