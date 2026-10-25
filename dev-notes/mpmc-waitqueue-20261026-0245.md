# 去除 RwLock：SlabList 自建异步等待队列（20261026-0245）

- 日期：2026-10-26
- 状态：**已实施**
- 结论先行：
  1. 生产/消费两个方向不再各持一把 `CooperativeRwLock`。快路径直接对 Vyukov
     环做 CAS，慢路径进入本方向的**自建异步等待队列**；队列节点由
     `mpmc-chan` 自有的 `SlabList` 批量分配。
  2. 去掉读写锁的关键收益不是“少一次加锁”，而是**取消了写者优先闸门**：旧实现
     中写者一入队，`WRITER_QUEUED` 会挡住同方向所有读快路径，导致失败者全部
     变成新写者、队列自我维持。新实现的快路径永远直接尝试，队列只承载确实无法
     完成的操作。
  3. 代价是同方向 FIFO 从“严格写者优先”退化为**近似公平**：快路径可以插队到
     已 park 的等待者之前，等待者被唤醒后也要重新竞争。这是本设计明确接受的
     取舍，已写入 crate 文档。
  4. 实测（同机、同参数、2026-10-26 3 轮重跑中位数）：1P1C/cap64 async
     290.6 → 270.7 ns/item，try 339.8 → 182.2 ns/item；4P4C/cap64 async
     457.1 → 308.4 ns/item，try 727.9 → 587.2 ns/item；其余配置也全部下降。
     4P4C 的 try 轮间波动较大（475–628 ns），详见 §6。

## 1. 背景：为什么必须去掉写者优先

`mpmc-opt-20261025-1830.md` 的 P0 计数已经给出根因：

- 方案 C 在 4P4C/cap64 下 `slow enter/op ≈ 0.98`，`lock park/op ≈ 0.98`；
- 慢路径的成本主要是**每个 op 一次 `write_async` 等锁 park/wake**，而不是“持写锁
  park”；
- 一旦形成写者队列，`WRITER_QUEUED` 让同方向所有读快路径失败，失败者又成为
  新写者，队列无法排空。

P1 有界自旋把该配置从约 10 µs/item 拉到 0.46 µs/item，但只要仍保留“写者入队即
挡住快路径”的规则，高竞争下就仍有大量操作被迫进入写者队列。因此本轮直接移除
方向锁，把“排队”从读写锁中拿出来，改由队列 + 原子计数承担。

## 2. 已确认的决策

| 决策点 | 裁决 | 直接影响 |
| --- | --- | --- |
| 等待节点承载 | **移植 `bplus_tree_cmp` 的 `SlabList`**，chunk 大小是类型级编译期常量 | 稳态无逐节点分配；固定大小保证对齐/无碎片；节点地址稳定，可直接做侵入式链接 |
| 快路径是否让路 | **不让路**：始终先做无锁 CAS | 消除写者队列自维持；同方向 FIFO 变为近似公平 |
| 公开 API | **移除** `ProdGate`/`ConsGate` GAT、`new_with_locks`、`x_deps::atomic_sync`、`hybrid` feature | 配置只保留 allocator、两个 chunk 大小、`FAST_SPIN` |
| 配置形态 | `TrConfig` 用关联类型 `ProdWaitSlab`/`ConsWaitSlab` 指定 `SlabListConfig<CHUNK_SIZE>`；提供 4K/16K/64K 预设别名 | chunk 大小在编译期固定，槽位数受 `MAX_SLOTS_PER_CHUNK` 约束，不会因运行时任意大小产生碎片或 `u16` 溢出 |
| `try_*` 语义 | 直接尝试环操作；只有满/空/关闭会失败 | 不再因“方向锁被慢写者占用”返回 would-block |
| 取消语义 | `WaitGuard` 以 `Drop` 归还节点 | future 被丢弃也保证节点/waker 不泄漏 |

## 3. 结构

```text
MpmcChan<'a, T, N, B, C>
├── buf_       : B (BorrowMut<MpmcBuf<T, N>>)      // Vyukov 序列环
├── prod_wait_ : WaitQueue<C::ChanAlloc>            // 生产方向 FIFO
├── cons_wait_ : WaitQueue<C::ChanAlloc>            // 消费方向 FIFO
├── senders_ / receivers_ : AtomicUsize
└── tx_closed_ / rx_closed_ : AtomicBool

WaitQueue<A>
├── state_   : SpinLock<QueueState<A>>
│   ├── head_ / tail_ : Option<SlotPtr<WaitNode>>
│   ├── len_ : usize
│   └── slab_ : SlabList<WaitNode, A>
└── waiting_ : AtomicUsize        // 快速提示：0 时 notify 不取锁

WaitNode
├── waker_ : Option<Waker>
├── state_ : Waiting | Notified | Done
├── prev_ / next_ : Option<SlotPtr<WaitNode>>
```

`SlabList` 从 `bplus_tree_cmp` 移植，保留其**类型级 chunk 配置**：

- 单 chunk 大小来自 `TrSlabConfig::CHUNK_SIZE`，是编译期常量；`ChunkLayout`
  在需要时按该常量与 `T` 重算，不在运行期接受任意字节数；
- arena 自持 allocator 实例，`insert_` / `remove_` / `clear_` 不再额外借入
  allocator；`Drop` 自动清理 chunk。

固定 chunk 大小是有意为之：它保证每次分配使用同一布局与对齐，长期运行不会
因为大小混杂产生碎片；槽位数在构造时对照 `MAX_SLOTS_PER_CHUNK`（2^15）校验，
因此 `self_idx` 的 `u16` 索引与 occupied 位不会溢出。

## 4. 并发路径审计

| 路径 | 锁/原子状态 | 唤醒责任 | 存活论证 |
| --- | --- | --- | --- |
| 快路径 `try_*` / 首次 async 尝试 | 直接 `tail`/`head` CAS；无队列锁 | 成功后 `notify_cons_` / `notify_prod_` | 环 CAS 本身保证槽位独占；不持有任何队列节点 |
| 有界自旋 | 同上；每 16 次检查取消/关闭 | 成功后同样通知对向 | 无堆分配、无锁；取消仅返回错误 |
| 慢路径入队 | 队列自旋锁内 `SlabList::insert_` + 链接；`waiting_` 加一 | 由对向成功操作唤醒队首 | `WaitGuard` 持有节点；节点地址稳定 |
| 慢路径复检 | 入队后立即再次尝试环操作 | 成功则 `finish_` 归还节点并通知对向 | 入队与复检之间即使通知被“错过”，节点状态 `Notified` 会让下一次 poll 返回 Ready |
| 唤醒 | 队首摘除、状态 `Notified`、取走 waker、`waiting_` 减一；**锁外** `wake` | 唤醒方只摘除不释放槽位 | 节点由等待者释放，唤醒方不再访问节点 |
| 取消 / future 丢弃 | `WaitGuard::drop` → `release_`：若仍 `Waiting` 则摘链，然后 `SlabList::remove_` | 无 | 节点可能残留 waker，锁外析构 |
| 关闭 | `tx_closed_`/`rx_closed_` 置位后 `notify_all_` 两个队列 | 关闭方 | 等待者被唤醒后检查关闭并返回；`WaitGuard` 释放节点 |
| 析构 | 所有句柄/future 消失后 `MpmcChan::drop` drain buffer；`WaitQueue` 随内核析构 | 无 | 此时不可能有等待者；`SlabList::drop` 兜底析构残留节点 |

### 4.1 丢失唤醒论证

经典的条件等待协议是“先入队、再复检、最后 park”：

1. 环操作失败；
2. 入队（挂到队尾、登记 waker）；
3. 再次尝试环操作；成功则归还节点并完成；
4. 仍失败则调用 `poll_wait_`：若节点状态已是 `Notified` 直接 Ready，否则登记
   waker 返回 Pending。

因此，即使对向的通知发生在“步骤 1 与步骤 2 之间”（队列为空，通知看不到本节点），
步骤 3 的复检也会发现条件已经满足；若条件仍未满足，说明通知对应的状态变化已被
别的快路径消耗，继续等待是正确的。

### 4.2 为什么 `waiting_` 只是提示

`notify_one_` 先读 `waiting_`，为 0 就直接返回、不取锁。这个读可能错过刚入队的
节点，但由于 4.1 的复检，错过一次通知不会导致永久 park；下一次环状态变化会再次
通知。反过来，`waiting_` 大于 0 时仍以锁内 `head_` 为准，空队列直接返回。

## 5. 测试

- `chan::wait_queue_::tests_`：FIFO 唤醒、取消归还、`notify_all`、通知先于 poll、
  空队列通知；
- `slab_list_::list_::tests_`：chunk 过小/槽数超限、插入/解引用/乱序归还、LIFO
  复用、chunk 扩容、Drop 只析构存活元素、ZST、重复归还 panic、外部 allocator；
- `chan::tests_`：新增“多生产者同时等待全部完成”与“关闭唤醒所有等待者”，
  保留原有并发压力、取消、drain、克隆关闭等用例；
- 共 40 个单元测试 + 15 个文档测试通过；`cargo clippy --all-targets` 无告警。

运行：

```sh
cargo test --offline
cargo clippy --offline --all-targets
```

## 6. 基准

命令（同机 4 核，`taskset -c 0-3`）：

```sh
BENCH_OPS=20000 BENCH_ROUNDS=3 BENCH_WARMUP=1 \
BENCH_PRODUCERS=<P> BENCH_CONSUMERS=<C> BENCH_CAP=<N> \
cargo bench --offline --bench mpmc_compare
```

原始 CSV：`mpmc-chan/.tmp/bench-rerun-20261026/`（本轮 3 轮重跑，匹配
`mpmc-bench-20261025-1803.md` 的命令）；5 轮批次在
`.tmp/waitq-bench-20261026/`；旧实现数据取自 `.tmp/p1-final-20261025-235154/`。
下表是 3 轮中位数（ns/item）。

| 配置 | 旧 P1 async | 新实现 async | 旧 P1 try | 新实现 try |
| --- | ---: | ---: | ---: | ---: |
| 1P1C cap64 | 290.6 | 270.7 | 339.8 | 182.2 |
| 2P2C cap64 | 325.2 | 262.5 | 378.5 | 198.7 |
| 2P2C cap1024 | 317.5 | 267.7 | 380.7 | 275.2 |
| 4P4C cap64 | 457.1 | 308.4 | 727.9 | 587.2 |

观察：

- 低竞争（1P1C）提升最大：快路径不再有方向读锁 RMW，`try` 路径只剩一次 CAS；
- 高竞争（4P4C）提升仍然明显，且不再出现旧实现“写者队列自维持”导致的双稳态；
- 新实现 4P4C/try 的轮间值为 475.3 / 587.2 / 627.8 ns，p95 为 627.8 ns；
  3 轮样本下中位数仍偏高，说明该配置对调度噪声敏感，需要更严格的结论时应
  增加轮数并固定 CPU 频率。

## 7. 语义变化与风险

1. **同方向 FIFO 是近似公平**：快路径不检查等待者，理论上可能让已 park 的等待者
   长时间拿不到槽位。实际由“每次成功操作都唤醒队首 + 等待者被唤醒后立即重试”
   保证有进展，但不承诺严格 FIFO。若未来需要严格公平，应另设“等待者优先窗口”
   或票据交接，而不是重新引入写者闸门。
2. **`try_*` 不再返回因锁忙导致的 would-block**：现在只有满/空/关闭会失败。对
   调用方而言这是更简单的语义，但依赖旧行为的代码需要调整。
3. **配置错误在构造时 panic**：chunk 字节数无法容纳至少一个等待节点时，
   `MpmcChan::new_in` 会 panic。默认配置不受影响。
4. **SlabList 是 crate 私有**：没有作为公开容器导出；其 `SlotPtr` 句柄的有效期
   约定只在 `wait_queue_` 内部使用。

## 8. 后续

- 如需进一步降低高竞争下的唤醒开销，可评估“每条成功操作只唤醒队首 + 队首自旋
  一次”的混合策略；
- 可增加一个只读诊断接口，暴露两个等待队列的长度（需要讨论是否属于公开 API）；
- 在更多平台（弱内存序、单核 executor）上补测丢失唤醒与取消风暴。
