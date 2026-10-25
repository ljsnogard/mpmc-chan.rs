# mpmc-chan

An array-based, async-runtime-agnostic, `no_std` MPMC channel with back pressure.

## Design

- **Fast path (no locks)**: `try_send`/`try_recv` and the first attempt of
  `send_async`/`recv_async` go straight to a Vyukov bounded sequence ring with a
  CAS. No read lock, no writer-queued hint, and no per-operation lock RMW.
- **Bounded spin**: after the first async fast-path failure, `send_async`/
  `recv_async` retry the lock-free fast path for `TrConfig::FAST_SPIN` iterations
  (default 512) before entering the wait queue. Cancellation/close are checked
  every 16 iterations so a cancelled future stays observable.
- **Per-direction async wait queue**: each direction owns an independent FIFO
  queue of wait nodes. A node stores the `Waker`, a state, and intrusive links.
  Nodes are allocated from a **SlabList** (chunked slab): after the first chunk,
  park/wake/cancel do no per-node heap allocation. `notify_one` pops the queue
  head and calls `wake` outside the queue spin lock; `notify_all` is used on close.
- **No rwlock**: the slow path no longer queues as a writer and never parks while
  holding a direction lock. A producer waiter is woken only when a consumer frees
  a slot; a consumer waiter is woken only when a producer publishes an item. A
  backlog in one direction therefore cannot block the other.
- **Fairness is approximate**: the fast path never yields to queued waiters, so a
  successful CAS can overtake a parked one. Same-direction waiters are woken in
  FIFO order, but strict writer-priority/barging-free semantics are not promised.
  This is intentional: the old write-lock queue became self-sustaining under
  contention (queued writers blocked every fast path, so almost every operation
  fell back to the slow path). Removing that gate is the main source of the
  performance gain; see `dev-notes/mpmc-waitqueue-20261026-0245.md`.
- **Cancel safety**: `WaitGuard` owns the queue node and releases it from `Drop`,
  so dropping a cancelled future cannot leak a node or a waker.
- The buffer is passed through `BorrowMut<MpmcBuf>`: the channel can own it
  (`MpmcBuf`, `Box<MpmcBuf>`) or borrow it as `&mut MpmcBuf`. Ownership/the `&mut`
  borrow guarantees that one buffer serves only one live channel; there is no
  runtime claim flag.

## Usage

```rust
use mpmc_chan::{MpmcBuf, MpmcChan};

let mut buf = MpmcBuf::<u32, 8>::new();
let (tx, rx) = MpmcChan::new(&mut buf);
tx.try_send(1).unwrap();
assert_eq!(rx.try_recv(), Ok(1));

// Async: `.await` or `.may_cancel_with(token)`.
// let value = rx.recv_async().await?;

// Custom tuning: `TrConfig` selects the outer Arc/SlabList allocator, the
// per-direction type-level SlabList chunk configuration and the spin budget.
// Use `new_in` with a custom config type and allocator instance.
// use mpmc_chan::{DefaultConfig, TrConfig, WaitSlab4K};
// use std::alloc::Global;
// struct SmallConfig;
// impl TrConfig for SmallConfig {
//     type ChanAlloc = <DefaultConfig as TrConfig>::ChanAlloc;
//     type ProdWaitSlab = WaitSlab4K<Global>;
//     type ConsWaitSlab = WaitSlab4K<Global>;
//     const FAST_SPIN: usize = 32;
// }
// let (tx, rx) = MpmcChan::<u32, 8, _, SmallConfig>::new_in(&mut buf, Global);

// A `'static` buffer needs a mutable place; an immutable
// `static BUF: MpmcBuf<...>` cannot produce `&mut`. For example:
// let buf: &'static mut MpmcBuf<u32, 8> = Box::leak(Box::new(MpmcBuf::new()));
// let (tx, rx) = MpmcChan::new(buf);
```

`N` must be a power of two and `N >= 2` (Vyukov's sequence ring cannot represent
capacity 1 correctly). A borrowed buffer must outlive all senders/receivers/futures.

## Status

- The lock-free fast path plus per-direction SlabList-backed wait queues is the
  only implementation; the old `CooperativeRwLock`/`hybrid` variants and their
  configuration GATs were removed.
- Tests: 40 unit tests + 15 doc tests, including multi-waiter FIFO,
  close-wakes-all, cancellation/reclaim and cross-chunk SlabList cases.
- Comparison on the same machine (`BENCH_OPS=20000`, 5 rounds, medians, ns/item):

  | scenario | previous P1 (rwlock) | SlabList wait queue |
  | --- | ---: | ---: |
  | 1P1C cap64 async | 290.6 | 134.7 |
  | 1P1C cap64 try | 339.8 | 82.4 |
  | 2P2C cap64 async | 325.2 | 250.6 |
  | 2P2C cap64 try | 378.5 | 235.6 |
  | 2P2C cap1024 async | 317.5 | 253.2 |
  | 2P2C cap1024 try | 380.7 | 222.3 |
  | 4P4C cap64 async | 457.1 | 313.4 |
  | 4P4C cap64 try | 727.9 | 396.7 |

  Raw CSVs and the causal discussion are in
  `dev-notes/mpmc-waitqueue-20261026-0245.md`.

```sh
cargo test
cargo clippy --all-targets
```

## Benchmarks

```sh
# default build
taskset -c 0-3 cargo bench --offline --bench mpmc_compare
```

Tune with `BENCH_OPS` (per producer, default 20000), `BENCH_ROUNDS` (5),
`BENCH_PRODUCERS` / `BENCH_CONSUMERS` (4/4), `BENCH_CAP` (64; mpmc-chan accepts
8/64/1024) and `BENCH_WARMUP` (1). `BENCH_IMPLS=mpmc|ext|all` filters the
implementations. The harness prints per-round CSV and a median summary; it is not
an apples-to-apples microbenchmark because async channels are driven by
`futures_lite::block_on`, crossbeam-channel uses OS blocking, and crossbeam-queue
uses spin.

## Platform

- `#![no_std]`, but uses `alloc`: the channel's `Arc` core and the SlabList chunks
  for wait nodes are heap allocated. The ring buffer itself does not allocate.
- Nightly toolchain (`gen_mcf2` requires nightly features).
- Dependencies point at the Gitee default branches. Before release, pin `rev`
  (or commit `Cargo.lock`); the local dev machine may need cached git checkouts
  when building offline.
