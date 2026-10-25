# mpmc-chan

An array-based, async-runtime-agnostic, `no_std` MPMC channel with back pressure.

## Design

- **Fast path (default, plan C)**: each direction first takes a *read* lock on its
  own `CooperativeRwLock`, then competes for a slot in a Vyukov bounded sequence
  ring. With the `hybrid` feature (plan D), `send_async`/`recv_async` skip the read
  lock and use a per-direction congestion hint; `try_send`/`try_recv` keep the
  read-lock path.
- **Slow path**: if the ring is full/empty (or the direction lock is busy), the
  operation releases the read lock, queues as a **writer** on the same direction
  lock, and — once admitted — parks while **holding** that direction write lock.
  Producers and consumers use independent locks, so a parked producer never blocks
  a consumer (and vice versa).
- Only one waiter per direction can be parked, so each direction needs just one
  single-waiter notification slot. Same-direction waiters are FIFO.
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

// A `'static` buffer needs a mutable place; an immutable
// `static BUF: MpmcBuf<...>` cannot produce `&mut`. For example:
// let buf: &'static mut MpmcBuf<u32, 8> = Box::leak(Box::new(MpmcBuf::new()));
// let (tx, rx) = MpmcChan::new(buf);
```

`N` must be a power of two and `N >= 2` (Vyukov's sequence ring cannot represent
capacity 1 correctly). A borrowed buffer must outlive all senders/receivers/futures.

## Status

- Plan C (read lock every fast-path operation + dual direction locks) is the
  default and is implemented/tested.
- Plan D (congestion hint for `send_async`/`recv_async`; `try_*` remain locked)
  is implemented behind `--features hybrid` and passes the same tests.
- A comparison harness (`benches/mpmc_compare.rs`) covers C, D, flume,
  crossbeam-channel and crossbeam-queue for async and try/spin workloads; see
  the provisional results and caveats in
  `dev-notes/mpmc-design-20261025-1535.md` §8.5.
- The first results show the async slow path, not the ring CAS, dominates under
  sustained full/empty; D is not a clear win, so C remains the default.

```sh
cargo test
cargo test --features hybrid
```

## Benchmarks

```sh
# C (default)
taskset -c 0-3 cargo bench --offline --bench mpmc_compare

# D (hybrid feature)
taskset -c 0-3 cargo bench --offline --features hybrid --bench mpmc_compare
```

Tune with `BENCH_OPS` (per producer, default 20000), `BENCH_ROUNDS` (5),
`BENCH_PRODUCERS` / `BENCH_CONSUMERS` (4/4), `BENCH_CAP` (64; mpmc-chan accepts
8/64/1024) and `BENCH_WARMUP` (1). The harness prints per-round CSV and a
median summary; it is not an apples-to-apples microbenchmark because async
channels are driven by `futures_lite::block_on`, crossbeam-channel uses OS
blocking, and crossbeam-queue uses spin.

## Platform

- `#![no_std]`, but uses `alloc`: `atomic_sync`'s `CooperativeRwLock` allocates an
  `Arc` core and arena chunks for its wait queues.
- Nightly toolchain (`gen_mcf2` / `atomic_sync` require nightly features).
- Dependencies point at the Gitee default branches. Before release, pin `rev`
  (or commit `Cargo.lock`); the local dev machine may need cached git checkouts
  when building offline.
