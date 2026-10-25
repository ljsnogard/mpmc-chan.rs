# mpmc-chan

An array-based, async-runtime-agnostic, `no_std` MPMC channel with back pressure.

## Design

- **Fast path**: each direction first takes a *read* lock on its own
  `CooperativeRwLock`, then competes for a slot in a Vyukov bounded sequence ring.
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

- Plan C (read lock every fast-path operation + dual direction locks) is
  implemented and tested.
- Plan D (congestion hint + lock only on the slow path) and the benchmark matrix
  are the next step. See `dev-notes/mpmc-design-20261025-1535.md`.

## Platform

- `#![no_std]`, but uses `alloc`: `atomic_sync`'s `CooperativeRwLock` allocates an
  `Arc` core and arena chunks for its wait queues.
- Nightly toolchain (`gen_mcf2` / `atomic_sync` require nightly features).
- Development currently uses local `path` dependencies plus a `[patch]` for
  `abs_cancel`; switch to pinned `rev` git dependencies before release.
