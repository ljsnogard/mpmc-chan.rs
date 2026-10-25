//! 通道层测试：构造/复用、关闭/drain、异步满/空唤醒、取消、克隆计数。

use alloc::{boxed::Box, vec::Vec};

use abs_cancel::{CancelledToken, NonCancellableToken, TrMayCancel};
use futures_lite::future::block_on;

use crate::{MpmcBuf, MpmcChan, RecvError, SendError, TryRecvError, TrySendError};

/// 泄漏一个 buffer 以获得 `'static` 借用，便于把句柄 move 进测试线程。
fn leak_buf_<T, const N: usize>() -> &'static mut MpmcBuf<T, N> {
    Box::leak(Box::new(MpmcBuf::new()))
}

/// 测试目标：验证构造、try_send/try_recv、容量与近似 len。
/// 手段：容量 4 的 buffer 构造通道，发送 1/2，检查 len，再依次接收。
/// 判断：初始 len=0；发送后 len=2；接收顺序为 1,2；capacity=4。
/// Test goal: verify construction, try_send/try_recv, capacity and approximate len.
/// Method: build a capacity-4 channel, send 1/2, inspect len, then receive.
/// Assertion: initial len=0; after sends len=2; receive order 1,2; capacity=4.
#[test]
fn build_and_try_() {
    let buf = leak_buf_::<u32, 4>();
    let (tx, rx) = MpmcChan::new(buf);

    assert_eq!(tx.capacity(), 4);
    assert_eq!(rx.capacity(), 4);
    assert_eq!(tx.len(), 0);

    assert_eq!(tx.try_send(1), Ok(()));
    assert_eq!(tx.try_send(2), Ok(()));
    assert_eq!(tx.len(), 2);

    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

/// 测试目标：验证 `MpmcChan::new` 接受 owned `MpmcBuf`（`BorrowMut` 的拥有型载体）。
/// 手段：把 `MpmcBuf` 按值交给 `MpmcChan::new`，发送并接收一个元素。
/// 判断：发送/接收成功；channel 拥有 buffer，无需运行时 claim。
/// Test goal: verify `MpmcChan::new` accepts an owned `MpmcBuf` (BorrowMut owned carrier).
/// Method: move a `MpmcBuf` into `MpmcChan::new`, then send/recv one value.
/// Assertion: send/recv succeed; the channel owns the buffer without a runtime claim.
#[test]
fn owned_buffer_() {
    let buf = MpmcBuf::<u8, 2>::new();
    let (tx, rx) = MpmcChan::new(buf);
    tx.try_send(1).unwrap();
    assert_eq!(rx.try_recv(), Ok(1));
}

/// 测试目标：验证 `BorrowMut<MpmcBuf>` 接受 `Box<MpmcBuf>` 载体。
/// 手段：把 `Box<MpmcBuf>` 按值交给 `MpmcChan::new`，发送并接收。
/// 判断：send/recv 成功；channel 通过 Box 拥有 buffer。
/// Test goal: verify `BorrowMut<MpmcBuf>` accepts `Box<MpmcBuf>`.
/// Method: move `Box<MpmcBuf>` into `MpmcChan::new`, then send/recv.
/// Assertion: send/recv succeed; the channel owns the buffer through the Box.
#[test]
fn boxed_buffer_() {
    let buf = Box::new(MpmcBuf::<u8, 2>::new());
    let (tx, rx) = MpmcChan::new(buf);
    tx.try_send(3).unwrap();
    assert_eq!(rx.try_recv(), Ok(3));
}

/// 测试目标：验证 `BorrowMut<MpmcBuf>` 也能接受 `static mut` 放置的 buffer。
/// 手段：`static mut BUF` 用 `addr_of_mut!` 取得 `&'static mut`，再交给 `MpmcChan::new`。
/// 判断：静态 buffer 上的 send/recv 正常。不可变 `static BUF` 不在此测试范围。
/// Test goal: verify `BorrowMut<MpmcBuf>` accepts a `static mut` buffer.
/// Method: obtain `&'static mut` from `static mut BUF` via `addr_of_mut!`, pass to new.
/// Assertion: send/recv on the static buffer succeed. Immutable `static BUF` is not covered.
#[test]
fn static_buffer_() {
    static mut BUF: MpmcBuf<u32, 2> = MpmcBuf::new();

    // SAFETY: 本测试独占这个 static；没有其他线程访问 BUF。
    let buf = unsafe { &mut *core::ptr::addr_of_mut!(BUF) };
    let (tx, rx) = MpmcChan::new(buf);
    tx.try_send(11).unwrap();
    assert_eq!(rx.try_recv(), Ok(11));
}

/// 测试目标：最后一个 Sender 退出后，Receiver 先 drain 再收到 Closed。
/// 手段：发送 1/2，drop Sender，再连续 try_recv。
/// 判断：先得到 1,2，随后得到 Closed；发送新元素得到 Closed。
/// Test goal: after the last Sender drops, Receiver drains then observes Closed.
/// Method: send 1/2, drop Sender, call try_recv repeatedly.
/// Assertion: receive 1,2 then Closed; a new try_send returns Closed.
#[test]
fn sender_drop_drains_then_closed_() {
    let buf = leak_buf_::<u32, 4>();
    let (tx, rx) = MpmcChan::new(buf);
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    drop(tx);

    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
}

/// 测试目标：Receiver 显式 close 后停止新发送，但仍可 drain 已入队元素。
/// 手段：发送 1/2，调用 rx.close()，再 try_recv；随后调用 tx.try_send。
/// 判断：仍能收到 1,2；之后 Closed；tx.try_send 返回 Closed。
/// Test goal: Receiver::close stops new sends but still allows draining queued items.
/// Method: send 1/2, call rx.close(), drain, then try_send again.
/// Assertion: drain yields 1,2 then Closed; try_send returns Closed.
#[test]
fn receiver_close_drains_() {
    let buf = leak_buf_::<u32, 4>();
    let (tx, rx) = MpmcChan::new(buf);
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    rx.close();

    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
    assert_eq!(tx.try_send(3), Err(TrySendError::Closed(3)));
}

/// 测试目标：验证最后一个 Receiver 退出后，Sender 立刻观察到 Closed。
/// 手段：发送一个元素后 drop Receiver，再 try_send。
/// 判断：try_send 返回 Closed，元素原样返回。
/// Test goal: verify senders observe Closed immediately after the last Receiver drops.
/// Method: send one item, drop Receiver, then try_send.
/// Assertion: try_send returns Closed and returns the value.
#[test]
fn receiver_drop_closes_sender_() {
    let buf = leak_buf_::<u32, 2>();
    let (tx, rx) = MpmcChan::new(buf);
    tx.try_send(1).unwrap();
    drop(rx);

    assert!(tx.is_closed());
    assert_eq!(tx.try_send(2), Err(TrySendError::Closed(2)));
}

/// 测试目标：满时异步 send 会 park，并在消费者腾出空间后被唤醒。
/// 手段：容量 2 填满；生产者线程 block_on(send_async(3))；主线程 pop 1 后再收 2/3。
/// 判断：生产者 join 成功，接收顺序为 1,2,3；若 park/唤醒丢失，测试会挂起并被外层 timeout 终止。
/// Test goal: a full channel parks an async send and wakes it after a pop.
/// Method: fill capacity 2; producer blocks on send_async(3); main pops 1 then receives 2/3.
/// Assertion: producer joins; receive order 1,2,3; a lost wakeup would hang until timeout.
#[test]
fn async_full_wakes_() {
    let buf = leak_buf_::<u32, 2>();
    let (tx, rx) = MpmcChan::new(buf);
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();

    let producer =
        std::thread::spawn(move || block_on(async { tx.send_async(3).await }).expect("send_async"));

    std::thread::sleep(std::time::Duration::from_millis(20));
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(block_on(async { rx.recv_async().await }), Ok(2));
    assert_eq!(block_on(async { rx.recv_async().await }), Ok(3));
    producer.join().expect("producer 线程不应 panic");
}

/// 测试目标：空时异步 recv 会 park，并在生产者入队后被唤醒。
/// 手段：消费者线程 block_on(recv_async())；主线程 sleep 后 try_send(7)。
/// 判断：消费者 join 得到 7；若唤醒丢失，测试会挂起并被外层 timeout 终止。
/// Test goal: an empty channel parks an async recv and wakes it after a push.
/// Method: consumer blocks on recv_async(); main sleeps then try_send(7).
/// Assertion: consumer joins with 7; a lost wakeup would hang until timeout.
#[test]
fn async_empty_wakes_() {
    let buf = leak_buf_::<u32, 2>();
    let (tx, rx) = MpmcChan::new(buf);

    let consumer =
        std::thread::spawn(move || block_on(async { rx.recv_async().await }).expect("recv_async"));
    std::thread::sleep(std::time::Duration::from_millis(20));
    tx.try_send(7).unwrap();

    assert_eq!(consumer.join().expect("consumer 线程不应 panic"), 7);
}

/// 测试目标：已经取消的令牌必须在接触方向锁之前返回 Cancelled(item)。
/// 手段：填满容量 2，用 CancelledToken 调用 send_async(3).may_cancel_with(...)。
/// 判断：返回 SendError::Cancelled(3)；随后仍可正常 drain 1/2，说明没有占用/破坏环。
/// Test goal: an already-cancelled token returns Cancelled(item) before touching locks.
/// Method: fill capacity 2, call send_async(3) with CancelledToken.
/// Assertion: returns SendError::Cancelled(3); queued 1/2 remain drainable.
#[test]
fn cancel_immediate_() {
    let buf = leak_buf_::<u32, 2>();
    let (tx, rx) = MpmcChan::new(buf);
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();

    let result = block_on(tx.send_async(3).may_cancel_with(CancelledToken::new()));
    assert_eq!(result, Err(SendError::Cancelled(3)));

    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

/// 测试目标：不可取消令牌路径可以正常完成异步发送/接收。
/// 手段：容量 2，用 NonCancellableToken 发送 5 再接收。
/// 判断：send 返回 Ok；recv 返回 Ok(5)。
/// Test goal: the non-cancellable token path completes async send/recv normally.
/// Method: use NonCancellableToken to send 5 then receive.
/// Assertion: send is Ok; recv is Ok(5).
#[test]
fn non_cancellable_roundtrip_() {
    let buf = leak_buf_::<u32, 2>();
    let (tx, rx) = MpmcChan::new(buf);

    let sent = block_on(tx.send_async(5).may_cancel_with(NonCancellableToken::new()));
    assert_eq!(sent, Ok(()));
    let received = block_on(rx.recv_async().may_cancel_with(NonCancellableToken::new()));
    assert_eq!(received, Ok(5));
}

/// 测试目标：Sender/Receiver 克隆共享同一个通道，最后一个句柄退出才关闭。
/// 手段：克隆 tx；drop 原 tx 后由 tx2 发送；再 drop tx2 后接收端 drain 并 Closed。
/// 判断：tx2 发送成功；rx 收到值；drop tx2 后 rx 得到 Closed。
/// Test goal: cloned handles share a channel and only the last drop closes it.
/// Method: clone tx, drop original, send via clone, then drop clone and drain.
/// Assertion: clone send succeeds; rx receives; after clone drop rx observes Closed.
#[test]
fn clone_last_close_() {
    let buf = leak_buf_::<u32, 2>();
    let (tx, rx) = MpmcChan::new(buf);
    let tx2 = tx.clone();
    drop(tx);

    assert_eq!(tx2.try_send(9), Ok(()));
    assert_eq!(rx.try_recv(), Ok(9));
    drop(tx2);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
}

/// 测试目标：4 生产者/4 消费者通过异步 send/recv 并发跑完整条通道，不丢不重。
/// 手段：容量 64；每个生产者发送 500 个递增标签；消费者 block_on(recv_async) 直到 Closed；
///       主线程先 drop 自己的句柄，让最后一个生产者退出后触发关闭。
/// 判断：所有线程 join 成功；消费总数 = 2000；消费值总和 = 0..2000 的总和。
/// Test goal: 4 producers/4 consumers exercise async send/recv concurrently without loss.
/// Method: capacity 64; each producer sends 500 tagged values; consumers block_on(recv_async)
///         until Closed; main drops its own handles so the last producer closes the channel.
/// Assertion: all threads join; total received = 2000; sum matches 0..2000.
#[test]
fn async_channel_stress_() {
    const PRODUCERS: usize = 4;
    const CONSUMERS: usize = 4;
    const PER_PRODUCER: usize = 500;
    const TOTAL: usize = PRODUCERS * PER_PRODUCER;

    let buf = leak_buf_::<usize, 64>();
    let (tx, rx) = MpmcChan::new(buf);

    let mut handles = Vec::new();
    for p in 0..PRODUCERS {
        let tx = tx.clone();
        handles.push(std::thread::spawn(move || {
            for n in 0..PER_PRODUCER {
                let value = p * PER_PRODUCER + n;
                block_on(async { tx.send_async(value).await }).expect("send_async");
            }
        }));
    }

    let mut consumers = Vec::new();
    for _ in 0..CONSUMERS {
        let rx = rx.clone();
        consumers.push(std::thread::spawn(move || {
            let mut count = 0usize;
            let mut sum = 0usize;
            loop {
                match block_on(async { rx.recv_async().await }) {
                    Ok(value) => {
                        count += 1;
                        sum += value;
                    }
                    Err(RecvError::Closed) => break,
                    Err(RecvError::Cancelled) => panic!("不可取消路径不应返回 Cancelled"),
                }
            }
            (count, sum)
        }));
    }

    // 主线程不再持有句柄：最后一个生产者退出后，通道自然关闭。
    drop(tx);
    drop(rx);

    for h in handles {
        h.join().expect("producer 线程不应 panic");
    }
    let mut total_count = 0usize;
    let mut total_sum = 0usize;
    for h in consumers {
        let (count, sum) = h.join().expect("consumer 线程不应 panic");
        total_count += count;
        total_sum += sum;
    }

    assert_eq!(total_count, TOTAL);
    assert_eq!(total_sum, (0..TOTAL).sum::<usize>());
}

/// 测试目标：确认公开句柄/内核满足 Send+Sync，异步 Future 满足 Send。
/// 手段：编译期调用 `assert_send_sync::<...>()` 与 `assert_send::<...>()`。
/// 判断：若任一类型不满足，编译失败。
/// Test goal: compile-time check handles/core are Send+Sync and async futures are Send.
/// Method: instantiate assert_send_sync / assert_send for the public types.
/// Assertion: compilation fails if any bound is missing.
#[test]
fn handles_are_send_sync_() {
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_send<T: Send>() {}
    assert_send_sync::<crate::MpmcChan<'static, u32, 4, MpmcBuf<u32, 4>>>();
    assert_send_sync::<crate::Sender<'static, u32, 4, MpmcBuf<u32, 4>>>();
    assert_send_sync::<crate::Receiver<'static, u32, 4, MpmcBuf<u32, 4>>>();
    assert_send::<
        crate::SendFuture<
            'static,
            'static,
            'static,
            u32,
            MpmcBuf<u32, 4>,
            crate::DefaultConfig,
            4,
            abs_cancel::NonCancellableToken,
        >,
    >();
    assert_send::<
        crate::RecvFuture<
            'static,
            'static,
            'static,
            u32,
            MpmcBuf<u32, 4>,
            crate::DefaultConfig,
            4,
            abs_cancel::NonCancellableToken,
        >,
    >();
}

/// 测试目标：验证 `RecvError` 在关闭后的异步接收路径上正确返回。
/// 手段：空通道 drop Sender 后 block_on(recv_async())。
/// 判断：返回 RecvError::Closed。
/// Test goal: async recv on a closed empty channel returns RecvError::Closed.
/// Method: drop Sender on an empty channel, then block_on(recv_async()).
/// Assertion: returns RecvError::Closed.
#[test]
fn async_recv_closed_() {
    let buf = leak_buf_::<u32, 2>();
    let (tx, rx) = MpmcChan::new(buf);
    drop(tx);
    assert_eq!(
        block_on(async { rx.recv_async().await }),
        Err(RecvError::Closed)
    );
}

/// 测试目标：验证自定义 `TrConfig` 的 allocator 与自旋预算可被通道采用。
/// 手段：定义 `Spin32Config`（复用默认 allocator，`FAST_SPIN = 32`），用
///       `new_in` 构造容量 4 的通道，跑一次 try 往返与两次 async 往返。
/// 判断：`FAST_SPIN == 32`；try/async 发送接收值正确；关闭后 recv 返回 `Closed`。
/// Test goal: verify a custom TrConfig supplies the allocator and spin budget.
/// Method: define Spin32Config (default allocator, FAST_SPIN = 32), build a
///         capacity-4 channel via new_in, run try and async roundtrips.
/// Assertion: FAST_SPIN == 32; values match; closed recv returns Closed.
#[test]
fn custom_config_() {
    use crate::{DefaultConfig, TrConfig};

    struct Spin32Config;
    impl TrConfig for Spin32Config {
        type ChanAlloc = <DefaultConfig as TrConfig>::ChanAlloc;
        type ProdAlloc = <DefaultConfig as TrConfig>::ProdAlloc;
        type ConsAlloc = <DefaultConfig as TrConfig>::ConsAlloc;
        const FAST_SPIN: usize = 32;
    }

    assert_eq!(Spin32Config::FAST_SPIN, 32);

    let mut buf = MpmcBuf::<u32, 4>::new();
    let chan = <<Spin32Config as TrConfig>::ChanAlloc as Default>::default();
    let prod = <<Spin32Config as TrConfig>::ProdAlloc as Default>::default();
    let cons = <<Spin32Config as TrConfig>::ConsAlloc as Default>::default();
    let (tx, rx) = MpmcChan::<u32, 4, _, Spin32Config>::new_in(&mut buf, chan, prod, cons);

    assert_eq!(tx.try_send(7), Ok(()));
    assert_eq!(rx.try_recv(), Ok(7));
    assert!(block_on(async { tx.send_async(9).await }).is_ok());
    assert!(matches!(
        block_on(async { rx.recv_async().await }),
        Ok(9)
    ));

    drop(tx);
    assert!(matches!(
        block_on(async { rx.recv_async().await }),
        Err(RecvError::Closed)
    ));
}
