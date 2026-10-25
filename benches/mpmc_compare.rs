//! MPMC 通道性能对比：mpmc-chan C / D、flume、crossbeam-channel、crossbeam-queue。
//!
//! # 场景
//!
//! - `async`：生产者/消费者线程各自 `block_on` 一次异步 send/recv；mpmc 的 C/D 与
//!   flume async 可比。crossbeam-channel 是阻塞 API，单独标成 `blocking`；
//!   crossbeam-queue 是无等待的 spin 基线，标成 `spin`。
//! - `try`：所有实现用非阻塞 try_send/try_recv（或 ArrayQueue push/pop）在 spin
//!   循环里跑。mpmc 的 D 在 `try_*` 上故意仍走读锁，因此这一组预计 C≈D。
//!
//! # 输出
//!
//! 逐行 CSV：
//!
//! ```text
//! RES,<impl>,<scenario>,<round>,<items>,<secs>,<items_per_sec>,<ns_per_item>
//! SUM,<impl>,<scenario>,<median_ns_per_item>,<median_items_per_sec>,<p95_ns_per_item>
//! MET,<impl>,<scenario>,<round>,<counter_name>,<value>   # 仅 --cfg mpmc_metrics
//! ```
//!
//! # 运行
//!
//! ```sh
//! taskset -c 0-3 cargo bench --offline --bench mpmc_compare
//! taskset -c 0-3 cargo bench --offline --features hybrid --bench mpmc_compare
//! # 只统计快/慢路径比例（计数会扭曲时间，不能用来测吞吐）：
//! RUSTFLAGS="--cfg mpmc_metrics" taskset -c 0-3 cargo bench --offline --bench mpmc_compare
//! ```
//!
//! 环境变量：`BENCH_OPS`（每个生产者，默认 20000）、`BENCH_ROUNDS`（默认 5）、
//! `BENCH_PRODUCERS` / `BENCH_CONSUMERS`（默认 4/4）、`BENCH_CAP`（默认 64，
//! mpmc-chan 只支持 8/64/1024）、`BENCH_WARMUP`（默认 1）、`BENCH_IMPLS`
//! （`all`/`mpmc`/`ext`，默认 `all`；筛选被测实现以缩短参数扫描时间）。
//!
//! `mpmc_metrics` 不是 Cargo feature，而是构建期 `--cfg` 开关：默认构建不编译
//! 任何计数代码，打开后每次计数都是全局原子 RMW，只用于看比例。
//!
//! 这不是严格 apples-to-apples 的微基准：异步实现使用 `futures_lite::block_on`
//! 驱动，crossbeam-channel 使用 OS 阻塞，crossbeam-queue 使用 spin。比较时请看
//! 同一 scenario 内的相对值，以及不同轮次/机器的方差。

use std::{
    env,
    hint::{black_box, spin_loop},
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use crossbeam_channel as cb;
use crossbeam_queue::ArrayQueue;
use flume as fl;
use futures_lite::future::block_on;
use mpmc_chan::{MpmcBuf, MpmcChan, RecvError, TryRecvError, TrySendError};

/// 本次编译对应的 mpmc-chan 方案标签。
fn mpmc_label_() -> &'static str {
    if cfg!(feature = "hybrid") {
        "mpmc-D"
    } else {
        "mpmc-C"
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ImplFilter_ {
    All,
    Mpmc,
    Ext,
}

#[derive(Clone, Copy)]
struct Cfg {
    ops_: usize,
    rounds_: usize,
    producers_: usize,
    consumers_: usize,
    cap_: usize,
    warmup_: bool,
    impls_: ImplFilter_,
}

impl Cfg {
    fn from_env_() -> Self {
        let cfg = Cfg {
            ops_: env_usize_("BENCH_OPS", 20_000),
            rounds_: env_usize_("BENCH_ROUNDS", 5),
            producers_: env_usize_("BENCH_PRODUCERS", 4),
            consumers_: env_usize_("BENCH_CONSUMERS", 4),
            cap_: env_usize_("BENCH_CAP", 64),
            warmup_: env_bool_("BENCH_WARMUP", true),
            impls_: match env::var("BENCH_IMPLS").as_deref() {
                Ok("mpmc") => ImplFilter_::Mpmc,
                Ok("ext") => ImplFilter_::Ext,
                _ => ImplFilter_::All,
            },
        };
        assert!(cfg.ops_ > 0, "BENCH_OPS 必须 > 0");
        assert!(cfg.rounds_ > 0, "BENCH_ROUNDS 必须 > 0");
        assert!(cfg.producers_ > 0, "BENCH_PRODUCERS 必须 > 0");
        assert!(cfg.consumers_ > 0, "BENCH_CONSUMERS 必须 > 0");
        assert!(
            matches!(cfg.cap_, 8 | 64 | 1024),
            "BENCH_CAP 必须是 8/64/1024（mpmc-chan 的 const N）"
        );
        cfg
    }

    fn total_(&self) -> usize {
        self.producers_ * self.ops_
    }

    fn with_ops_(&self, ops_: usize) -> Self {
        Cfg { ops_, ..*self }
    }
}

fn env_usize_(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn env_bool_(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(v) => !matches!(v.trim(), "0" | "false" | "no"),
        Err(_) => default,
    }
}

fn expected_sum_(total: usize) -> u64 {
    (0..total as u64).sum()
}

fn verify_(count: usize, sum: u64, cfg: &Cfg) {
    let total = cfg.total_();
    assert_eq!(count, total, "接收数量不匹配：{count} != {total}");
    assert_eq!(sum, expected_sum_(total), "接收值总和不匹配（可能丢/重）");
    black_box(count);
    black_box(sum);
}

fn median_(values: &mut [f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(|a, b| a.total_cmp(b));
    let n = values.len();
    if n % 2 == 1 {
        values[n / 2]
    } else {
        (values[n / 2 - 1] + values[n / 2]) / 2.0
    }
}

/// 最近秩百分位；`values` 会被排序。
fn percentile_(values: &mut [f64], pct: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(|a, b| a.total_cmp(b));
    let rank = (pct / 100.0 * values.len() as f64).ceil() as usize;
    values[rank.saturating_sub(1).min(values.len() - 1)]
}

/// 打印本轮计数器快照（仅 `--cfg mpmc_metrics` 且被测实现是 mpmc 时）。
#[cfg(mpmc_metrics)]
fn print_metrics_(impl_: &str, scenario: &str, round: usize) {
    if !impl_.starts_with("mpmc") {
        return;
    }
    let snapshot = mpmc_chan::bench_metrics::snapshot();
    let names = mpmc_chan::bench_metrics::names();
    for (name, value) in names.iter().zip(snapshot.iter()) {
        if *value > 0 {
            println!("MET,{impl_},{scenario},{round},{name},{value}");
        }
    }
}

fn run_series_<F>(impl_: &str, scenario: &str, cfg: &Cfg, mut run_once: F)
where
    F: FnMut(&Cfg) -> Duration,
{
    if cfg.warmup_ {
        let warm = cfg.with_ops_((cfg.ops_ / 4).max(1));
        let _ = run_once(&warm);
    }

    let mut ns_per_item = Vec::with_capacity(cfg.rounds_);
    for round in 0..cfg.rounds_ {
        #[cfg(mpmc_metrics)]
        if impl_.starts_with("mpmc") {
            mpmc_chan::bench_metrics::reset();
        }
        let elapsed = run_once(cfg);
        let items = cfg.total_();
        let secs = elapsed.as_secs_f64();
        let ns_per = secs * 1e9 / items as f64;
        let ips = items as f64 / secs;
        println!("RES,{impl_},{scenario},{round},{items},{secs:.6},{ips:.0},{ns_per:.3}");
        #[cfg(mpmc_metrics)]
        print_metrics_(impl_, scenario, round);
        ns_per_item.push(ns_per);
    }

    let median = median_(&mut ns_per_item);
    let p95 = percentile_(&mut ns_per_item, 95.0);
    println!(
        "SUM,{impl_},{scenario},{median:.3},{:.0},{p95:.3}",
        1e9 / median
    );
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// mpmc-chan：async
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

fn mpmc_async_once_<const N: usize>(cfg: &Cfg) -> Duration {
    let (tx, rx) = MpmcChan::new(MpmcBuf::<u64, N>::new());
    let barrier = Arc::new(Barrier::new(cfg.producers_ + cfg.consumers_ + 1));

    let mut producers = Vec::new();
    for p in 0..cfg.producers_ {
        let tx = tx.clone();
        let barrier = Arc::clone(&barrier);
        let ops = cfg.ops_;
        producers.push(thread::spawn(move || {
            barrier.wait();
            // 整个生产者循环只进一次 block_on：测的是通道 async 快/慢路径，
            // 不是每次操作都新建一个 block_on parker 的开销。
            block_on(async {
                for n in 0..ops {
                    let value = (p * ops + n) as u64;
                    tx.send_async(value).await.expect("mpmc send_async");
                }
            });
        }));
    }

    let mut consumers = Vec::new();
    for _ in 0..cfg.consumers_ {
        let rx = rx.clone();
        let barrier = Arc::clone(&barrier);
        consumers.push(thread::spawn(move || {
            barrier.wait();
            block_on(async {
                let mut count = 0usize;
                let mut sum = 0u64;
                loop {
                    match rx.recv_async().await {
                        Ok(value) => {
                            count += 1;
                            sum = sum.wrapping_add(value);
                        }
                        Err(RecvError::Closed) => break,
                        Err(RecvError::Cancelled) => panic!("mpmc recv_async 被取消"),
                    }
                }
                (count, sum)
            })
        }));
    }

    drop(tx);
    drop(rx);

    let start = Instant::now();
    barrier.wait();
    for h in producers {
        h.join().expect("mpmc producer 线程 panic");
    }
    let mut count = 0usize;
    let mut sum = 0u64;
    for h in consumers {
        let (c, s) = h.join().expect("mpmc consumer 线程 panic");
        count += c;
        sum = sum.wrapping_add(s);
    }
    let elapsed = start.elapsed();
    verify_(count, sum, cfg);
    elapsed
}

fn mpmc_async_(cfg: &Cfg) -> Duration {
    match cfg.cap_ {
        8 => mpmc_async_once_::<8>(cfg),
        64 => mpmc_async_once_::<64>(cfg),
        1024 => mpmc_async_once_::<1024>(cfg),
        _ => unreachable!("BENCH_CAP 已在 from_env_ 校验"),
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// mpmc-chan：try（非阻塞 spin）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

fn mpmc_try_once_<const N: usize>(cfg: &Cfg) -> Duration {
    let (tx, rx) = MpmcChan::new(MpmcBuf::<u64, N>::new());
    let barrier = Arc::new(Barrier::new(cfg.producers_ + cfg.consumers_ + 1));

    let mut producers = Vec::new();
    for p in 0..cfg.producers_ {
        let tx = tx.clone();
        let barrier = Arc::clone(&barrier);
        let ops = cfg.ops_;
        producers.push(thread::spawn(move || {
            barrier.wait();
            for n in 0..ops {
                let mut value = (p * ops + n) as u64;
                loop {
                    match tx.try_send(value) {
                        Ok(()) => break,
                        Err(TrySendError::Full(v)) => {
                            value = v;
                            spin_loop();
                        }
                        Err(TrySendError::Closed(v)) => {
                            panic!("mpmc try_send 已关闭，value={v}")
                        }
                    }
                }
            }
        }));
    }

    let mut consumers = Vec::new();
    for _ in 0..cfg.consumers_ {
        let rx = rx.clone();
        let barrier = Arc::clone(&barrier);
        consumers.push(thread::spawn(move || {
            barrier.wait();
            let mut count = 0usize;
            let mut sum = 0u64;
            loop {
                match rx.try_recv() {
                    Ok(value) => {
                        count += 1;
                        sum = sum.wrapping_add(value);
                    }
                    Err(TryRecvError::Empty) => spin_loop(),
                    Err(TryRecvError::Closed) => break,
                }
            }
            (count, sum)
        }));
    }

    drop(tx);
    drop(rx);

    let start = Instant::now();
    barrier.wait();
    for h in producers {
        h.join().expect("mpmc producer 线程 panic");
    }
    let mut count = 0usize;
    let mut sum = 0u64;
    for h in consumers {
        let (c, s) = h.join().expect("mpmc consumer 线程 panic");
        count += c;
        sum = sum.wrapping_add(s);
    }
    let elapsed = start.elapsed();
    verify_(count, sum, cfg);
    elapsed
}

fn mpmc_try_(cfg: &Cfg) -> Duration {
    match cfg.cap_ {
        8 => mpmc_try_once_::<8>(cfg),
        64 => mpmc_try_once_::<64>(cfg),
        1024 => mpmc_try_once_::<1024>(cfg),
        _ => unreachable!("BENCH_CAP 已在 from_env_ 校验"),
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// flume：async / try
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

fn flume_async_once_(cfg: &Cfg) -> Duration {
    let (tx, rx) = fl::bounded::<u64>(cfg.cap_);
    let barrier = Arc::new(Barrier::new(cfg.producers_ + cfg.consumers_ + 1));

    let mut producers = Vec::new();
    for p in 0..cfg.producers_ {
        let tx = tx.clone();
        let barrier = Arc::clone(&barrier);
        let ops = cfg.ops_;
        producers.push(thread::spawn(move || {
            barrier.wait();
            block_on(async {
                for n in 0..ops {
                    let value = (p * ops + n) as u64;
                    tx.send_async(value).await.expect("flume send_async");
                }
            });
        }));
    }

    let mut consumers = Vec::new();
    for _ in 0..cfg.consumers_ {
        let rx = rx.clone();
        let barrier = Arc::clone(&barrier);
        consumers.push(thread::spawn(move || {
            barrier.wait();
            block_on(async {
                let mut count = 0usize;
                let mut sum = 0u64;
                while let Ok(value) = rx.recv_async().await {
                    count += 1;
                    sum = sum.wrapping_add(value);
                }
                (count, sum)
            })
        }));
    }

    drop(tx);
    drop(rx);

    let start = Instant::now();
    barrier.wait();
    for h in producers {
        h.join().expect("flume producer 线程 panic");
    }
    let mut count = 0usize;
    let mut sum = 0u64;
    for h in consumers {
        let (c, s) = h.join().expect("flume consumer 线程 panic");
        count += c;
        sum = sum.wrapping_add(s);
    }
    let elapsed = start.elapsed();
    verify_(count, sum, cfg);
    elapsed
}

fn flume_try_once_(cfg: &Cfg) -> Duration {
    let (tx, rx) = fl::bounded::<u64>(cfg.cap_);
    let barrier = Arc::new(Barrier::new(cfg.producers_ + cfg.consumers_ + 1));

    let mut producers = Vec::new();
    for p in 0..cfg.producers_ {
        let tx = tx.clone();
        let barrier = Arc::clone(&barrier);
        let ops = cfg.ops_;
        producers.push(thread::spawn(move || {
            barrier.wait();
            for n in 0..ops {
                let mut value = (p * ops + n) as u64;
                loop {
                    match tx.try_send(value) {
                        Ok(()) => break,
                        Err(fl::TrySendError::Full(v)) => {
                            value = v;
                            spin_loop();
                        }
                        Err(fl::TrySendError::Disconnected(v)) => {
                            panic!("flume try_send 已断开，value={v}")
                        }
                    }
                }
            }
        }));
    }

    let mut consumers = Vec::new();
    for _ in 0..cfg.consumers_ {
        let rx = rx.clone();
        let barrier = Arc::clone(&barrier);
        consumers.push(thread::spawn(move || {
            barrier.wait();
            let mut count = 0usize;
            let mut sum = 0u64;
            loop {
                match rx.try_recv() {
                    Ok(value) => {
                        count += 1;
                        sum = sum.wrapping_add(value);
                    }
                    Err(fl::TryRecvError::Empty) => spin_loop(),
                    Err(fl::TryRecvError::Disconnected) => break,
                }
            }
            (count, sum)
        }));
    }

    drop(tx);
    drop(rx);

    let start = Instant::now();
    barrier.wait();
    for h in producers {
        h.join().expect("flume producer 线程 panic");
    }
    let mut count = 0usize;
    let mut sum = 0u64;
    for h in consumers {
        let (c, s) = h.join().expect("flume consumer 线程 panic");
        count += c;
        sum = sum.wrapping_add(s);
    }
    let elapsed = start.elapsed();
    verify_(count, sum, cfg);
    elapsed
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// crossbeam-channel：blocking / try
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

fn cb_chan_blocking_once_(cfg: &Cfg) -> Duration {
    let (tx, rx) = cb::bounded::<u64>(cfg.cap_);
    let barrier = Arc::new(Barrier::new(cfg.producers_ + cfg.consumers_ + 1));

    let mut producers = Vec::new();
    for p in 0..cfg.producers_ {
        let tx = tx.clone();
        let barrier = Arc::clone(&barrier);
        let ops = cfg.ops_;
        producers.push(thread::spawn(move || {
            barrier.wait();
            for n in 0..ops {
                let value = (p * ops + n) as u64;
                tx.send(value).expect("crossbeam-channel send");
            }
        }));
    }

    let mut consumers = Vec::new();
    for _ in 0..cfg.consumers_ {
        let rx = rx.clone();
        let barrier = Arc::clone(&barrier);
        consumers.push(thread::spawn(move || {
            barrier.wait();
            let mut count = 0usize;
            let mut sum = 0u64;
            while let Ok(value) = rx.recv() {
                count += 1;
                sum = sum.wrapping_add(value);
            }
            (count, sum)
        }));
    }

    drop(tx);
    drop(rx);

    let start = Instant::now();
    barrier.wait();
    for h in producers {
        h.join().expect("crossbeam-channel producer 线程 panic");
    }
    let mut count = 0usize;
    let mut sum = 0u64;
    for h in consumers {
        let (c, s) = h.join().expect("crossbeam-channel consumer 线程 panic");
        count += c;
        sum = sum.wrapping_add(s);
    }
    let elapsed = start.elapsed();
    verify_(count, sum, cfg);
    elapsed
}

fn cb_chan_try_once_(cfg: &Cfg) -> Duration {
    let (tx, rx) = cb::bounded::<u64>(cfg.cap_);
    let barrier = Arc::new(Barrier::new(cfg.producers_ + cfg.consumers_ + 1));

    let mut producers = Vec::new();
    for p in 0..cfg.producers_ {
        let tx = tx.clone();
        let barrier = Arc::clone(&barrier);
        let ops = cfg.ops_;
        producers.push(thread::spawn(move || {
            barrier.wait();
            for n in 0..ops {
                let mut value = (p * ops + n) as u64;
                loop {
                    match tx.try_send(value) {
                        Ok(()) => break,
                        Err(cb::TrySendError::Full(v)) => {
                            value = v;
                            spin_loop();
                        }
                        Err(cb::TrySendError::Disconnected(v)) => {
                            panic!("crossbeam-channel try_send 已断开，value={v}")
                        }
                    }
                }
            }
        }));
    }

    let mut consumers = Vec::new();
    for _ in 0..cfg.consumers_ {
        let rx = rx.clone();
        let barrier = Arc::clone(&barrier);
        consumers.push(thread::spawn(move || {
            barrier.wait();
            let mut count = 0usize;
            let mut sum = 0u64;
            loop {
                match rx.try_recv() {
                    Ok(value) => {
                        count += 1;
                        sum = sum.wrapping_add(value);
                    }
                    Err(cb::TryRecvError::Empty) => spin_loop(),
                    Err(cb::TryRecvError::Disconnected) => break,
                }
            }
            (count, sum)
        }));
    }

    drop(tx);
    drop(rx);

    let start = Instant::now();
    barrier.wait();
    for h in producers {
        h.join().expect("crossbeam-channel producer 线程 panic");
    }
    let mut count = 0usize;
    let mut sum = 0u64;
    for h in consumers {
        let (c, s) = h.join().expect("crossbeam-channel consumer 线程 panic");
        count += c;
        sum = sum.wrapping_add(s);
    }
    let elapsed = start.elapsed();
    verify_(count, sum, cfg);
    elapsed
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// crossbeam-queue：spin 基线
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

fn cb_queue_spin_once_(cfg: &Cfg) -> Duration {
    let queue = Arc::new(ArrayQueue::<u64>::new(cfg.cap_));
    let received = Arc::new(AtomicUsize::new(0));
    let total = cfg.total_();
    let barrier = Arc::new(Barrier::new(cfg.producers_ + cfg.consumers_ + 1));

    let mut producers = Vec::new();
    for p in 0..cfg.producers_ {
        let queue = Arc::clone(&queue);
        let barrier = Arc::clone(&barrier);
        let ops = cfg.ops_;
        producers.push(thread::spawn(move || {
            barrier.wait();
            for n in 0..ops {
                let mut value = (p * ops + n) as u64;
                loop {
                    match queue.push(value) {
                        Ok(()) => break,
                        Err(v) => {
                            value = v;
                            spin_loop();
                        }
                    }
                }
            }
        }));
    }

    let mut consumers = Vec::new();
    for _ in 0..cfg.consumers_ {
        let queue = Arc::clone(&queue);
        let received = Arc::clone(&received);
        let barrier = Arc::clone(&barrier);
        consumers.push(thread::spawn(move || {
            barrier.wait();
            let mut count = 0usize;
            let mut sum = 0u64;
            loop {
                if received.load(Ordering::Acquire) >= total {
                    break;
                }
                match queue.pop() {
                    Some(value) => {
                        count += 1;
                        sum = sum.wrapping_add(value);
                        received.fetch_add(1, Ordering::AcqRel);
                    }
                    None => spin_loop(),
                }
            }
            (count, sum)
        }));
    }

    let start = Instant::now();
    barrier.wait();
    for h in producers {
        h.join().expect("crossbeam-queue producer 线程 panic");
    }
    let mut count = 0usize;
    let mut sum = 0u64;
    for h in consumers {
        let (c, s) = h.join().expect("crossbeam-queue consumer 线程 panic");
        count += c;
        sum = sum.wrapping_add(s);
    }
    let elapsed = start.elapsed();
    verify_(count, sum, cfg);
    elapsed
}

fn main() {
    let cfg = Cfg::from_env_();
    eprintln!(
        "# mpmc_compare impl={} ops={} rounds={} producers={} consumers={} cap={} warmup={}",
        mpmc_label_(),
        cfg.ops_,
        cfg.rounds_,
        cfg.producers_,
        cfg.consumers_,
        cfg.cap_,
        cfg.warmup_
    );

    let mpmc = mpmc_label_();
    if cfg.impls_ != ImplFilter_::Ext {
        run_series_(mpmc, "async", &cfg, mpmc_async_);
        run_series_(mpmc, "try", &cfg, mpmc_try_);
    }

    if cfg.impls_ != ImplFilter_::Mpmc {
        run_series_("flume", "async", &cfg, flume_async_once_);
        run_series_("flume", "try", &cfg, flume_try_once_);

        run_series_(
            "crossbeam-channel",
            "blocking",
            &cfg,
            cb_chan_blocking_once_,
        );
        run_series_("crossbeam-channel", "try", &cfg, cb_chan_try_once_);

        run_series_("crossbeam-queue", "spin", &cfg, cb_queue_spin_once_);
    }
}
