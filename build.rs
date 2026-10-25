//! 构建脚本：声明基准计数用的自定义 `cfg`，避免 `unexpected_cfgs` 警告。
//!
//! `mpmc_metrics` 不是 Cargo feature，只在显式传入
//! `RUSTFLAGS="--cfg mpmc_metrics"` 时开启；默认构建不编译任何计数代码。

fn main() {
    println!("cargo::rustc-check-cfg=cfg(mpmc_metrics)");
}
