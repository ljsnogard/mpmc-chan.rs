//! 计数宏：`--cfg mpmc_metrics` 下展开为真实计数，否则展开为空。
//!
//! 用宏而不是函数，是为了让未开启计数时连计数器常量名都不参与解析；调用点
//! 只需写 `metric_!(PUSH_FAST_OK);`，默认构建中展开成空块并被编译器消除。

/// 给 [`crate::bench_metrics`] 的某个计数器加一；未开启 `mpmc_metrics` 时为空。
macro_rules! metric_ {
    ($name:ident) => {{
        #[cfg(mpmc_metrics)]
        {
            crate::bench_metrics::inc_(crate::bench_metrics::$name);
        }
    }};
}

pub(super) use metric_;
