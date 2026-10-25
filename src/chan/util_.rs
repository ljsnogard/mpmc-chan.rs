//! 异步等待的小工具：只保留自旋检查间隔。
//!
//! 取消竞速直接使用 `abs_cancel::ok_or::XtOkOr`，不再自研 `poll_fn` 竞速。

/// P1：自旋中每多少次检查一次取消令牌。
///
/// 自旋是同步段，没有 `.await`；取消/关闭不能只靠下一次 await 观察。每 16 次
/// 检查一次，既控制延迟又避免把原子 load 放进最内层循环。
pub(super) const CANCEL_CHECK_EVERY: usize = 16;
