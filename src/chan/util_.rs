//! 异步等待的小工具。

use core::{
    future::{Future, IntoFuture, poll_fn},
    pin::pin,
    task::Poll,
};

use abs_cancel::TrCancellationToken;

/// P1：自旋中每多少次检查一次取消令牌。
///
/// 自旋是同步段，没有 `.await`；取消/关闭不能只靠下一次 await 观察。每 16 次
/// 检查一次，既控制延迟又避免把原子 load 放进最内层循环。
#[cfg(not(feature = "hybrid"))]
pub(super) const CANCEL_CHECK_EVERY: usize = 16;

/// 让 `fut` 与取消令牌竞速；令牌先就绪时返回 `None` 并丢弃 `fut`。
///
/// 丢弃 `fut` 会触发其中 Guard 的 Drop（释放方向写锁、撤销锁等待节点），
/// 因此取消路径不需要额外清理。
///
/// 这里用 `child_token()` 而不是要求 `C: Clone`：`TrCancellationToken` 只保证
/// 能派生 child；child 在父令牌取消时同样取消。
pub(super) async fn race_cancel_<F, C>(cancel: &C, fut: F) -> Option<F::Output>
where
    F: Future,
    C: TrCancellationToken,
{
    let mut fut = pin!(fut);
    let mut cancelled = pin!(cancel.child_token().cancellation());

    poll_fn(|cx| {
        if cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Option::None);
        }
        fut.as_mut().poll(cx).map(Option::Some)
    })
    .await
}

/// 让 `IntoFuture` 也走 [`race_cancel_`] 的便捷包装。
pub(super) async fn race_cancel_into_<F, C>(cancel: &C, fut: F) -> Option<F::Output>
where
    F: IntoFuture,
    C: TrCancellationToken,
{
    race_cancel_(cancel, fut.into_future()).await
}
