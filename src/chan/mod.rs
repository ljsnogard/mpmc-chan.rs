//! 双锁 MPMC 通道的内部模块与公开导出。
//!
//! 导出列表逐项列出，不使用 `*` 自动匹配；实际逻辑分散在 `core_` /
//! `sender_` / `receiver_` / `waker_` / `util_` / `error_` 中。

mod core_;
mod error_;
mod receiver_;
mod sender_;
mod util_;
mod waker_;

pub use core_::MpmcChan;
pub use error_::{ChanBuildError, RecvError, SendError, TryRecvError, TrySendError};
pub use receiver_::Receiver;
/// 异步接收参数载体；可直接 `.await`，或先 `.may_cancel_with(token)`。
pub use receiver_::{RecvAsync, RecvFuture};
pub use sender_::Sender;
/// 异步发送参数载体；可直接 `.await`，或先 `.may_cancel_with(token)`。
pub use sender_::{SendAsync, SendFuture};

#[cfg(test)]
mod tests_;
