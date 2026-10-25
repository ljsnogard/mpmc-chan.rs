//! 自建等待队列 MPMC 通道的内部模块与公开导出。
//!
//! 导出列表逐项列出，不使用 `*` 自动匹配；实际逻辑分散在 `core_` /
//! `sender_` / `receiver_` / `wait_queue_` / `config_` / `util_` / `error_` 中。

mod config_;
mod core_;
mod error_;
mod metrics_;
mod receiver_;
mod sender_;
mod util_;
mod wait_queue_;

pub use config_::{
    DEFAULT_CHUNK_BYTES, DefaultConfig, MAX_SLOTS_PER_CHUNK, SlabListConfig, TrConfig,
    TrSlabConfig, WaitSlab4K, WaitSlab16K, WaitSlab64K,
};
pub use core_::MpmcChan;
pub use error_::{RecvError, SendError, TryRecvError, TrySendError};
pub use receiver_::Receiver;
/// 异步接收参数载体；可直接 `.await`，或先 `.may_cancel_with(token)`。
pub use receiver_::{RecvAsync, RecvFuture};
pub use sender_::Sender;
/// 异步发送参数载体；可直接 `.await`，或先 `.may_cancel_with(token)`。
pub use sender_::{SendAsync, SendFuture};

#[cfg(test)]
mod tests_;
