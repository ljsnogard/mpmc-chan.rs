//! 通道的公开错误类型。

use core::fmt;

/// 非阻塞发送失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// 当前无法发送：环满、目标槽尚未回收，或生产方向锁被慢写者/短临界区占用。
    /// 元素原样返回。
    Full(T),
    /// 接收侧已关闭（或发送侧已显式关闭）。元素原样返回。
    Closed(T),
}

impl<T: fmt::Debug> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => f.write_str("通道当前不可写（满或方向锁被占用）"),
            TrySendError::Closed(_) => f.write_str("通道的接收侧已关闭"),
        }
    }
}

impl<T: fmt::Debug> core::error::Error for TrySendError<T> {}

/// 异步发送失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError<T> {
    /// 接收侧已关闭（或发送侧已显式关闭）。元素原样返回。
    Closed(T),
    /// 等待被取消；元素尚未入队，原样返回。
    Cancelled(T),
}

impl<T: fmt::Debug> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::Closed(_) => f.write_str("通道的接收侧已关闭"),
            SendError::Cancelled(_) => f.write_str("发送等待被取消"),
        }
    }
}

impl<T: fmt::Debug> core::error::Error for SendError<T> {}

/// 非阻塞接收失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// 当前取不到元素：环空、头槽已预留但生产者尚未发布，或消费方向锁被
    /// 慢写者/短临界区占用。
    Empty,
    /// 通道已关闭且没有更多元素。
    Closed,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => f.write_str("通道当前不可读（空或方向锁被占用）"),
            TryRecvError::Closed => f.write_str("通道已关闭且没有更多元素"),
        }
    }
}

impl core::error::Error for TryRecvError {}

/// 异步接收失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    /// 通道已关闭且没有更多元素；已入队的元素会先被 drain。
    Closed,
    /// 等待被取消。
    Cancelled,
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecvError::Closed => f.write_str("通道已关闭且没有更多元素"),
            RecvError::Cancelled => f.write_str("接收等待被取消"),
        }
    }
}

impl core::error::Error for RecvError {}
