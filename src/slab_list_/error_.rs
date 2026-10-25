//! SlabList 的错误类型。

use core::fmt;

use crate::chan::MAX_SLOTS_PER_CHUNK;

/// SlabList 在创建阶段可能返回的错误。
///
/// 这些错误都源于配置提供的 Chunk 字节数与具体元素类型 `T` 的槽布局不匹配。
/// 错误只会出现在创建 arena 时，不会出现在后续的 `insert_` / `remove_` 路径上。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SlabListError {
    /// 单 Chunk 的字节数不足以容纳 header、对齐填充和至少一个槽位。
    ChunkTooSmall,

    /// 按配置字节数计算出的槽位数超过 [`MAX_SLOTS_PER_CHUNK`]。
    TooManySlots,

    /// Chunk 的内存布局无法表示，例如大小超过 `isize::MAX`。
    LayoutOverflow,
}

impl fmt::Display for SlabListError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SlabListError::ChunkTooSmall => f.write_str("单个 Chunk 的内存不足以容纳至少一个槽位"),
            SlabListError::TooManySlots => {
                write!(f, "单个 Chunk 的槽位数超过上限 {MAX_SLOTS_PER_CHUNK}")
            }
            SlabListError::LayoutOverflow => f.write_str("单个 Chunk 的内存布局计算溢出或非法"),
        }
    }
}

impl core::error::Error for SlabListError {}
