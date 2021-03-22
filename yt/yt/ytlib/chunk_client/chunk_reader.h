#pragma once

#include "block.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! A basic interface for reading chunks from a suitable source.
struct IChunkReader
    : public virtual TRefCounted
{
    //! Asynchronously reads a given set of blocks.
    //! Returns a collection of blocks, each corresponding to a single given index.
    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientChunkReadOptions& options,
        const std::vector<int>& blockIndexes,
        std::optional<i64> estimatedSize = {}) = 0;

    //! Asynchronously reads a given range of blocks.
    //! The call may return less blocks than requested.
    //! If an empty list of blocks is returned then there are no blocks in the given range.
    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientChunkReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        std::optional<i64> estimatedSize = {}) = 0;

    //! Asynchronously obtains a meta, possibly filtered by #partitionTag and #extensionTags.
    virtual TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientChunkReadOptions& options,
        std::optional<int> partitionTag = std::nullopt,
        const std::optional<std::vector<int>>& extensionTags = {}) = 0;

    //! Returns the id of the read this reader is assigned to read.
    virtual TChunkId GetChunkId() const = 0;

    //! Upon fatal failures reader updates its failure time.
    virtual TInstant GetLastFailureTime() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
