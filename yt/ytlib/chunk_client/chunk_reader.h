#pragma once

#include "public.h"

#include <core/misc/ref.h>

#include <core/actions/future.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! A basic interface for reading chunks from a suitable source.
struct IChunkReader
    : public virtual TRefCounted
{
    //! Asynchronously reads a given set of blocks.
    //! Returns a collection of blocks, each corresponding to a single given index.
    virtual TFuture<std::vector<TSharedRef>> ReadBlocks(const std::vector<int>& blockIndexes) = 0;

    //! Asynchronously reads a given range of blocks.
    //! The call may return less blocks than requested.
    //! If an empty list of blocks is returned then there are no blocks in the given range.
    virtual TFuture<std::vector<TSharedRef>> ReadBlocks(int firstBlockIndex, int blockCount) = 0;

    //! Asynchronously obtains a meta, possibly filtered by #partitionTag and #extensionTags.
    virtual TFuture<NProto::TChunkMeta> GetMeta(
        const TNullable<int>& partitionTag = Null,
        const std::vector<int>* extensionTags = nullptr) = 0;

    virtual TChunkId GetChunkId() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkReader)

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
