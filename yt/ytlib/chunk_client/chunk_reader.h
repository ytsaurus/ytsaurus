#pragma once

#include "block.h"
#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/client/misc/workload.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TClientBlockReadOptions
{
    TWorkloadDescriptor WorkloadDescriptor;
    TChunkReaderStatisticsPtr ChunkReaderStatistics;
    TReadSessionId ReadSessionId;
};

//! A basic interface for reading chunks from a suitable source.
struct IChunkReader
    : public virtual TRefCounted
{
    //! Asynchronously reads a given set of blocks.
    //! Returns a collection of blocks, each corresponding to a single given index.
    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& options,
        const std::vector<int>& blockIndexes,
        const TNullable<i64>& estimatedSize = Null) = 0;

    //! Asynchronously reads a given range of blocks.
    //! The call may return less blocks than requested.
    //! If an empty list of blocks is returned then there are no blocks in the given range.
    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        const TNullable<i64>& estimatedSize = Null) = 0;

    //! Asynchronously obtains a meta, possibly filtered by #partitionTag and #extensionTags.
    virtual TFuture<NProto::TChunkMeta> GetMeta(
        const TClientBlockReadOptions& options,
        const TNullable<int>& partitionTag = Null,
        const TNullable<std::vector<int>>& extensionTags = Null) = 0;

    virtual TChunkId GetChunkId() const = 0;

    virtual bool IsValid() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
