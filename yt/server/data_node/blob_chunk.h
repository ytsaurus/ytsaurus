#pragma once

#include "public.h"
#include "chunk_detail.h"

#include <core/misc/cache.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! A base for both TStoredBlobChunk and TCachedBlobChunk.
class TBlobChunk
    : public TChunk
{
public:
    virtual TAsyncGetMetaResult GetMeta(
        i64 priority,
        const std::vector<int>* tags = nullptr) override;

    virtual TAsyncError ReadBlocks(
        int firstBlockIndex,
        int blockCount,
        i64 priority,
        std::vector<TSharedRef>* blocks) override;

protected:
    TBlobChunk(
        TLocationPtr location,
        const TChunkId& chunkId,
        const NChunkClient::NProto::TChunkMeta& meta,
        const NChunkClient::NProto::TChunkInfo& info,
        NCellNode::TNodeMemoryTracker* memoryUsageTracker);

    TBlobChunk(
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        NCellNode::TNodeMemoryTracker* memoryUsageTracker);

    virtual void EvictFromCache() override;
    virtual TFuture<void> RemoveFiles() override;

private:
    TAsyncError ReadMeta(i64 priority);
    void DoReadMeta(TPromise<TError> promise);
    void InitializeCachedMeta(const NChunkClient::NProto::TChunkMeta& meta);

    i64 ComputePendingReadSize(int firstBlockIndex, int blockCount);

    void DoReadBlocks(
        int firstBlockIndex,
        int blockCount,
        i64 pendingSize,
        TPromise<TError> promise,
        std::vector<TSharedRef>* blocks);


    NChunkClient::NProto::TBlocksExt BlocksExt_;
    
};

////////////////////////////////////////////////////////////////////////////////

//! A blob chunk owned by TChunkStore.
class TStoredBlobChunk
    : public TBlobChunk
{
public:
    TStoredBlobChunk(
        TLocationPtr location,
        const TChunkId& chunkId,
        const NChunkClient::NProto::TChunkMeta& meta,
        const NChunkClient::NProto::TChunkInfo& info,
        NCellNode::TNodeMemoryTracker* memoryUsageTracker);

    TStoredBlobChunk(
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        NCellNode::TNodeMemoryTracker* memoryUsageTracker);

};

DEFINE_REFCOUNTED_TYPE(TStoredBlobChunk)

////////////////////////////////////////////////////////////////////////////////

//! A blob chunk owned by TChunkCache.
class TCachedBlobChunk
    : public TBlobChunk
    , public TCacheValueBase<TChunkId, TCachedBlobChunk>
{
public:
    TCachedBlobChunk(
        TLocationPtr location,
        const TChunkId& chunkId,
        const NChunkClient::NProto::TChunkMeta& meta,
        const NChunkClient::NProto::TChunkInfo& info,
        TChunkCachePtr chunkCache,
        NCellNode::TNodeMemoryTracker* memoryUsageTracker);

    TCachedBlobChunk(
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        TChunkCachePtr chunkCache,
        NCellNode::TNodeMemoryTracker* memoryUsageTracker);

    ~TCachedBlobChunk();

private:
    TWeakPtr<TChunkCache> ChunkCache_;

};

DEFINE_REFCOUNTED_TYPE(TCachedBlobChunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

