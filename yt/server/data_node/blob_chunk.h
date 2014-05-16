#pragma once

#include "public.h"
#include "chunk.h"

#include <core/misc/cache.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! A base for both TStoredBlobChunk and TCachedBlobChunk.
class TBlobChunk
    : public IChunk
{
public:
    virtual const TChunkId& GetId() const override;
    virtual TLocationPtr GetLocation() const override;
    virtual const NChunkClient::NProto::TChunkInfo& GetInfo() const override;

    virtual Stroka GetFileName() const override;

    virtual TAsyncGetMetaResult GetMeta(
        i64 priority,
        const std::vector<int>* tags = nullptr) override;

    virtual TAsyncError ReadBlocks(
        int firstBlockIndex,
        int blockCount,
        i64 priority,
        std::vector<TSharedRef>* blocks) override;

    virtual TRefCountedChunkMetaPtr GetCachedMeta() const override;

    virtual bool TryAcquireReadLock() override;
    virtual void ReleaseReadLock() override;
    virtual bool IsReadLockAcquired() const override;

    virtual TFuture<void> ScheduleRemoval() override;

protected:
    TChunkId Id_;
    TLocationPtr Location_;
    NChunkClient::NProto::TChunkInfo Info_;

    TBlobChunk(
        TLocationPtr location,
        const TChunkId& chunkId,
        const NChunkClient::NProto::TChunkMeta& chunkMeta,
        const NChunkClient::NProto::TChunkInfo& chunkInfo,
        NCellNode::TNodeMemoryTracker* memoryUsageTracker);

    TBlobChunk(
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        NCellNode::TNodeMemoryTracker* memoryUsageTracker);

    ~TBlobChunk();

    void EvictChunkReader();

private:
    void DoRemoveChunk();

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

    TSpinLock SpinLock_;
    TRefCountedChunkMetaPtr Meta_;
    NChunkClient::NProto::TBlocksExt BlocksExt_;
    

    int ReadLockCounter_ = 0;
    bool RemovalScheduled_ = false;
    TPromise<void> RemovedEvent_;

    NCellNode::TNodeMemoryTracker* MemoryUsageTracker_;

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
        const NChunkClient::NProto::TChunkMeta& chunkMeta,
        const NChunkClient::NProto::TChunkInfo& chunkInfo,
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
        const NChunkClient::NProto::TChunkMeta& chunkMeta,
        const NChunkClient::NProto::TChunkInfo& chunkInfo,
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

