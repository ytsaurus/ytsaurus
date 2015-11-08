#pragma once

#include "public.h"
#include "chunk_detail.h"
#include "block_store.h"
#include "artifact.h"

#include <core/misc/async_cache.h>

#include <core/concurrency/rw_spinlock.h>

#include <server/misc/memory_usage_tracker.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! A base for both TStoredBlobChunk and TCachedBlobChunk.
class TBlobChunkBase
    : public TChunkBase
{
public:
    virtual NChunkClient::NProto::TChunkInfo GetInfo() const override;

    virtual bool IsActive() const override;

    virtual TFuture<TRefCountedChunkMetaPtr> ReadMeta(
        const TWorkloadDescriptor& workloadDescriptor,
        const TNullable<std::vector<int>>& extensionTags) override;

    virtual TFuture<std::vector<TSharedRef>> ReadBlockSet(
        const std::vector<int>& blockIndexes,
        const TWorkloadDescriptor& workloadDescriptor,
        bool populateCache,
        NChunkClient::IBlockCachePtr blockCache) override;

    virtual TFuture<std::vector<TSharedRef>> ReadBlockRange(
        int firstBlockIndex,
        int blockCount,
        const TWorkloadDescriptor& workloadDescriptor,
        bool populateCache,
        NChunkClient::IBlockCachePtr blockCache) override;

    virtual void SyncRemove(bool force) override;

protected:
    TBlobChunkBase(
        NCellNode::TBootstrap* bootstrap,
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        const NChunkClient::NProto::TChunkMeta* meta);

    virtual TFuture<void> AsyncRemove() override;

private:
    struct TReadBlockSetSession
        : public TIntrinsicRefCounted
    {
        struct TBlockEntry
        {
            int LocalIndex = -1;
            int BlockIndex = -1;
            bool Cached = false;
            TCachedBlockCookie Cookie;
        };

        std::vector<TBlockEntry> Entries;
        std::vector<TSharedRef> Blocks;
    };

    using TReadBlockSetSessionPtr = TIntrusivePtr<TReadBlockSetSession>;


    NChunkClient::NProto::TChunkInfo Info_;

    NConcurrency::TReaderWriterSpinLock CachedMetaLock_;
    TPromise<void> CachedMetaPromise_;
    TRefCountedChunkMetaPtr CachedMeta_;
    NCellNode::TNodeMemoryTrackerGuard MemoryTrackerGuard_;
    NChunkClient::NProto::TBlocksExt CachedBlocksExt_;


    TFuture<void> GetMeta(const TWorkloadDescriptor& workloadDescriptor);
    void SetMetaLoadSuccess(const NChunkClient::NProto::TChunkMeta& meta);
    void SetMetaLoadError(const TError& error);
    void DoReadMeta(TChunkReadGuard readGuard);
    void DoReadBlockSet(
        TReadBlockSetSessionPtr session,
        const TWorkloadDescriptor& workloadDescriptor);
};

////////////////////////////////////////////////////////////////////////////////

//! A blob chunk owned by TChunkStore.
class TStoredBlobChunk
    : public TBlobChunkBase
{
public:
    TStoredBlobChunk(
        NCellNode::TBootstrap* bootstrap,
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        const NChunkClient::NProto::TChunkMeta* meta = nullptr);

};

DEFINE_REFCOUNTED_TYPE(TStoredBlobChunk)

////////////////////////////////////////////////////////////////////////////////

//! A blob chunk owned by TChunkCache.
class TCachedBlobChunk
    : public TBlobChunkBase
    , public TAsyncCacheValueBase<TArtifactKey, TCachedBlobChunk>
{
public:
    TCachedBlobChunk(
        NCellNode::TBootstrap* bootstrap,
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        const NChunkClient::NProto::TChunkMeta* meta,
        const TArtifactKey& key,
        TClosure destroyed);

    ~TCachedBlobChunk();

private:
    const TClosure Destroyed_;

};

DEFINE_REFCOUNTED_TYPE(TCachedBlobChunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

