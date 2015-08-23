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
        i64 priority,
        const TNullable<std::vector<int>>& extensionTags) override;

    virtual TFuture<std::vector<TSharedRef>> ReadBlockSet(
        const std::vector<int>& blockIndexes,
        i64 priority,
        bool populateCache,
        NChunkClient::IBlockCachePtr blockCache) override;

    virtual TFuture<std::vector<TSharedRef>> ReadBlockRange(
        int firstBlockIndex,
        int blockCount,
        i64 priority,
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
            int BlockIndex = -1;
            TSharedRef Data;
            TCachedBlockCookie Cookie;
            bool Cached = false;
        };

        std::vector<TBlockEntry> Entries;
        std::vector<TFuture<void>> CacheResults;
        TPromise<std::vector<TSharedRef>> Promise = NewPromise<std::vector<TSharedRef>>();
    };

    using TReadBlockSetSessionPtr = TIntrusivePtr<TReadBlockSetSession>;


    NChunkClient::NProto::TChunkInfo Info_;

    NConcurrency::TReaderWriterSpinLock CachedMetaLock_;
    TPromise<void> CachedMetaPromise_;
    TRefCountedChunkMetaPtr CachedMeta_;
    NCellNode::TNodeMemoryTrackerGuard MemoryTrackerGuard_;
    NChunkClient::NProto::TBlocksExt CachedBlocksExt_;


    TFuture<void> GetMeta(i64 priority);
    void SetMetaLoadSuccess(const NChunkClient::NProto::TChunkMeta& meta);
    void SetMetaLoadError(const TError& error);
    void DoReadMeta(TChunkReadGuard readGuard);

    void DoReadBlockSet(
        TReadBlockSetSessionPtr session,
        TPendingReadSizeGuard pendingReadSizeGuard);

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

