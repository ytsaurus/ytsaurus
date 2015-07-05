#pragma once

#include "public.h"
#include "chunk_detail.h"

#include <core/misc/async_cache.h>

#include <core/concurrency/rw_spinlock.h>

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

    virtual TFuture<std::vector<TSharedRef>> ReadBlocks(
        int firstBlockIndex,
        int blockCount,
        i64 priority) override;

    virtual void SyncRemove(bool force) override;

protected:
    TBlobChunkBase(
        NCellNode::TBootstrap* bootstrap,
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        const NChunkClient::NProto::TChunkMeta* meta);
    ~TBlobChunkBase();

    virtual TFuture<void> AsyncRemove() override;

private:
    NChunkClient::NProto::TChunkInfo Info_;

    NConcurrency::TReaderWriterSpinLock CachedMetaLock_;
    TPromise<void> CachedMetaPromise_;
    TRefCountedChunkMetaPtr CachedMeta_;
    NChunkClient::NProto::TBlocksExt CachedBlocksExt_;


    TFuture<void> GetMeta(i64 priority);
    void SetMetaLoadSuccess(const NChunkClient::NProto::TChunkMeta& meta);
    void SetMetaLoadError(const TError& error);
    void DoReadMeta(TChunkReadGuard readGuard);

    void DoReadBlocks(
        int firstBlockIndex,
        int blockCount,
        TPendingReadSizeGuard pendingReadSizeGuard,
        TPromise<std::vector<TSharedRef>> promise);

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
    , public TAsyncCacheValueBase<TChunkId, TCachedBlobChunk>
{
public:
    TCachedBlobChunk(
        NCellNode::TBootstrap* bootstrap,
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        const NChunkClient::NProto::TChunkMeta* meta,
        TClosure destroyed);

    ~TCachedBlobChunk();

private:
    const TClosure Destroyed_;

};

DEFINE_REFCOUNTED_TYPE(TCachedBlobChunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

