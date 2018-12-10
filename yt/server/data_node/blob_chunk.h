#pragma once

#include "public.h"
#include "artifact.h"
#include "chunk_block_manager.h"
#include "chunk_detail.h"
#include "chunk_meta_manager.h"

#include <yt/ytlib/chunk_client/block.h>
#include <yt/ytlib/chunk_client/chunk_info.pb.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/misc/async_cache.h>

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

    virtual TFuture<NChunkClient::TRefCountedChunkMetaPtr> ReadMeta(
        const TBlockReadOptions& options,
        const std::optional<std::vector<int>>& extensionTags = std::nullopt) override;

    virtual TFuture<std::vector<NChunkClient::TBlock>> ReadBlockSet(
        const std::vector<int>& blockIndexes,
        const TBlockReadOptions& options);

    virtual TFuture<std::vector<NChunkClient::TBlock>> ReadBlockRange(
        int firstBlockIndex,
        int blockCount,
        const TBlockReadOptions& options);

    virtual void SyncRemove(bool force) override;

    NChunkClient::TRefCountedBlocksExtPtr FindCachedBlocksExt();
    NChunkClient::TRefCountedBlocksExtPtr GetCachedBlocksExt();

protected:
    TBlobChunkBase(
        NCellNode::TBootstrap* bootstrap,
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        NChunkClient::TRefCountedChunkMetaPtr meta);

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
        std::vector<NChunkClient::TBlock> Blocks;
        TBlockReadOptions Options;
    };

    using TReadBlockSetSessionPtr = TIntrusivePtr<TReadBlockSetSession>;


    NChunkClient::NProto::TChunkInfo Info_;

    NConcurrency::TReaderWriterSpinLock CachedBlocksExtLock_;
    NChunkClient::TRefCountedBlocksExtPtr CachedBlocksExt_;
    TPromise<void> CachedBlocksExtPromise_;
    std::atomic<bool> HasCachedBlocksExt_ = {false};

    //! Returns true if location must be disabled.
    bool IsFatalError(const TError& error) const;


    TFuture<void> ReadBlocksExt(const TBlockReadOptions& options);
    void SetBlocksExt(const NChunkClient::TRefCountedChunkMetaPtr& meta);

    void DoReadMeta(
        TChunkReadGuard readGuard,
        TCachedChunkMetaCookie cookie,
        const TBlockReadOptions& options);
    TFuture<void> OnBlocksExtLoaded(const TReadBlockSetSessionPtr& session);
    void DoReadBlockSet(
        const TReadBlockSetSessionPtr& session,
        TPendingIOGuard pendingIOGuard);
};

DEFINE_REFCOUNTED_TYPE(TBlobChunkBase)

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
        NChunkClient::TRefCountedChunkMetaPtr meta = nullptr);

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
        NChunkClient::TRefCountedChunkMetaPtr meta,
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

