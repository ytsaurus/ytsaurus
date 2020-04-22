#pragma once

#include "artifact.h"
#include "chunk_block_manager.h"
#include "chunk_detail.h"
#include "chunk_meta_manager.h"

#include <yt/ytlib/chunk_client/block.h>
#include <yt/ytlib/chunk_client/proto/chunk_info.pb.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/misc/async_cache.h>

namespace NYT::NDataNode {

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

protected:
    TBlobChunkBase(
        NCellNode::TBootstrap* bootstrap,
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        NChunkClient::TRefCountedChunkMetaPtr meta);

    virtual TFuture<void> AsyncRemove() override;

private:
    struct TReadBlockSetSession
        : public TReadSessionBase
    {
        struct TBlockEntry
        {
            int BlockIndex = -1;
            bool Cached = false;
            TCachedBlockCookie Cookie;
            NChunkClient::TBlock Block;
        };

        IInvokerPtr Invoker;
        std::optional<NProfiling::TWallTimer> ReadTimer;
        std::unique_ptr<TBlockEntry[]> Entries;
        int CurrentEntryIndex = 0;
        int EntryCount = 0;
        std::vector<TFuture<void>> AsyncResults;
        TPromise<std::vector<NChunkClient::TBlock>> SessionPromise = NewPromise<std::vector<NChunkClient::TBlock>>();
        TPromise<void> DiskFetchPromise;
    };

    using TReadBlockSetSessionPtr = TIntrusivePtr<TReadBlockSetSession>;

    NChunkClient::NProto::TChunkInfo Info_;

    NConcurrency::TReaderWriterSpinLock BlocksExtLock_;
    TWeakPtr<NChunkClient::TRefCountedBlocksExt> WeakBlocksExt_;

    TSpinLock CachedReaderSpinLock_;
    TWeakPtr<NChunkClient::TFileReader> CachedWeakReader_;

    //! Returns true if location must be disabled.
    static bool IsFatalError(const TError& error);

    NChunkClient::TFileReaderPtr GetReader();

    void CompleteSession(const TIntrusivePtr<TReadBlockSetSession>& session);
    static void FailSession(const TIntrusivePtr<TReadBlockSetSession>& session, const TError& error);

    void DoReadMeta(
        const TReadMetaSessionPtr& session,
        TCachedChunkMetaCookie cookie);
    void OnBlocksExtLoaded(
        const TReadBlockSetSessionPtr& session,
        const NChunkClient::TRefCountedBlocksExtPtr& blocksExt);

    void DoReadSession(
        const TReadBlockSetSessionPtr& session,
        i64 pendingDataSize);
    void DoReadBlockSet(
        const TReadBlockSetSessionPtr& session,
        TPendingIOGuard&& pendingIOGuard);
    void OnBlocksRead(
        const TReadBlockSetSessionPtr& session,
        int firstBlockIndex,
        int beginEntryIndex,
        int endEntryIndex,
        TPendingIOGuard&& pendingIOGuard,
        const TErrorOr<std::vector<NChunkClient::TBlock>>& blocksOrError);

    //! Returns `true` if chunk was writen with `sync_on_close` option.
    //! Default value is `true`.
    bool ShouldSyncOnClose() const;
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
        TClosure destroyedHandler);

    ~TCachedBlobChunk();

private:
    const TClosure DestroyedHandler_;
};

DEFINE_REFCOUNTED_TYPE(TCachedBlobChunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

