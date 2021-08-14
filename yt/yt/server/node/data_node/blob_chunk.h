#pragma once

#include "artifact.h"
#include "chunk_block_manager.h"
#include "chunk_detail.h"
#include "chunk_meta_manager.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/block_cache.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_info.pb.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/concurrency/spinlock.h>

#include <yt/yt/core/misc/async_slru_cache.h>

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
        const TChunkReadOptions& options,
        const std::optional<std::vector<int>>& extensionTags = std::nullopt) override;

    virtual TFuture<std::vector<NChunkClient::TBlock>> ReadBlockSet(
        const std::vector<int>& blockIndexes,
        const TChunkReadOptions& options) override;

    virtual TFuture<std::vector<NChunkClient::TBlock>> ReadBlockRange(
        int firstBlockIndex,
        int blockCount,
        const TChunkReadOptions& options) override;

    virtual NIO::IIOEngine::TReadRequest MakeChunkFragmentReadRequest(
        const NIO::TChunkFragmentDescriptor& fragmentDescriptor) override;

    virtual void SyncRemove(bool force) override;

    NChunkClient::TRefCountedBlocksExtPtr FindCachedBlocksExt();

protected:
    TBlobChunkBase(
        NClusterNode::IBootstrapBase* bootstrap,
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
            //! Index of this entry before sorting by block index.
            int EntryIndex = -1;
            bool Cached = false;
            std::unique_ptr<NChunkClient::ICachedBlockCookie> Cookie;
            NChunkClient::TBlock Block;
            i64 BeginOffset = -1;
            i64 EndOffset = -1;
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

    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, BlocksExtLock_);
    TWeakPtr<NChunkClient::TRefCountedBlocksExt> WeakBlocksExt_;

    // Protected by LifetimeLock_.
    TWeakPtr<NIO::TChunkFileReader> CachedWeakReader_;
    NIO::TChunkFileReaderPtr PreparedReader_;

    NIO::TChunkFileReaderPtr GetReader();

    virtual TFuture<void> PrepareReader(TReaderGuard& readerGuard) override;
    virtual void ReleaseReader(TWriterGuard& writerGuard) override;

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
        int blocksToRead,
        int beginEntryIndex,
        int endEntryIndex,
        TPendingIOGuard&& pendingIOGuard,
        const TErrorOr<std::vector<NChunkClient::TBlock>>& blocksOrError);

    //! Returns `true` if chunk was writen with `sync_on_close` option.
    //! Default value is `true`.
    bool ShouldSyncOnClose();

    //! Artifact chunks are not readable.
    bool IsReadable();
};

DEFINE_REFCOUNTED_TYPE(TBlobChunkBase)

////////////////////////////////////////////////////////////////////////////////

//! A blob chunk owned by TChunkStore.
class TStoredBlobChunk
    : public TBlobChunkBase
{
public:
    TStoredBlobChunk(
        NClusterNode::IBootstrapBase* bootstrap,
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
        NClusterNode::IBootstrapBase* bootstrap,
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
