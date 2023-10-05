#pragma once

#include "artifact.h"
#include "chunk_detail.h"
#include "chunk_meta_manager.h"
#include "location.h"

#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/block_cache.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_info.pb.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/misc/async_slru_cache.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! A base for both TStoredBlobChunk and TCachedBlobChunk.
class TBlobChunkBase
    : public TChunkBase
{
public:
    NChunkClient::NProto::TChunkInfo GetInfo() const override;

    bool IsActive() const override;

    TFuture<NChunkClient::TRefCountedChunkMetaPtr> ReadMeta(
        const TChunkReadOptions& options,
        const std::optional<std::vector<int>>& extensionTags = std::nullopt) override;

    TFuture<std::vector<NChunkClient::TBlock>> ReadBlockSet(
        const std::vector<int>& blockIndexes,
        const TChunkReadOptions& options) override;

    TFuture<std::vector<NChunkClient::TBlock>> ReadBlockRange(
        int firstBlockIndex,
        int blockCount,
        const TChunkReadOptions& options) override;

    TFuture<void> PrepareToReadChunkFragments(
        const NChunkClient::TClientChunkReadOptions& options,
        bool useDirectIO) override;

    NIO::IIOEngine::TReadRequest MakeChunkFragmentReadRequest(
        const NIO::TChunkFragmentDescriptor& fragmentDescriptor,
        bool useDirectIO) override;

    void SyncRemove(bool force) override;

    NIO::TBlocksExtPtr FindCachedBlocksExt();

protected:
    TBlobChunkBase(
        TChunkContextPtr context,
        TChunkLocationPtr location,
        const TChunkDescriptor& descriptor,
        NChunkClient::TRefCountedChunkMetaPtr meta);

    TFuture<void> AsyncRemove() override;

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
        std::vector<TFuture<void>> Futures;
        TPromise<std::vector<NChunkClient::TBlock>> SessionPromise = NewPromise<std::vector<NChunkClient::TBlock>>();
        TPromise<void> DiskFetchPromise;
        NIO::TBlocksExtPtr BlocksExt;
        TPendingIOGuard PendingIOGuard;
        std::atomic<bool> Finished = false;
    };

    using TReadBlockSetSessionPtr = TIntrusivePtr<TReadBlockSetSession>;

    NChunkClient::NProto::TChunkInfo Info_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, BlocksExtLock_);
    TWeakPtr<NIO::TBlocksExt> WeakBlocksExt_;

    // Protected by LifetimeLock_.
    TWeakPtr<NIO::TChunkFileReader> CachedWeakReader_;
    NIO::TChunkFileReaderPtr PreparedReader_;

    NIO::TChunkFileReaderPtr GetReader();
    void ReleaseReader(NThreading::TWriterGuard<NThreading::TReaderWriterSpinLock>& writerGuard) override;

    TSharedRef WrapBlockWithDelayedReferenceHolder(TSharedRef&& rawReference, TDuration delayBeforeFree);

    void CompleteSession(const TReadBlockSetSessionPtr& session);
    static void FailSession(const TReadBlockSetSessionPtr& session, const TError& error);

    void DoReadMeta(
        const TReadMetaSessionPtr& session,
        TCachedChunkMetaCookie cookie);
    void OnBlocksExtLoaded(
        const TReadBlockSetSessionPtr& session,
        const NIO::TBlocksExtPtr& blocksExt);

    void DoReadSession(
        const TReadBlockSetSessionPtr& session,
        i64 pendingDataSize);
    void DoReadBlockSet(
        const TReadBlockSetSessionPtr& session);
    void OnBlocksRead(
        const TReadBlockSetSessionPtr& session,
        int firstBlockIndex,
        int blocksToRead,
        int beginEntryIndex,
        int endEntryIndex,
        const TErrorOr<std::vector<NChunkClient::TBlock>>& blocksOrError);

    //! Returns `true` if chunk was written with `sync_on_close` option.
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
        TChunkContextPtr context,
        TChunkLocationPtr location,
        const TChunkDescriptor& descriptor,
        NChunkClient::TRefCountedChunkMetaPtr meta = nullptr);
};

DEFINE_REFCOUNTED_TYPE(TStoredBlobChunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
