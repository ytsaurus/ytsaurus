#include "stdafx.h"
#include "blob_chunk.h"
#include "private.h"
#include "location.h"
#include "blob_reader_cache.h"
#include "chunk_cache.h"
#include "block_store.h"

#include <core/profiling/scoped_timer.h>

#include <core/concurrency/thread_affinity.h>

#include <ytlib/chunk_client/file_reader.h>
#include <ytlib/chunk_client/file_writer.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <server/misc/memory_usage_tracker.h>

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>

namespace NYT {
namespace NDataNode {

using namespace NConcurrency;
using namespace NCellNode;
using namespace NNodeTrackerClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

static NProfiling::TSimpleCounter DiskBlobReadByteCounter("/blob_block_read_bytes");

////////////////////////////////////////////////////////////////////////////////

TBlobChunkBase::TBlobChunkBase(
    TBootstrap* bootstrap,
    TLocationPtr location,
    const TChunkDescriptor& descriptor,
    const TChunkMeta* meta)
    : TChunkBase(
        bootstrap,
        location,
        descriptor.Id)
{
    Info_.set_disk_space(descriptor.DiskSpace);
    if (meta) {
        SetMetaLoadSuccess(*meta);
    }
}

TBlobChunkBase::~TBlobChunkBase()
{
    if (CachedMeta_) {
        auto* tracker = Bootstrap_->GetMemoryUsageTracker();
        tracker->Release(EMemoryCategory::ChunkMeta, CachedMeta_->SpaceUsed());
    }
}

TChunkInfo TBlobChunkBase::GetInfo() const
{
    return Info_;
}

bool TBlobChunkBase::IsActive() const
{
    return false;
}

TFuture<TRefCountedChunkMetaPtr> TBlobChunkBase::ReadMeta(
    i64 priority,
    const TNullable<std::vector<int>>& extensionTags)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return GetMeta(priority).Apply(BIND([=] () {
        return FilterMeta(CachedMeta_, extensionTags);
    }));
}

TFuture<void> TBlobChunkBase::GetMeta(i64 priority)
{
    VERIFY_THREAD_AFFINITY_ANY();

    {
        TReaderGuard guard(CachedMetaLock_);
        if (CachedMetaPromise_) {
            return CachedMetaPromise_.ToFuture();
        }
    }

    auto readGuard = TChunkReadGuard::TryAcquire(this);
    if (!readGuard) {
        return MakeFuture(
            TError("Cannot read meta of chunk %v: chunk is scheduled for removal",
                Id_));
    }

    {
        TWriterGuard guard(CachedMetaLock_);
        if (CachedMetaPromise_) {
            return CachedMetaPromise_.ToFuture();
        }
        CachedMetaPromise_ = NewPromise<void>();
    }

    auto callback = BIND(
        &TBlobChunkBase::DoReadMeta,
        MakeStrong(this),
        Passed(std::move(readGuard)));

    Location_
        ->GetMetaReadInvoker()
        ->Invoke(callback, priority);

    return CachedMetaPromise_.ToFuture();
}


void TBlobChunkBase::SetMetaLoadSuccess(const NChunkClient::NProto::TChunkMeta& meta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TWriterGuard guard(CachedMetaLock_);

    CachedBlocksExt_ = GetProtoExtension<TBlocksExt>(meta.extensions());
    CachedMeta_ = New<TRefCountedChunkMeta>(meta);

    auto* tracker = Bootstrap_->GetMemoryUsageTracker();
    tracker->Acquire(EMemoryCategory::ChunkMeta, CachedMeta_->SpaceUsed());

    if (!CachedMetaPromise_) {
        CachedMetaPromise_ = NewPromise<void>();
    }

    auto promise = CachedMetaPromise_;

    guard.Release();

    promise.Set();
}

void TBlobChunkBase::SetMetaLoadError(const TError& error)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YCHECK(!error.IsOK());

    TWriterGuard guard(CachedMetaLock_);

    auto promise = CachedMetaPromise_;

    guard.Release();

    promise.Set(error);
}

void TBlobChunkBase::DoReadMeta(TChunkReadGuard /*readGuard*/)
{
    const auto& Profiler = Location_->GetProfiler();
    LOG_DEBUG("Started reading chunk meta (LocationId: %v, ChunkId: %v)",
        Location_->GetId(),
        Id_);

    NChunkClient::TFileReaderPtr reader;
    PROFILE_TIMING ("/meta_read_time") {
        auto readerCache = Bootstrap_->GetBlobReaderCache();
        try {
            reader = readerCache->GetReader(this);
        } catch (const std::exception& ex) {
            SetMetaLoadError(TError(ex));
            return;
        }
    }

    LOG_DEBUG("Finished reading chunk meta (LocationId: %v, ChunkId: %v)",
        Location_->GetId(),
        Id_);

    SetMetaLoadSuccess(reader->GetMeta());
}

TFuture<std::vector<TSharedRef>> TBlobChunkBase::ReadBlocks(
    int firstBlockIndex,
    int blockCount,
    i64 priority)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YCHECK(firstBlockIndex >= 0);
    YCHECK(blockCount >= 0);

    return GetMeta(priority).Apply(BIND([=, this_ = MakeStrong(this)] () -> TFuture<std::vector<TSharedRef>> {
        i64 dataSize = 0;
        for (int blockIndex = firstBlockIndex; blockIndex < firstBlockIndex + blockCount; ++blockIndex) {
            dataSize += CachedBlocksExt_.blocks(blockIndex).size();
        }

        auto blockStore = Bootstrap_->GetBlockStore();
        auto pendingReadSizeGuard = blockStore->IncreasePendingReadSize(dataSize);

        auto promise = NewPromise<std::vector<TSharedRef>>();

        auto callback = BIND(
            &TBlobChunkBase::DoReadBlocks,
            MakeStrong(this),
            firstBlockIndex,
            blockCount,
            Passed(std::move(pendingReadSizeGuard)),
            promise);

        Location_
            ->GetDataReadInvoker()
            ->Invoke(callback, priority);

        return promise.ToFuture();
    }));
}

void TBlobChunkBase::DoReadBlocks(
    int firstBlockIndex,
    int blockCount,
    TPendingReadSizeGuard pendingReadSizeGuard,
    TPromise<std::vector<TSharedRef>> promise)
{
    auto blockStore = Bootstrap_->GetBlockStore();
    auto readerCache = Bootstrap_->GetBlobReaderCache();

    try {
        auto reader = readerCache->GetReader(this);

        LOG_DEBUG("Started reading blob chunk blocks (BlockIds: %v:%v-%v, LocationId: %v)",
            Id_,
            firstBlockIndex,
            firstBlockIndex + blockCount - 1,
            Location_->GetId());
            
        NProfiling::TScopedTimer timer;

        // NB: The reader is synchronous.
        auto blocksOrError = reader->ReadBlocks(firstBlockIndex, blockCount).Get();

        auto readTime = timer.GetElapsed();

        LOG_DEBUG("Finished reading blob chunk blocks (BlockIds: %v:%v-%v, LocationId: %v)",
            Id_,
            firstBlockIndex,
            firstBlockIndex + blockCount - 1,
            Location_->GetId());

        if (!blocksOrError.IsOK()) {
            auto error = TError(
                NChunkClient::EErrorCode::IOError,
                "Error reading blob chunk %v",
                Id_)
                << TError(blocksOrError);
            Location_->Disable(error);
            THROW_ERROR error;
        }

        auto& locationProfiler = Location_->GetProfiler();
        i64 pendingSize = pendingReadSizeGuard.GetSize();
        locationProfiler.Enqueue("/blob_block_read_size", pendingSize);
        locationProfiler.Enqueue("/blob_block_read_time", readTime.MicroSeconds());
        locationProfiler.Enqueue("/blob_block_read_throughput", pendingSize * 1000000 / (1 + readTime.MicroSeconds()));
        DataNodeProfiler.Increment(DiskBlobReadByteCounter, pendingSize);

        promise.Set(blocksOrError.Value());
    } catch (const std::exception& ex) {
        promise.Set(TError(ex));
    }
}

void TBlobChunkBase::SyncRemove(bool force)
{
    auto readerCache = Bootstrap_->GetBlobReaderCache();
    readerCache->EvictReader(this);

    if (force) {
        Location_->RemoveChunkFiles(Id_);
    } else {
        Location_->MoveChunkFilesToTrash(Id_);
    }
}

TFuture<void> TBlobChunkBase::AsyncRemove()
{
    return BIND(&TBlobChunkBase::SyncRemove, MakeStrong(this), false)
        .AsyncVia(Location_->GetWritePoolInvoker())
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

TStoredBlobChunk::TStoredBlobChunk(
    TBootstrap* bootstrap,
    TLocationPtr location,
    const TChunkDescriptor& descriptor,
    const TChunkMeta* meta)
    : TBlobChunkBase(
        bootstrap,
        location,
        descriptor,
        meta)
{ }

////////////////////////////////////////////////////////////////////////////////

TCachedBlobChunk::TCachedBlobChunk(
    TBootstrap* bootstrap,
    TLocationPtr location,
    const TChunkDescriptor& descriptor,
    const TChunkMeta* meta,
    TClosure destroyed)
    : TBlobChunkBase(
        bootstrap,
        location,
        descriptor,
        meta)
    , TAsyncCacheValueBase<TChunkId, TCachedBlobChunk>(GetId())
    , Destroyed_(destroyed)
{ }

TCachedBlobChunk::~TCachedBlobChunk()
{
    Destroyed_.Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
