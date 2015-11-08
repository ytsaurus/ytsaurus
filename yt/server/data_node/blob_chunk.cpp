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
#include <ytlib/chunk_client/block_cache.h>

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
    PROFILE_TIMING("/meta_read_time") {
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

void TBlobChunkBase::DoReadBlockSet(TReadBlockSetSessionPtr session)
{
    auto blockStore = Bootstrap_->GetBlockStore();
    auto config = Bootstrap_->GetConfig()->DataNode;

    // Prepare to serve request: compute pending data size.

    i64 cachedDataSize = 0;
    i64 pendingDataSize = 0;
    int cachedBlockCount = 0;
    int pendingBlockCount = 0;

    for (int index = 0; index < session->Entries.size(); ++index) {
        const auto& entry = session->Entries[index];
        auto blockDataSize = CachedBlocksExt_.blocks(entry.BlockIndex).size();

        if (entry.Cached) {
            cachedDataSize += blockDataSize;
            ++cachedBlockCount;
        } else {
            pendingDataSize += blockDataSize;
            ++pendingBlockCount;
            if (pendingDataSize >= config->MaxBytesPerRead ||
                pendingBlockCount >= config->MaxBlocksPerRead) {
                break;
            }
        }
    }

    int totalBlockCount = cachedBlockCount + pendingBlockCount;
    session->Entries.resize(totalBlockCount);
    session->Blocks.resize(totalBlockCount);

    auto pendingReadSizeGuard = blockStore->IncreasePendingReadSize(pendingDataSize);

    // Serve request.

    auto& locationProfiler = Location_->GetProfiler();
    auto reader = Bootstrap_->GetBlobReaderCache()->GetReader(this);

    int currentIndex = 0;
    while (currentIndex < session->Entries.size()) {
        if (session->Entries[currentIndex].Cached) {
            ++currentIndex;
            continue;
        }

        int beginIndex = currentIndex;
        int endIndex = currentIndex;
        int firstBlockIndex = session->Entries[beginIndex].BlockIndex;

        while (
            endIndex < session->Entries.size() &&
            !session->Entries[endIndex].Cached &&
            session->Entries[endIndex].BlockIndex == firstBlockIndex + (endIndex - beginIndex))
        {
            ++endIndex;
        }

        int blocksToRead = endIndex - beginIndex;

        LOG_DEBUG(
            "Started reading blob chunk blocks (BlockIds: %v:%v-%v, LocationId: %v)",
            Id_,
            beginIndex,
            endIndex - 1,
            Location_->GetId());

        NProfiling::TScopedTimer timer;
        // NB: The reader is synchronous.
        auto blocksOrError = reader->ReadBlocks(firstBlockIndex, blocksToRead).Get();
        auto readTime = timer.GetElapsed();

        if (!blocksOrError.IsOK()) {
            auto error = TError(
                NChunkClient::EErrorCode::IOError,
                "Error reading blob chunk %v",
                Id_) << TError(blocksOrError);
            Location_->Disable(error);
            THROW_ERROR error;
        }

        const auto& blocks = blocksOrError.Value();
        YCHECK(blocks.size() == blocksToRead);

        i64 bytesRead = 0;
        for (int index = beginIndex; index < endIndex; ++index) {
            auto data = blocks[index - beginIndex];
            bytesRead += data.Size();

            auto& entry = session->Entries[index];

            session->Blocks[entry.LocalIndex] = data;

            if (entry.Cookie.IsActive()) {
                struct TCachedBlobChunkBlockTag {};

                // NB: Prevent cache from holding the whole block sequence.
                if (blocks.size() > 1) {
                    data = TSharedRef::MakeCopy<TCachedBlobChunkBlockTag>(data);
                }

                auto cachedBlockId = TBlockId(Id_, entry.BlockIndex);
                auto cachedBlock = New<TCachedBlock>(cachedBlockId, std::move(data), Null);
                entry.Cookie.EndInsert(cachedBlock);
            }
        }

        LOG_DEBUG(
            "Finished reading blob chunk blocks (BlockIds: %v:%v-%v, BytesRead: %v, LocationId: %v)",
            Id_,
            beginIndex,
            endIndex - 1,
            bytesRead,
            Location_->GetId());

        locationProfiler.Enqueue("/blob_block_read_size", bytesRead);
        locationProfiler.Enqueue("/blob_block_read_time", readTime.MicroSeconds());
        locationProfiler.Enqueue("/blob_block_read_throughput", bytesRead * 1000000 / (1 + readTime.MicroSeconds()));
        DataNodeProfiler.Increment(DiskBlobReadByteCounter, bytesRead);

        currentIndex = endIndex;
    }
}

TFuture<std::vector<TSharedRef>> TBlobChunkBase::ReadBlockSet(
    const std::vector<int>& blockIndexes,
    i64 priority,
    bool populateCache,
    IBlockCachePtr blockCache)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto blockStore = Bootstrap_->GetBlockStore();

    bool canServeFromCache = true;

    auto session = New<TReadBlockSetSession>();
    session->Entries.resize(blockIndexes.size());
    session->Blocks.resize(blockIndexes.size());

    std::vector<TFuture<void>> asyncResults;

    for (int localIndex = 0; localIndex < blockIndexes.size(); ++localIndex) {
        auto& entry = session->Entries[localIndex];
        entry.LocalIndex = localIndex;
        entry.BlockIndex = blockIndexes[localIndex];

        auto blockId = TBlockId(Id_, entry.BlockIndex);
        auto block = blockCache->Find(blockId, EBlockType::CompressedData);
        if (block) {
            session->Blocks[entry.LocalIndex] = std::move(block);
            entry.Cached = true;
        } else if (populateCache) {
            entry.Cookie = blockStore->BeginInsertCachedBlock(blockId);
            if (!entry.Cookie.IsActive()) {
                entry.Cached = true;
                auto asyncCachedBlock = entry.Cookie.GetValue().Apply(
                    BIND([session, localIndex] (const TCachedBlockPtr& cachedBlock) {
                        session->Blocks[localIndex] = cachedBlock->GetData();
                    }));
                asyncResults.emplace_back(std::move(asyncCachedBlock));
            }
        }

        if (!entry.Cached) {
            canServeFromCache = false;
        }
    }

    // Fast path: we can serve request right away.
    if (canServeFromCache && asyncResults.empty()) {
        return MakeFuture(std::move(session->Blocks));
    }

    // Slow path: either read data from chunk or wait for cache to be filled.
    if (!canServeFromCache) {
        // Reorder blocks sequentially to improve read performance.
        std::sort(
            session->Entries.begin(),
            session->Entries.end(),
            [] (const TReadBlockSetSession::TBlockEntry& lhs, const TReadBlockSetSession::TBlockEntry& rhs) {
                return lhs.BlockIndex < rhs.BlockIndex;
            });

        auto asyncMeta = GetMeta(priority);
        auto asyncRead = BIND(&TBlobChunkBase::DoReadBlockSet, MakeStrong(this), session)
             .AsyncVia(Location_->GetDataReadInvoker()->BindPriority(priority));

        asyncResults.emplace_back(asyncMeta.Apply(std::move(asyncRead)));
    }

    auto asyncResult = Combine(asyncResults);
    return asyncResult.Apply(BIND([session] () { return std::move(session->Blocks); }));
}

TFuture<std::vector<TSharedRef>> TBlobChunkBase::ReadBlockRange(
    int firstBlockIndex,
    int blockCount,
    i64 priority,
    bool populateCache,
    IBlockCachePtr blockCache)
{
    VERIFY_THREAD_AFFINITY_ANY();

    YCHECK(firstBlockIndex >= 0);
    YCHECK(blockCount >= 0);

    std::vector<int> blockIndexes;
    for (int blockIndex = firstBlockIndex; blockIndex < firstBlockIndex + blockCount; ++blockIndex) {
        blockIndexes.push_back(blockIndex);
    }

    return ReadBlockSet(blockIndexes, priority, populateCache, std::move(blockCache));
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
