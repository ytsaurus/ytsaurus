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
    MemoryTrackerGuard_ = TNodeMemoryTrackerGuard::Acquire(
        tracker,
        EMemoryCategory::ChunkMeta,
        CachedMeta_->SpaceUsed());

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

TFuture<std::vector<TSharedRef>> TBlobChunkBase::ReadBlockSet(
    const std::vector<int>& blockIndexes,
    i64 priority,
    bool populateCache,
    IBlockCachePtr blockCache)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return GetMeta(priority).Apply(BIND([=, this_ = MakeStrong(this)] () mutable -> TFuture<std::vector<TSharedRef>> {
        i64 totalDataSize = 0;
        int blockCount = 0;
        auto config = Bootstrap_->GetConfig()->DataNode;
        for (auto it = blockIndexes.begin(); it != blockIndexes.end(); ++it) {
            totalDataSize += CachedBlocksExt_.blocks(*it).size();
            blockCount += 1;
            // Trim blockIndexes to obey data size and block count limits.
            if (totalDataSize > config->MaxBytesPerRead || blockCount >= config->MaxBlocksPerRead) {
                break;
            }
        }

        auto blockStore = Bootstrap_->GetBlockStore();

        auto session = New<TReadBlockSetSession>();
        session->Entries.resize(blockCount);
        i64 pendingDataSize = 0;
        for (int localIndex = 0; localIndex < blockCount; ++localIndex) {
            auto& entry = session->Entries[localIndex];
            entry.BlockIndex = blockIndexes[localIndex];
            auto blockId = TBlockId(Id_, entry.BlockIndex);
            auto block = blockCache->Find(blockId, EBlockType::CompressedData);
            if (block) {
                entry.Data = block;
                entry.Cached = true;
            } else if (populateCache) {
                entry.Cookie = blockStore->BeginInsertCachedBlock(blockId);
                if (!entry.Cookie.IsActive()) {
                    entry.Cached = true;
                    auto asyncResult = entry.Cookie.GetValue().Apply(
                        BIND([session, &entry] (const TCachedBlockPtr& cachedBlock) {
                            entry.Data = cachedBlock->GetData();
                        }));
                    session->CacheResults.push_back(asyncResult);
                }
            }
            if (!entry.Data) {
                pendingDataSize += CachedBlocksExt_.blocks(entry.BlockIndex).size();
            }
        }

        auto pendingReadSizeGuard = blockStore->IncreasePendingReadSize(pendingDataSize);

        auto callback = BIND(
            &TBlobChunkBase::DoReadBlockSet,
            MakeStrong(this),
            session,
            Passed(std::move(pendingReadSizeGuard)));

        Location_
            ->GetDataReadInvoker()
            ->Invoke(callback, priority);

        return session->Promise.ToFuture();
    }));
}

void TBlobChunkBase::DoReadBlockSet(
    TReadBlockSetSessionPtr session,
    TPendingReadSizeGuard pendingReadSizeGuard)
{
    try {
        auto& locationProfiler = Location_->GetProfiler();
        auto readerCache = Bootstrap_->GetBlobReaderCache();
        auto reader = readerCache->GetReader(this);

        int currentLocalIndex = 0;
        while (currentLocalIndex < session->Entries.size()) {
            if (session->Entries[currentLocalIndex].Cached) {
                ++currentLocalIndex;
                continue;
            }

            int startLocalIndex = currentLocalIndex;
            int startBlockIndex = session->Entries[startLocalIndex].BlockIndex;
            int endLocalIndex = startLocalIndex;
            while (endLocalIndex < session->Entries.size() &&
                   !session->Entries[endLocalIndex].Cached &&
                   session->Entries[endLocalIndex].BlockIndex == startBlockIndex + (endLocalIndex - startLocalIndex))
            {
                ++endLocalIndex;
            }

            int blocksToRead = endLocalIndex - startLocalIndex;

            LOG_DEBUG("Started reading blob chunk blocks (BlockIds: %v:%v-%v, LocationId: %v)",
                Id_,
                startBlockIndex,
                startBlockIndex + blocksToRead - 1,
                Location_->GetId());

            NProfiling::TScopedTimer timer;
            // NB: The reader is synchronous.
            auto blocksOrError = reader->ReadBlocks(startBlockIndex, blocksToRead)
                .Get();
            auto readTime = timer.GetElapsed();

            if (!blocksOrError.IsOK()) {
                auto error = TError(
                    NChunkClient::EErrorCode::IOError,
                    "Error reading blob chunk %v",
                    Id_)
                    << TError(blocksOrError);
                Location_->Disable(error);
                YUNREACHABLE(); // Disable() exits the process.
            }

            const auto& blocks = blocksOrError.Value();
            YCHECK(blocks.size() == blocksToRead);

            i64 bytesRead = 0;
            for (int localIndex = currentLocalIndex; localIndex < currentLocalIndex + blocksToRead; ++localIndex) {
                auto& entry = session->Entries[localIndex];
                YCHECK(!entry.Cached);
                YCHECK(!entry.Data);
                entry.Data = blocks[localIndex - currentLocalIndex];
                if (entry.Cookie.IsActive()) {
                    auto blockId = TBlockId(Id_, entry.BlockIndex);
                    struct TCachedBlobChunkBlockTag { };
                    auto cachedData = entry.Data;
                    // NB: Prevent cache from holding the whole block sequence.
                    if (blocks.size() > 1) {
                        cachedData = TSharedRef::MakeCopy<TCachedBlobChunkBlockTag>(cachedData);
                    }
                    auto cachedBlock = New<TCachedBlock>(blockId, cachedData, Null);
                    entry.Cookie.EndInsert(cachedBlock);
                }
                bytesRead += entry.Data.Size();
            }

            LOG_DEBUG("Finished reading blob chunk blocks (BlockIds: %v:%v-%v, BytesRead: %v, LocationId: %v)",
                Id_,
                startBlockIndex,
                startBlockIndex + blocksToRead - 1,
                bytesRead,
                Location_->GetId());

            locationProfiler.Enqueue("/blob_block_read_size", bytesRead);
            locationProfiler.Enqueue("/blob_block_read_time", readTime.MicroSeconds());
            locationProfiler.Enqueue("/blob_block_read_throughput", bytesRead * 1000000 / (1 + readTime.MicroSeconds()));
            DataNodeProfiler.Increment(DiskBlobReadByteCounter, bytesRead);

            currentLocalIndex = endLocalIndex;
        }

        if (!session->CacheResults.empty()) {
            WaitFor(Combine(session->CacheResults))
                .ThrowOnError();
        }

        std::vector<TSharedRef> blocks;
        for (auto& entry : session->Entries) {
            YCHECK(entry.Data);
            blocks.push_back(entry.Data);
        }

        session->Promise.Set(std::move(blocks));
    } catch (const std::exception& ex) {
        session->Promise.Set(TError(ex));
    }
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

    return ReadBlockSet(
        blockIndexes,
        priority,
        populateCache,
        blockCache);
}

void TBlobChunkBase::SyncRemove(bool force)
{
    auto readerCache = Bootstrap_->GetBlobReaderCache();
    readerCache->EvictReader(this);

    Location_->RemoveChunkFiles(Id_, force);
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
    const TArtifactKey& key,
    TClosure destroyed)
    : TBlobChunkBase(
        bootstrap,
        location,
        descriptor,
        meta)
    , TAsyncCacheValueBase<TArtifactKey, TCachedBlobChunk>(key)
    , Destroyed_(destroyed)
{ }

TCachedBlobChunk::~TCachedBlobChunk()
{
    Destroyed_.Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
