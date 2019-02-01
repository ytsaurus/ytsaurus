#include "blob_chunk.h"
#include "private.h"
#include "blob_reader_cache.h"
#include "chunk_block_manager.h"
#include "chunk_cache.h"
#include "location.h"
#include "chunk_meta_manager.h"

#include <yt/server/node/cell_node/bootstrap.h>
#include <yt/server/node/cell_node/config.h>

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/file_reader.h>
#include <yt/ytlib/chunk_client/file_writer.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/misc/memory_zone.h>

namespace NYT::NDataNode {

using namespace NConcurrency;
using namespace NCellNode;
using namespace NNodeTrackerClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TBlobChunkBase::TBlobChunkBase(
    TBootstrap* bootstrap,
    TLocationPtr location,
    const TChunkDescriptor& descriptor,
    TRefCountedChunkMetaPtr meta)
    : TChunkBase(
        bootstrap,
        location,
        descriptor.Id)
{
    Info_.set_disk_space(descriptor.DiskSpace);

    if (meta) {
        const auto& chunkMetaManager = Bootstrap_->GetChunkMetaManager();
        chunkMetaManager->PutCachedMeta(Id_, meta);

        auto blocksExt = New<TRefCountedBlocksExt>(GetProtoExtension<TBlocksExt>(meta->extensions()));
        chunkMetaManager->PutCachedBlocksExt(Id_, blocksExt);
        WeakBlocksExt_ = blocksExt;
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
    const TBlockReadOptions& options,
    const std::optional<std::vector<int>>& extensionTags)
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& chunkMetaManager = Bootstrap_->GetChunkMetaManager();
    auto cookie = chunkMetaManager->BeginInsertCachedMeta(Id_);
    auto result = cookie.GetValue();

    auto priority = options.WorkloadDescriptor.GetPriority();
    try {
        if (cookie.IsActive()) {
            auto readGuard = TChunkReadGuard::AcquireOrThrow(this);

            auto callback = BIND(
                &TBlobChunkBase::DoReadMeta,
                MakeStrong(this),
                Passed(std::move(readGuard)),
                Passed(std::move(cookie)),
                options);

            Bootstrap_
                ->GetChunkBlockManager()->GetReaderInvoker()
                ->Invoke(callback, priority);
        }
    } catch (const std::exception& ex) {
        cookie.Cancel(ex);
    }

    return
        result.Apply(BIND([=] (const TCachedChunkMetaPtr& cachedMeta) {
            return FilterMeta(cachedMeta->GetMeta(), extensionTags);
        })
       .AsyncVia(CreateFixedPriorityInvoker(Bootstrap_->GetChunkBlockManager()->GetReaderInvoker(), priority)));
}

TRefCountedBlocksExtPtr TBlobChunkBase::FindCachedBlocksExt()
{
    TReaderGuard guard(BlocksExtLock_);
    return WeakBlocksExt_.Lock();
}

bool TBlobChunkBase::IsFatalError(const TError& error)
{
    if (error.FindMatching(NChunkClient::EErrorCode::BlockOutOfRange) ||
        error.FindMatching(NYT::EErrorCode::Canceled))
    {
        return false;
    }

    return true;
}

void TBlobChunkBase::DoReadMeta(
    TChunkReadGuard /*readGuard*/,
    TCachedChunkMetaCookie cookie,
    const TBlockReadOptions& options)
{
    YT_LOG_DEBUG("Started reading chunk meta (ChunkId: %v, LocationId: %v, WorkloadDescriptor: %v, ReadSessionId: %v)",
        Id_,
        Location_->GetId(),
        options.WorkloadDescriptor,
        options.ReadSessionId);

    TRefCountedChunkMetaPtr meta;
    TWallTimer readTimer;
    try {
        const auto& readerCache = Bootstrap_->GetBlobReaderCache();
        auto reader = readerCache-> GetReader(this);
        meta = WaitFor(reader->GetMeta(options))
            .ValueOrThrow();
    } catch (const std::exception& ex) {
        cookie.Cancel(ex);
        return;
    }
    auto readTime = readTimer.GetElapsedTime();

    const auto& locationProfiler = Location_->GetProfiler();
    auto& performanceCounters = Location_->GetPerformanceCounters();
    locationProfiler.Update(performanceCounters.BlobChunkMetaReadTime, NProfiling::DurationToValue(readTime));

    YT_LOG_DEBUG("Finished reading chunk meta (ChunkId: %v, LocationId: %v, ReadSessionId: %v, ReadTime: %v)",
        Id_,
        Location_->GetId(),
        options.ReadSessionId,
        readTime);

    const auto& chunkMetaManager = Bootstrap_->GetChunkMetaManager();
    chunkMetaManager->EndInsertCachedMeta(std::move(cookie), Id_, std::move(meta));
}

TFuture<void> TBlobChunkBase::OnBlocksExtLoaded(
    const TReadBlockSetSessionPtr& session,
    const TRefCountedBlocksExtPtr& blocksExt)
{
    // Prepare to serve the request: compute pending data size.
    i64 cachedDataSize = 0;
    i64 pendingDataSize = 0;
    int cachedBlockCount = 0;
    int pendingBlockCount = 0;

    auto config = Bootstrap_->GetConfig()->DataNode;
    for (int index = 0; index < session->Entries.size(); ++index) {
        const auto& entry = session->Entries[index];
        auto blockDataSize = blocksExt->blocks(entry.BlockIndex).size();
        if (entry.Cached) {
            cachedDataSize += blockDataSize;
            ++cachedBlockCount;
        } else {
            pendingDataSize += blockDataSize;
            ++pendingBlockCount;
            if (pendingDataSize >= config->MaxBytesPerRead ||
                pendingBlockCount >= config->MaxBlocksPerRead)
            {
                break;
            }
        }
    }

    int totalBlockCount = cachedBlockCount + pendingBlockCount;
    session->Entries.resize(totalBlockCount);
    session->Blocks.resize(totalBlockCount);

    const auto& outThrottler = Location_->GetOutThrottler(session->Options.WorkloadDescriptor);
    auto throttleFuture = VoidFuture;
    if (!outThrottler->TryAcquire(pendingDataSize)) {
        YT_LOG_DEBUG("Disk read throttling is active (PendingDataSize: %v, WorkloadDescriptor: %v)",
            pendingDataSize,
            session->Options.WorkloadDescriptor);
        throttleFuture = outThrottler->Throttle(pendingDataSize);
    }

    // Actually serve the request: delegate to the appropriate thread.
    return
        throttleFuture.Apply(BIND([=, this_ = MakeStrong(this)] {
            auto pendingIOGuard = Location_->IncreasePendingIOSize(
                EIODirection::Read,
                session->Options.WorkloadDescriptor,
                pendingDataSize);
            // Note that outer Apply checks that the return value is of type
            // TError and returns the TFuture<void> instead of TFuture<TError> here.
            TBlobChunkBase::DoReadBlockSet(
                session,
                std::move(pendingIOGuard));
        }).AsyncVia(CreateFixedPriorityInvoker(
            Bootstrap_->GetChunkBlockManager()->GetReaderInvoker(),
            session->Options.WorkloadDescriptor.GetPriority())));
}

void TBlobChunkBase::DoReadBlockSet(
    const TReadBlockSetSessionPtr& session,
    TPendingIOGuard /*pendingIOGuard*/)
{
    const auto& readerCache = Bootstrap_->GetBlobReaderCache();
    auto reader = readerCache->GetReader(this);

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

        YT_LOG_DEBUG("Started reading blob chunk blocks (BlockIds: %v:%v-%v, LocationId: %v, WorkloadDescriptor: %v, ReadSessionId: %v)",
            Id_,
            firstBlockIndex + beginIndex,
            firstBlockIndex + endIndex - 1,
            Location_->GetId(),
            session->Options.WorkloadDescriptor,
            session->Options.ReadSessionId);

        TWallTimer readTimer;
        auto blocksOrError = WaitFor(reader->ReadBlocks(
            session->Options,
            firstBlockIndex,
            blocksToRead,
            std::nullopt));
        auto readTime = readTimer.GetElapsedTime();

        if (!blocksOrError.IsOK()) {
            auto error = TError(
                NChunkClient::EErrorCode::IOError,
                "Error reading blob chunk %v",
                Id_)
                << TError(blocksOrError);
            if (IsFatalError(blocksOrError)) {
                Location_->Disable(error);
                Y_UNREACHABLE();
            }
            THROW_ERROR error;
        }

        const auto& blocks = blocksOrError.Value();
        YCHECK(blocks.size() == blocksToRead);

        i64 bytesRead = 0;
        TWallTimer populateCacheTimer;
        for (int index = beginIndex; index < endIndex; ++index) {
            auto data = blocks[index - beginIndex];
            bytesRead += data.Size();

            auto& entry = session->Entries[index];

            session->Blocks[entry.LocalIndex] = data;

            if (entry.Cookie.IsActive()) {
                // NB: Copy block to move data to undumpable memory and to
                // prevent cache from holding the whole block sequence.
                {
                    TMemoryZoneGuard memoryZoneGuard(EMemoryZone::Undumpable);
                    data.Data = TSharedRef::MakeCopy<TCachedBlobChunkBlockTag>(data.Data);
                }

                auto blockId = TBlockId(Id_, entry.BlockIndex);
                auto cachedBlock = New<TCachedBlock>(blockId, std::move(data), std::nullopt);
                entry.Cookie.EndInsert(cachedBlock);
            }
        }
        auto populateCacheTime = populateCacheTimer.GetElapsedTime();

        YT_LOG_DEBUG("Finished reading blob chunk blocks (BlockIds: %v:%v-%v, LocationId: %v, BytesRead: %v, "
            "ReadTime: %v, PopulateCacheTime: %v, ReadSessionId: %v)",
            Id_,
            firstBlockIndex + beginIndex,
            firstBlockIndex + endIndex - 1,
            Location_->GetId(),
            bytesRead,
            readTime,
            populateCacheTime,
            session->Options.ReadSessionId);

        const auto& locationProfiler = Location_->GetProfiler();
        auto& performanceCounters = Location_->GetPerformanceCounters();
        locationProfiler.Update(performanceCounters.BlobBlockReadSize, bytesRead);
        locationProfiler.Update(performanceCounters.BlobBlockReadTime, NProfiling::DurationToValue(readTime));
        locationProfiler.Update(performanceCounters.BlobBlockReadThroughput, bytesRead * 1000000 / (1 + readTime.MicroSeconds()));

        Location_->IncreaseCompletedIOSize(EIODirection::Read, session->Options.WorkloadDescriptor, bytesRead);

        currentIndex = endIndex;
    }
}

TFuture<std::vector<TBlock>> TBlobChunkBase::ReadBlockSet(
    const std::vector<int>& blockIndexes,
    const TBlockReadOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto session = New<TReadBlockSetSession>();
    session->Entries.resize(blockIndexes.size());
    session->Blocks.resize(blockIndexes.size());
    session->Options = options;

    bool diskFetchNeeded = false;
    std::vector<TFuture<void>> asyncResults;
    for (int localIndex = 0; localIndex < blockIndexes.size(); ++localIndex) {
        auto& entry = session->Entries[localIndex];
        entry.LocalIndex = localIndex;
        entry.BlockIndex = blockIndexes[localIndex];

        auto blockId = TBlockId(Id_, entry.BlockIndex);
        auto block = options.FetchFromCache && options.BlockCache
            ? options.BlockCache->Find(blockId, EBlockType::CompressedData)
            : TBlock();
        if (block) {
            session->Options.ChunkReaderStatistics->DataBytesReadFromCache += block.Size();
            session->Blocks[entry.LocalIndex] = std::move(block);
            entry.Cached = true;
        } else if (options.FetchFromDisk && options.PopulateCache) {
            const auto& chunkBlockManager = Bootstrap_->GetChunkBlockManager();
            entry.Cookie = chunkBlockManager->BeginInsertCachedBlock(blockId);
            if (!entry.Cookie.IsActive()) {
                entry.Cached = true;
                auto asyncCachedBlock = entry.Cookie.GetValue().Apply(
                    BIND([session, localIndex] (const TCachedBlockPtr& cachedBlock) {
                        auto block = cachedBlock->GetData();
                        session->Options.ChunkReaderStatistics->DataBytesReadFromCache += block.Size();
                        session->Blocks[localIndex] = std::move(block);
                    }));
                asyncResults.emplace_back(std::move(asyncCachedBlock));
            }
        }

        if (!entry.Cached) {
            diskFetchNeeded = true;
        }
    }

    // Fast path: we can serve request right away.
    if (!diskFetchNeeded && asyncResults.empty()) {
        return MakeFuture(std::move(session->Blocks));
    }

    // Slow path: either read data from chunk or wait for the cache to be filled.
    if (options.FetchFromDisk && diskFetchNeeded) {
        // Reorder blocks sequentially to improve read performance.
        std::sort(
            session->Entries.begin(),
            session->Entries.end(),
            [] (const TReadBlockSetSession::TBlockEntry& lhs, const TReadBlockSetSession::TBlockEntry& rhs) {
                return lhs.BlockIndex < rhs.BlockIndex;
            });

        auto blocksExt = FindCachedBlocksExt();
        if (blocksExt) {
            asyncResults.push_back(OnBlocksExtLoaded(session, blocksExt));
        } else {
            const auto& chunkMetaManager = Bootstrap_->GetChunkMetaManager();
            auto cookie = chunkMetaManager->BeginInsertCachedBlocksExt(Id_);
            auto asyncCachedBlocksExt = cookie.GetValue();

            if (cookie.IsActive()) {
                ReadMeta(options)
                    .Subscribe(BIND([=, this_ = MakeStrong(this), cookie = std::move(cookie)] (const TErrorOr<TRefCountedChunkMetaPtr>& result) mutable {
                        if (result.IsOK()) {
                            auto blocksExt = New<TRefCountedBlocksExt>(GetProtoExtension<TBlocksExt>(result.Value()->extensions()));
                            {
                                TWriterGuard guard(BlocksExtLock_);
                                WeakBlocksExt_ = blocksExt;
                            }
                            chunkMetaManager->EndInsertCachedBlocksExt(std::move(cookie), Id_, blocksExt);
                        } else {
                            cookie.Cancel(TError(result));
                        }
                    }));
            }

            asyncResults.push_back(asyncCachedBlocksExt.Apply(BIND([=, this_ = MakeStrong(this)] (const TCachedBlocksExtPtr& cachedBlocksExt) {
                OnBlocksExtLoaded(session, cachedBlocksExt->GetBlocksExt());
            })));
        }
    }

    auto asyncResult = Combine(asyncResults);
    return asyncResult.Apply(BIND([session = std::move(session)] () {
        return std::move(session->Blocks);
    }));
}

TFuture<std::vector<TBlock>> TBlobChunkBase::ReadBlockRange(
    int firstBlockIndex,
    int blockCount,
    const TBlockReadOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    YCHECK(firstBlockIndex >= 0);
    YCHECK(blockCount >= 0);

    std::vector<int> blockIndexes;
    for (int blockIndex = firstBlockIndex; blockIndex < firstBlockIndex + blockCount; ++blockIndex) {
        blockIndexes.push_back(blockIndex);
    }

    return ReadBlockSet(blockIndexes, options);
}

void TBlobChunkBase::SyncRemove(bool force)
{
    const auto& readerCache = Bootstrap_->GetBlobReaderCache();
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
    TRefCountedChunkMetaPtr meta)
    : TBlobChunkBase(
        bootstrap,
        std::move(location),
        descriptor,
        std::move(meta))
{ }

////////////////////////////////////////////////////////////////////////////////

TCachedBlobChunk::TCachedBlobChunk(
    TBootstrap* bootstrap,
    TLocationPtr location,
    const TChunkDescriptor& descriptor,
    TRefCountedChunkMetaPtr meta,
    const TArtifactKey& key,
    TClosure destroyed)
    : TBlobChunkBase(
        bootstrap,
        std::move(location),
        descriptor,
        std::move(meta))
    , TAsyncCacheValueBase<TArtifactKey, TCachedBlobChunk>(key)
    , Destroyed_(destroyed)
{ }

TCachedBlobChunk::~TCachedBlobChunk()
{
    Destroyed_.Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
