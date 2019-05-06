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

    auto session = New<TReadMetaSession>();
    session->Options = options;

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
        return MakeFuture<TRefCountedChunkMetaPtr>(ex);
    }

    return
        result.Apply(BIND([=, this_ = MakeStrong(this), session = std::move(session)] (const TCachedChunkMetaPtr& cachedMeta) {
            ProfileReadMetaLatency(session);
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

void TBlobChunkBase::CompleteSession(const TReadBlockSetSessionPtr& session)
{
    ProfileReadBlockSetLatency(session);

    std::vector<TBlock> blocks;
    blocks.reserve(session->EntryCount);
    for (int entryIndex = 0; entryIndex < session->EntryCount; ++entryIndex) {
        auto& entry = session->Entries[entryIndex];
        blocks.push_back(std::move(entry.Block));
    }
    session->Promise.TrySet(std::move(blocks));
}

void TBlobChunkBase::FailSession(const TReadBlockSetSessionPtr& session, const TError& error)
{
    session->Promise.TrySet(error);
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
        auto reader = readerCache->GetReader(this);
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
    chunkMetaManager->EndInsertCachedMeta(std::move(cookie), std::move(meta));
}

void TBlobChunkBase::OnBlocksExtLoaded(
    const TReadBlockSetSessionPtr& session,
    const TRefCountedBlocksExtPtr& blocksExt)
{
    // Run async cache lookup.
    i64 pendingDataSize = 0;
    int pendingBlockCount = 0;
    bool diskFetchNeeded = false;
    const auto& config = Bootstrap_->GetConfig()->DataNode;
    for (int entryIndex = 0; entryIndex < session->EntryCount; ++entryIndex) {
        auto& entry = session->Entries[entryIndex];
        if (entry.Cached) {
            continue;
        }

        if (session->Options.PopulateCache) {
            const auto& chunkBlockManager = Bootstrap_->GetChunkBlockManager();
            auto blockId = TBlockId(Id_, entry.BlockIndex);
            entry.Cookie = chunkBlockManager->BeginInsertCachedBlock(blockId);
            if (!entry.Cookie.IsActive()) {
                entry.Cached = true;
                session->AsyncResults.push_back(entry.Cookie.GetValue().Apply(
                    BIND([session, entryIndex] (const TCachedBlockPtr& cachedBlock) {
                        auto block = cachedBlock->GetData();
                        session->Options.ChunkReaderStatistics->DataBytesReadFromCache += block.Size();
                        session->Entries[entryIndex].Block = std::move(block);
                    })));
                continue;
            }
        }

        diskFetchNeeded = true;
        pendingDataSize += blocksExt->blocks(entry.BlockIndex).size();
        pendingBlockCount += 1;
        if (pendingDataSize >= config->MaxBytesPerRead ||
            pendingBlockCount >= config->MaxBlocksPerRead) {
            session->EntryCount = entryIndex;
            YT_LOG_DEBUG("Read session trimmed (PendingDataSize: %v, PendingBlockCount: %v, TrimmedBlockCount: %v)",
                pendingDataSize,
                pendingBlockCount,
                session->EntryCount);
            break;
        }
    }

    if (diskFetchNeeded) {
        const auto& outThrottler = Location_->GetOutThrottler(session->Options.WorkloadDescriptor);
        auto throttleAsyncResult = VoidFuture;
        if (!outThrottler->TryAcquire(pendingDataSize)) {
            YT_LOG_DEBUG("Disk read throttling is active (PendingDataSize: %v, WorkloadDescriptor: %v)",
                pendingDataSize,
                session->Options.WorkloadDescriptor);
            throttleAsyncResult = outThrottler->Throttle(pendingDataSize);
        }

        // Actually serve the request: delegate to the appropriate thread.
        session->AsyncResults.push_back(
            throttleAsyncResult.Apply(BIND([=, this_ = MakeStrong(this)] {
                auto pendingIOGuard = Location_->IncreasePendingIOSize(
                    EIODirection::Read,
                    session->Options.WorkloadDescriptor,
                    pendingDataSize);
                // Note that outer Apply checks that the return value is of type
                // TError and returns the TFuture<void> instead of TFuture<TError> here.
                DoReadBlockSet(
                    session,
                    std::move(pendingIOGuard));
            }).AsyncVia(CreateFixedPriorityInvoker(
                Bootstrap_->GetChunkBlockManager()->GetReaderInvoker(),
                session->Options.WorkloadDescriptor.GetPriority()))));
    }

    Combine(session->AsyncResults)
        .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TError& error) {
            if (error.IsOK()) {
                CompleteSession(session);
            } else {
                FailSession(session, error);
            }
        }));

    session->Promise.OnCanceled(BIND([session] {
        TError error("Read session canceled");
        for (int entryIndex = 0; entryIndex < session->EntryCount; ++entryIndex) {
            auto& entry = session->Entries[entryIndex];
            if (!entry.Cached && !entry.Latch.test_and_set()) {
                entry.Cookie.Cancel(error);
            }
        }
        for (auto& asyncResult : session->AsyncResults) {
            asyncResult.Cancel();
        }
        FailSession(session, error);
    }));
}

void TBlobChunkBase::DoReadBlockSet(
    const TReadBlockSetSessionPtr& session,
    TPendingIOGuard /*pendingIOGuard*/)
{
    const auto& readerCache = Bootstrap_->GetBlobReaderCache();
    auto reader = readerCache->GetReader(this);

    int currentEntryIndex = 0;
    while (currentEntryIndex < session->EntryCount) {
        if (session->Entries[currentEntryIndex].Cached) {
            ++currentEntryIndex;
            continue;
        }

        // Extract the maximum contiguous run of blocks.
        int beginEntryIndex = currentEntryIndex;
        int endEntryIndex = currentEntryIndex;
        int firstBlockIndex = session->Entries[beginEntryIndex].BlockIndex;
        while (
            endEntryIndex < session->EntryCount &&
            !session->Entries[endEntryIndex].Cached &&
            session->Entries[endEntryIndex].BlockIndex == firstBlockIndex + (endEntryIndex - beginEntryIndex))
        {
            ++endEntryIndex;
        }
        int blocksToRead = endEntryIndex - beginEntryIndex;

        YT_LOG_DEBUG("Started reading blob chunk blocks (BlockIds: %v:%v-%v, LocationId: %v, WorkloadDescriptor: %v, ReadSessionId: %v)",
            Id_,
            firstBlockIndex,
            firstBlockIndex + blocksToRead - 1,
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
        for (int entryIndex = beginEntryIndex; entryIndex < endEntryIndex; ++entryIndex) {
            auto block = blocks[entryIndex - beginEntryIndex];
            auto& entry = session->Entries[entryIndex];
            entry.Block = block;
            bytesRead += block.Size();
            if (!entry.Latch.test_and_set() && entry.Cookie.IsActive()) {
                // NB: Copy block to move data to undumpable memory and to
                // prevent cache from holding the whole block sequence.
                {
                    TMemoryZoneGuard memoryZoneGuard(EMemoryZone::Undumpable);
                    block.Data = TSharedRef::MakeCopy<TCachedBlobChunkBlockTag>(block.Data);
                }

                auto blockId = TBlockId(Id_, entry.BlockIndex);
                auto cachedBlock = New<TCachedBlock>(blockId, std::move(block), std::nullopt);
                entry.Cookie.EndInsert(cachedBlock);
            }
        }
        auto populateCacheTime = populateCacheTimer.GetElapsedTime();

        YT_LOG_DEBUG("Finished reading blob chunk blocks (BlockIds: %v:%v-%v, LocationId: %v, BytesRead: %v, "
            "ReadTime: %v, PopulateCacheTime: %v, ReadSessionId: %v)",
            Id_,
            firstBlockIndex,
            firstBlockIndex + blocksToRead - 1,
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
        locationProfiler.Increment(performanceCounters.BlobBlockReadBytes, bytesRead);

        Location_->IncreaseCompletedIOSize(EIODirection::Read, session->Options.WorkloadDescriptor, bytesRead);

        currentEntryIndex = endEntryIndex;
    }
}

void TBlobChunkBase::ProfileReadBlockSetLatency(const TReadBlockSetSessionPtr& session)
{
    const auto& locationProfiler = Location_->GetProfiler();
    auto& performanceCounters = Location_->GetPerformanceCounters();
    locationProfiler.Update(
        performanceCounters.BlobBlockReadLatencies[session->Options.WorkloadDescriptor.Category],
        session->Timer.GetElapsedValue());
}

void TBlobChunkBase::ProfileReadMetaLatency(const TReadMetaSessionPtr& session)
{
    const auto& locationProfiler = Location_->GetProfiler();
    auto& performanceCounters = Location_->GetPerformanceCounters();
    locationProfiler.Update(
        performanceCounters.BlobBlockReadLatencies[session->Options.WorkloadDescriptor.Category],
        session->Timer.GetElapsedValue());
}

TFuture<std::vector<TBlock>> TBlobChunkBase::ReadBlockSet(
    const std::vector<int>& blockIndexes,
    const TBlockReadOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    // Initialize session.
    auto session = New<TReadBlockSetSession>();
    session->Options = options;
    session->EntryCount = static_cast<int>(blockIndexes.size());
    session->Entries.reset(new TReadBlockSetSession::TBlockEntry[session->EntryCount]);
    for (int entryIndex = 0; entryIndex < session->EntryCount; ++entryIndex) {
        auto& entry = session->Entries[entryIndex];
        entry.BlockIndex = blockIndexes[entryIndex];
    }

    // Run sync cache lookup.
    bool allCached = true;
    if (options.FetchFromCache && options.BlockCache) {
        for (int entryIndex = 0; entryIndex < static_cast<int>(blockIndexes.size()); ++entryIndex) {
            auto& entry = session->Entries[entryIndex];
            auto blockId = TBlockId(Id_, entry.BlockIndex);
            auto block = options.BlockCache->Find(blockId, EBlockType::CompressedData);
            if (block) {
                session->Options.ChunkReaderStatistics->DataBytesReadFromCache += block.Size();
                entry.Block = std::move(block);
                entry.Cached = true;
            } else {
                allCached = false;
            }
        }
    } else {
        allCached = false;
    }

    // Check for fast path.
    if (allCached || !options.FetchFromDisk) {
        CompleteSession(session);
        return session->Promise.ToFuture();
    }

    // Need blocks ext.
    auto blocksExt = FindCachedBlocksExt();
    if (blocksExt) {
        OnBlocksExtLoaded(session, blocksExt);
    } else {
        const auto& chunkMetaManager = Bootstrap_->GetChunkMetaManager();
        auto cookie = chunkMetaManager->BeginInsertCachedBlocksExt(Id_);
        auto asyncBlocksExt = cookie.GetValue();
        if (cookie.IsActive()) {
            ReadMeta(options)
                .Subscribe(BIND([=, this_ = MakeStrong(this), cookie = std::move(cookie)] (const TErrorOr<TRefCountedChunkMetaPtr>& result) mutable {
                    if (result.IsOK()) {
                        auto blocksExt = New<TRefCountedBlocksExt>(GetProtoExtension<TBlocksExt>(result.Value()->extensions()));
                        {
                            TWriterGuard guard(BlocksExtLock_);
                            WeakBlocksExt_ = blocksExt;
                        }
                        chunkMetaManager->EndInsertCachedBlocksExt(std::move(cookie), blocksExt);
                    } else {
                        cookie.Cancel(TError(result));
                    }
                }));
        }
        asyncBlocksExt.Subscribe(
            BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TCachedBlocksExtPtr>& cachedBlocksExtOrError) {
                if (cachedBlocksExtOrError.IsOK()) {
                    const auto& cachedBlocksExt = cachedBlocksExtOrError.Value();
                    OnBlocksExtLoaded(session, cachedBlocksExt->GetBlocksExt());
                } else {
                    FailSession(session, cachedBlocksExtOrError);
                }
            }));
    }

    return session->Promise.ToFuture();
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
