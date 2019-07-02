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
                ->GetStorageHeavyInvoker()
                ->Invoke(callback, options.WorkloadDescriptor.GetPriority());
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
       .AsyncVia(Bootstrap_->GetStorageHeavyInvoker()));
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

NChunkClient::TFileReaderPtr TBlobChunkBase::GetReader()
{
    {
        auto guard = Guard(CachedReaderSpinLock_);
        auto reader = CachedWeakReader_.Lock();
        if (reader) {
            return reader;
        }
    }

    {
        const auto& readerCache = Bootstrap_->GetBlobReaderCache();
        auto reader = readerCache->GetReader(this);
        auto guard = Guard(CachedReaderSpinLock_);
        CachedWeakReader_ = reader;
        return reader;
    }
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
    session->SessionPromise.TrySet(std::move(blocks));
}

void TBlobChunkBase::FailSession(const TReadBlockSetSessionPtr& session, const TError& error)
{
    for (int entryIndex = 0; entryIndex < session->EntryCount; ++entryIndex) {
        auto& entry = session->Entries[entryIndex];
        if (!entry.Cached) {
            entry.Cookie.Cancel(error);
        }
    }

    for (auto& asyncResult : session->AsyncResults) {
        asyncResult.Cancel();
    }

    session->SessionPromise.TrySet(error);
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
        auto reader = GetReader();
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
            session->EntryCount = entryIndex + 1;
            YT_LOG_DEBUG("Read session trimmed (PendingDataSize: %v, PendingBlockCount: %v, TrimmedBlockCount: %v)",
                pendingDataSize,
                pendingBlockCount,
                session->EntryCount);
            break;
        }
    }

    if (diskFetchNeeded) {
        session->DiskFetchPromise = NewPromise<void>();
        session->AsyncResults.push_back(session->DiskFetchPromise.ToFuture());

        auto readCallback = BIND([=, this_ = MakeStrong(this)] {
            DoReadSession(session, pendingDataSize);
        });

        const auto& outThrottler = Location_->GetOutThrottler(session->Options.WorkloadDescriptor);
        if (outThrottler->TryAcquire(pendingDataSize)) {
            session->Invoker->Invoke(std::move(readCallback));
        } else {
            YT_LOG_DEBUG("Disk read throttling is active (PendingDataSize: %v, WorkloadDescriptor: %v)",
                pendingDataSize,
                session->Options.WorkloadDescriptor);
            auto throttleAsyncResult = outThrottler->Throttle(pendingDataSize);
            session->AsyncResults.push_back(throttleAsyncResult.Apply(readCallback.AsyncVia(session->Invoker)));
        }
    }

    Combine(session->AsyncResults)
        .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TError& error) {
            if (error.IsOK()) {
                CompleteSession(session);
            } else {
                FailSession(session, error);
            }
        }));

    session->SessionPromise.OnCanceled(BIND([session] {
        FailSession(session, TError(NYT::EErrorCode::Canceled, "Read session canceled"));
    }));
}

void TBlobChunkBase::DoReadSession(
    const TBlobChunkBase::TReadBlockSetSessionPtr& session,
    i64 pendingDataSize)
{
    auto pendingIOGuard = Location_->IncreasePendingIOSize(
        EIODirection::Read,
        session->Options.WorkloadDescriptor,
        pendingDataSize);
    DoReadBlockSet(session, 0, std::move(pendingIOGuard));
}

void TBlobChunkBase::DoReadBlockSet(
    const TReadBlockSetSessionPtr& session,
    int currentEntryIndex,
    TPendingIOGuard&& pendingIOGuard)
{
    while (true) {
        if (currentEntryIndex >= session->EntryCount) {
            session->DiskFetchPromise.Set();
            return;
        }

        if (!session->Entries[currentEntryIndex].Cached) {
            break;
        }

        ++currentEntryIndex;
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

    YT_LOG_DEBUG(
        "Started reading blob chunk blocks (BlockIds: %v:%v-%v, LocationId: %v, WorkloadDescriptor: %v, ReadSessionId: %v)",
        Id_,
        firstBlockIndex,
        firstBlockIndex + blocksToRead - 1,
        Location_->GetId(),
        session->Options.WorkloadDescriptor,
        session->Options.ReadSessionId);

    session->ReadTimer.emplace();

    auto reader = GetReader();
    auto asyncBlocks = reader->ReadBlocks(
        session->Options,
        firstBlockIndex,
        blocksToRead,
        std::nullopt);

    asyncBlocks.Subscribe(
        BIND(
            &TBlobChunkBase::OnBlocksRead,
            MakeStrong(this),
            session,
            firstBlockIndex,
            beginEntryIndex,
            endEntryIndex,
            Passed(std::move(pendingIOGuard)))
            .Via(session->Invoker));
}

void TBlobChunkBase::OnBlocksRead(
    const TReadBlockSetSessionPtr& session,
    int firstBlockIndex,
    int beginEntryIndex,
    int endEntryIndex,
    TPendingIOGuard&& pendingIOGuard,
    const TErrorOr<std::vector<TBlock>>& blocksOrError)
{
    if (!blocksOrError.IsOK()) {
        auto error = TError(
            NChunkClient::EErrorCode::IOError,
            "Error reading blob chunk %v",
            Id_)
            << TError(blocksOrError);
        if (IsFatalError(blocksOrError)) {
            Location_->Disable(error);
            YT_ABORT();
        }
        FailSession(session, error);
        return;
    }

    auto readTime = session->ReadTimer->GetElapsedTime();

    int blocksToRead = endEntryIndex - beginEntryIndex;

    const auto& blocks = blocksOrError.Value();
    YT_VERIFY(blocks.size() == blocksToRead);

    i64 bytesRead = 0;
    TWallTimer populateCacheTimer;
    for (int entryIndex = beginEntryIndex; entryIndex < endEntryIndex; ++entryIndex) {
        auto block = blocks[entryIndex - beginEntryIndex];
        auto& entry = session->Entries[entryIndex];
        entry.Block = block;
        bytesRead += block.Size();
        if (!entry.Cached) {
            // NB: Copy block to undumpable memory and to
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

    DoReadBlockSet(session, endEntryIndex, std::move(pendingIOGuard));
}

void TBlobChunkBase::ProfileReadBlockSetLatency(const TReadBlockSetSessionPtr& session)
{
    const auto& locationProfiler = Location_->GetProfiler();
    auto& performanceCounters = Location_->GetPerformanceCounters();
    locationProfiler.Update(
        performanceCounters.BlobBlockReadLatencies[session->Options.WorkloadDescriptor.Category],
        session->SessionTimer.GetElapsedValue());
}

void TBlobChunkBase::ProfileReadMetaLatency(const TReadMetaSessionPtr& session)
{
    const auto& locationProfiler = Location_->GetProfiler();
    auto& performanceCounters = Location_->GetPerformanceCounters();
    locationProfiler.Update(
        performanceCounters.BlobChunkMetaReadLatencies[session->Options.WorkloadDescriptor.Category],
        session->SessionTimer.GetElapsedValue());
}

TFuture<std::vector<TBlock>> TBlobChunkBase::ReadBlockSet(
    const std::vector<int>& blockIndexes,
    const TBlockReadOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    // Initialize session.
    auto session = New<TReadBlockSetSession>();
    session->Invoker = CreateFixedPriorityInvoker(
        Bootstrap_->GetStorageHeavyInvoker(),
        options.WorkloadDescriptor.GetPriority());
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
        return session->SessionPromise.ToFuture();
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

    return session->SessionPromise.ToFuture();
}

TFuture<std::vector<TBlock>> TBlobChunkBase::ReadBlockRange(
    int firstBlockIndex,
    int blockCount,
    const TBlockReadOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_VERIFY(firstBlockIndex >= 0);
    YT_VERIFY(blockCount >= 0);

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
