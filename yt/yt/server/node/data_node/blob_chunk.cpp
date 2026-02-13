#include "blob_chunk.h"

#include "blob_reader_cache.h"
#include "config.h"
#include "chunk_meta_manager.h"
#include "chunk_store.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/io/chunk_file_reader.h>
#include <yt/yt/server/lib/io/chunk_file_writer.h>
#include <yt/yt/server/lib/io/io_engine_base.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/random.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt/core/profiling/timing.h>

#include <util/system/align.h>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NIO;
using namespace NNodeTrackerClient;
using namespace NProfiling;
using namespace NThreading;

using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TBlobChunkBase::TBlobChunkBase(
    TChunkContextPtr context,
    TChunkLocationPtr location,
    const TChunkDescriptor& descriptor,
    TRefCountedChunkMetaPtr meta)
    : TChunkBase(
        std::move(context),
        std::move(location),
        descriptor.Id)
{
    Info_.set_disk_space(descriptor.DiskSpace);

    if (meta) {
        Context_->ChunkMetaManager->PutCachedMeta(Id_, meta);

        auto blocksExt = New<NIO::TBlocksExt>(GetProtoExtension<NChunkClient::NProto::TBlocksExt>(meta->extensions()));
        Context_->ChunkMetaManager->PutCachedBlocksExt(Id_, blocksExt);

        WeakBlocksExt_ = blocksExt;
    }
}

TChunkInfo TBlobChunkBase::GetInfo() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Info_;
}

bool TBlobChunkBase::IsActive() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return false;
}

TFuture<TRefCountedChunkMetaPtr> TBlobChunkBase::ReadMeta(
    const TChunkReadOptions& options,
    const std::optional<std::vector<int>>& extensionTags)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (!IsReadable()) {
        return MakeFuture<TRefCountedChunkMetaPtr>(TError("Chunk %v is not readable",
            GetId()));
    }

    auto session = New<TReadMetaSession>();
    try {
        StartReadSession(session, options);
    } catch (const std::exception& ex) {
        return MakeFuture<TRefCountedChunkMetaPtr>(ex);
    }

    auto cookie = Context_->ChunkMetaManager->BeginInsertCachedMeta(Id_);
    auto asyncMeta = cookie.GetValue();

    if (cookie.IsActive()) {
        auto callback = BIND(
            &TBlobChunkBase::DoReadMeta,
            MakeStrong(this),
            session,
            Passed(std::move(cookie)));

        Context_->StorageHeavyInvoker->Invoke(std::move(callback), options.WorkloadDescriptor.GetPriority());
    }

    return
        asyncMeta.Apply(BIND([=, this, this_ = MakeStrong(this), session = std::move(session)] (const TCachedChunkMetaPtr& cachedMeta) {
            ProfileReadMetaLatency(session);
            return FilterMeta(cachedMeta->GetMeta(), extensionTags);
        })
        .AsyncVia(Context_->StorageHeavyInvoker));
}

NIO::TBlocksExtPtr TBlobChunkBase::FindCachedBlocksExt()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    {
        auto guard = ReaderGuard(BlocksExtLock_);
        if (auto blocksExt = WeakBlocksExt_.Lock()) {
            return blocksExt;
        }
    }

    auto blocksExt = Context_->ChunkMetaManager->FindCachedBlocksExt(GetId());
    if (!blocksExt) {
        return nullptr;
    }

    {
        auto guard = WriterGuard(BlocksExtLock_);
        WeakBlocksExt_ = blocksExt;
    }

    YT_LOG_DEBUG("Per-chunk blocks ext populated from cache");

    return blocksExt;
}

TChunkFileReaderPtr TBlobChunkBase::GetReader()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    YT_VERIFY(ReadLockCounter_.load() > 0);

    {
        auto guard = ReaderGuard(LifetimeLock_);
        if (auto reader = CachedWeakReader_.Lock()) {
            return reader;
        }
    }

    auto reader = Context_->BlobReaderCache->GetReader(this);

    {
        auto guard = WriterGuard(LifetimeLock_);
        CachedWeakReader_ = reader;
    }

    return reader;
}

void TBlobChunkBase::ReleaseReader(TWriterGuard<TReaderWriterSpinLock>& writerGuard)
{
    YT_ASSERT_WRITER_SPINLOCK_AFFINITY(LifetimeLock_);
    YT_VERIFY(ReadLockCounter_.load() == 0);

    if (!PreparedReader_) {
        return;
    }

    auto reader = std::exchange(PreparedReader_, nullptr);

    writerGuard.Release();

    YT_LOG_DEBUG("Chunk reader released (ChunkId: %v, LocationId: %v, LocationUuid: %v, LocationIndex: %v)",
        Id_,
        Location_->GetId(),
        Location_->GetUuid(),
        Location_->GetIndex());
}

TSharedRef TBlobChunkBase::WrapBlockWithDelayedReferenceHolder(TSharedRef rawReference, TDuration delayBeforeFree)
{
    YT_LOG_DEBUG(
        "Simulate delay before blob read session block free (BlockSize: %v, Delay: %v)",
        rawReference.Size(),
        delayBeforeFree);
    return WrapWithDelayedReferenceHolder(
        std::move(rawReference),
        delayBeforeFree,
        GetCurrentInvoker());
}

void TBlobChunkBase::CompleteSession(const TReadBlockSetSessionPtr& session)
{
    YT_ASSERT_INVOKER_AFFINITY(session->Invoker);

    if (session->Finished.exchange(true)) {
        return;
    }

    YT_LOG_DEBUG("Read session completed (ChunkId: %v, LocationId: %v, LocationUuid: %v, LocationIndex: %v)",
        Id_,
        Location_->GetId(),
        Location_->GetUuid(),
        Location_->GetIndex());

    session->SessionAliveCheckFuture.Cancel(TError("Session completed"));

    ProfileReadBlockSetLatency(session);

    auto delayBeforeFree = Location_->GetDelayBeforeBlobSessionBlockFree();

    std::vector<TBlock> blocks;
    for (int entryIndex = 0; entryIndex < session->EntryCount; ++entryIndex) {
        auto& entry = session->Entries[entryIndex];
        auto originalEntryIndex = entry.EntryIndex;
        if (std::ssize(blocks) <= originalEntryIndex) {
            blocks.resize(originalEntryIndex + 1);
        }

        auto block = std::move(entry.Block);
        block.Data = TrackMemory(session->Options.MemoryUsageTracker, std::move(block.Data), true);

        if (delayBeforeFree) {
            block.Data = WrapBlockWithDelayedReferenceHolder(std::move(block.Data), *delayBeforeFree);
        }

        blocks[originalEntryIndex] = std::move(block);
    }

    session->SessionPromise.TrySet(std::move(blocks));
    session->LocationMemoryGuard.Transform(std::mem_fn(&TLocationMemoryGuard::Release));
}

void TBlobChunkBase::FailSession(const TReadBlockSetSessionPtr& session, const TError& error)
{
    YT_ASSERT_INVOKER_AFFINITY(session->Invoker);

    if (session->Finished.exchange(true)) {
        return;
    }

    YT_LOG_DEBUG("Read session failed (ChunkId: %v, LocationId: %v, LocationUuid: %v, LocationIndex: %v)",
        Id_,
        Location_->GetId(),
        Location_->GetUuid(),
        Location_->GetIndex());

    session->SessionAliveCheckFuture.Cancel(TError("Session failed"));

    for (int entryIndex = 0; entryIndex < session->EntryCount; ++entryIndex) {
        auto& entry = session->Entries[entryIndex];
        if (entry.Cookie) {
            entry.Cookie->SetBlock(error);
        }
    }

    for (const auto& future : session->Futures) {
        future.Cancel(error);
    }

    session->SessionPromise.TrySet(error);

    if (session->DiskFetchPromise) {
        session->DiskFetchPromise.TrySet(error);
    }

    session->LocationMemoryGuard.Transform(std::mem_fn(&TLocationMemoryGuard::Release));
}

void TBlobChunkBase::DoReadMeta(
    const TReadMetaSessionPtr& session,
    TCachedChunkMetaCookie cookie)
{
    YT_ASSERT_INVOKER_AFFINITY(Context_->StorageHeavyInvoker);

    YT_LOG_DEBUG("Started reading chunk meta (ChunkId: %v, LocationId: %v, LocationUuid: %v, LocationIndex: %v, WorkloadDescriptor: %v, ReadSessionId: %v)",
        Id_,
        Location_->GetId(),
        Location_->GetUuid(),
        Location_->GetIndex(),
        session->Options.WorkloadDescriptor,
        session->Options.ReadSessionId);

    TRefCountedChunkMetaPtr meta;
    TWallTimer readTimer;

    auto finalize = Finally([&] {
        auto readTime = readTimer.GetElapsedTime();

        session->Options.ChunkReaderStatistics->MetaReadFromDiskTime.fetch_add(
            DurationToValue(readTime),
            std::memory_order::relaxed);

        auto& performanceCounters = Location_->GetPerformanceCounters();
        performanceCounters.BlobChunkMetaReadTime.Record(readTime);
    });

    try {
        auto reader = GetReader();
        auto metaSize = reader->GetMetaSize();

        auto tags = session->Options.FairShareTags;
        if (tags.empty()) {
            auto category = session->Options.WorkloadDescriptor.Category;
            tags.emplace_back(
                ToString(category),
                Location_->GetFairShareWorkloadCategoryWeight(category));
        }

        // TODO(don-dron): Add resource acquiring (memory, cpu, net etc).
        auto fairShareQueueSlot = Location_->AddFairShareQueueSlot(
            metaSize,
            {},
            CreateHierarchyLevels(session->Options.FairShareTags));

        YT_VERIFY(fairShareQueueSlot.IsOK());

        meta = WaitFor(reader->GetMeta(session->Options, fairShareQueueSlot.Value()->GetSlot()->GetSlotId())
            .WithDeadline(session->Options.ReadMetaDeadLine))
            .ValueOrThrow();
    } catch (const std::exception& ex) {
        auto error = TError(ex);
        if (error.FindMatching(NChunkClient::EErrorCode::BrokenChunkFileMeta)) {
            if (ShouldSyncOnClose()) {
                Location_->ScheduleDisable(error);
            } else {
                YT_LOG_WARNING(error, "Error reading chunk meta, removing it (ChunkId: %v, LocationId: %v, LocationUuid: %v, LocationIndex: %v)",
                    Id_,
                    Location_->GetId(),
                    Location_->GetUuid(),
                    Location_->GetIndex());

                if (const auto& chunkStore = Location_->GetChunkStore()) {
                    YT_UNUSED_FUTURE(chunkStore->RemoveChunk(this));
                } else {
                    YT_UNUSED_FUTURE(ScheduleRemove());
                }
            }
        } else if (error.FindMatching(NFS::EErrorCode::IOError)) {
            // Location is probably broken.
            Location_->ScheduleDisable(error);
        }

        if (error.GetCode() == NYT::EErrorCode::Timeout) {
            readTimer.Stop();
            error = TError(NChunkClient::EErrorCode::ReadMetaTimeout, "Read meta from disk timed out")
                << TErrorAttribute("chunk_id", Id_)
                << TErrorAttribute("read_time", readTimer.GetElapsedTime());
        }

        cookie.Cancel(error);
        return;
    }

    readTimer.Stop();
    YT_LOG_DEBUG("Finished reading chunk meta (ChunkId: %v, LocationId: %v, LocationUuid: %v, LocationIndex: %v, ReadSessionId: %v, ReadTime: %v)",
        Id_,
        Location_->GetId(),
        Location_->GetUuid(),
        Location_->GetIndex(),
        session->Options.ReadSessionId,
        readTimer.GetElapsedTime());

    Context_->ChunkMetaManager->EndInsertCachedMeta(std::move(cookie), std::move(meta));
}

void TBlobChunkBase::OnBlocksExtLoaded(
    const TReadBlockSetSessionPtr& session,
    const TBlocksExtPtr& blocksExt)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    // Run async cache lookup.
    i64 pendingDataSize = 0;
    int pendingBlockCount = 0;
    bool diskFetchNeeded = false;

    const auto& config = Context_->DataNodeConfig;

    session->BlocksExt = blocksExt;

    for (int entryIndex = 0; entryIndex < session->EntryCount; ++entryIndex) {
        auto& entry = session->Entries[entryIndex];

        if (entry.BlockIndex >= std::ssize(blocksExt->Blocks)) {
            FailSession(session,
                TError(
                    NChunkClient::EErrorCode::MalformedReadRequest,
                    "Requested to read block with index %v from chunk %v while only %v blocks exist",
                    entry.BlockIndex,
                    GetId(),
                    std::ssize(blocksExt->Blocks)));
            return;
        }

        const auto& blockInfo = blocksExt->Blocks[entry.BlockIndex];
        entry.BeginOffset = blockInfo.Offset;
        entry.EndOffset = blockInfo.Offset + blockInfo.Size;

        YT_LOG_TRACE("Block entry (EntryIndex: %v, Block: %v, Cached: %v, BeginOffset: %v, EndOffset: %v)",
            entryIndex,
            entry.BlockIndex,
            entry.Cached,
            entry.BeginOffset,
            entry.EndOffset);

        if (entry.Cached) {
            continue;
        }

        if (session->Options.PopulateCache) {
            const auto& blockCache = session->Options.BlockCache;

            auto blockId = TBlockId(Id_, entry.BlockIndex);
            entry.Cookie = blockCache->GetBlockCookie(blockId, session->Options.BlockType);

            if (!entry.Cookie->IsActive()) {
                entry.Cached = true;
                session->Futures.push_back(entry.Cookie->GetBlockFuture().Apply(
                    BIND([session, entryIndex] {
                        const auto& entry = session->Entries[entryIndex];
                        auto block = entry.Cookie->GetBlock();
                        session->Options.ChunkReaderStatistics->DataBytesReadFromCache.fetch_add(
                            block.Size(),
                            std::memory_order::relaxed);
                        session->Entries[entryIndex].Block = std::move(block);
                    })));
                continue;
            }
        }

        diskFetchNeeded = true;
        pendingDataSize += blockInfo.Size;
        pendingBlockCount += 1;

        if (pendingDataSize >= config->MaxBytesPerRead ||
            pendingBlockCount >= config->MaxBlocksPerRead)
        {
            session->EntryCount = entryIndex + 1;
            YT_LOG_DEBUG(
                "Read session trimmed due to read constraints "
                "(PendingDataSize: %v, PendingBlockCount: %v, TrimmedBlockCount: %v)",
                pendingDataSize,
                pendingBlockCount,
                session->EntryCount);
            break;
        }
    }

    if (diskFetchNeeded) {
        session->DiskFetchPromise = NewPromise<void>();
        session->Futures.push_back(session->DiskFetchPromise.ToFuture());

        auto readCallback = BIND([=, this, this_ = MakeStrong(this)] {
            DoReadSession(session, pendingDataSize);
        });

        const auto& outThrottler = Location_->GetOutThrottler(session->Options.WorkloadDescriptor);
        if (outThrottler->TryAcquire(pendingDataSize)) {
            session->Invoker->Invoke(std::move(readCallback));
        } else {
            YT_LOG_DEBUG("Disk read throttling is active (PendingDataSize: %v, WorkloadDescriptor: %v)",
                pendingDataSize,
                session->Options.WorkloadDescriptor);
            auto throttleFuture = outThrottler->Throttle(pendingDataSize);
            session->Futures.push_back(throttleFuture.Apply(readCallback.AsyncVia(session->Invoker)));
        }
    }

    AllSucceeded(session->Futures)
        .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
            if (error.IsOK()) {
                CompleteSession(session);
            } else {
                FailSession(session, error);
            }
        }).Via(session->Invoker));

    auto cancelHandler = BIND([this, this_ = MakeStrong(this), session] (const TError& error) {
        FailSession(
            session,
            TError(NYT::EErrorCode::Canceled, "Session canceled") << error);
    }).Via(session->Invoker);

    if (!session->SessionPromise.OnCanceled(cancelHandler)) {
        cancelHandler.Run(TError(NYT::EErrorCode::Canceled, "Session canceled before setting cancel handler"));
    }
}

i64 TBlobChunkBase::GetAlignedPendingDataSize(i64 pendingDataSize)
{
    auto reader = GetReader();
    return AlignUp<i64>(pendingDataSize, reader->GetBlockAlignment());
}

void TBlobChunkBase::DoReadSession(
    const TBlobChunkBase::TReadBlockSetSessionPtr& session,
    i64 pendingDataSize)
{
    YT_ASSERT_INVOKER_AFFINITY(session->Invoker);

    auto alignedPendingDataSize = GetAlignedPendingDataSize(pendingDataSize);

    const auto& memoryTracker = Location_->GetReadMemoryTracker();
    auto memoryGuardOrError = TMemoryUsageTrackerGuard::TryAcquire(memoryTracker, alignedPendingDataSize);
    if (!memoryGuardOrError.IsOK()) {
        YT_LOG_DEBUG("Read session aborted due to memory pressure");
        Location_->ReportThrottledRead();

        auto error = TError("Read session aborted due to memory pressure");
        for (auto i = 0; i < session->EntryCount; ++i) {
            if (!session->Entries[i].Cached && session->Entries[i].Cookie) {
                session->Entries[i].Cookie->SetBlock(error);
            }
        }

        session->DiskFetchPromise.TrySet();
        return;
    }

    auto tags = session->Options.FairShareTags;
    if (tags.empty()) {
        auto category = session->Options.WorkloadDescriptor.Category;
        tags.emplace_back(
            ToString(category),
            Location_->GetFairShareWorkloadCategoryWeight(category));
    }

    // TODO(don-dron): Add resource acquiring (memory, cpu, net etc).
    auto fairShareSlotOrError = Location_->AddFairShareQueueSlot(
        alignedPendingDataSize,
        {},
        CreateHierarchyLevels(session->Options.FairShareTags));

    YT_VERIFY(fairShareSlotOrError.IsOK());

    session->FairShareSlot = fairShareSlotOrError.Value();

    session->LocationMemoryGuard.Store(Location_->AcquireLocationMemory(
        /*useLegacyUsedMemory*/ false,
        std::move(memoryGuardOrError.Value()),
        EIODirection::Read,
        session->Options.WorkloadDescriptor,
        alignedPendingDataSize));

    DoReadBlockSet(session);
}

std::tuple<int, int, THashMap<int, TBlobChunkBase::TReadBlockSetSession::TBlockEntry>>
TBlobChunkBase::FindLastEntryWithinReadGap(
    const TReadBlockSetSessionPtr& session,
    int beginEntryIndex)
{
    int endEntryIndex = beginEntryIndex + 1;
    const auto* previousEntry = &session->Entries[beginEntryIndex];

    const auto& blocksExt = session->BlocksExt;
    const auto& blockCache = session->Options.BlockCache;

    THashMap<int, TReadBlockSetSession::TBlockEntry> blockIndexToEntry;

    while (endEntryIndex < session->EntryCount) {
        const auto& entry = session->Entries[endEntryIndex];
        if (entry.Cached) {
            ++endEntryIndex;
            continue;
        }

        auto readGapSize = entry.BeginOffset - previousEntry->EndOffset;
        // Non-cached blocks are following in ascending order of block index.
        YT_VERIFY(readGapSize >= 0);

        if (readGapSize > Location_->GetCoalescedReadMaxGapSize()) {
            YT_LOG_DEBUG("Stopping run due to large gap ("
                "GapBlocks: %v, GapBlockOffsets: [%v,%v), "
                "GapBlockCount: %v, GapSize: %v)",
                FormatBlocks(previousEntry->BlockIndex + 1, entry.BlockIndex + 1),
                previousEntry->EndOffset,
                entry.BeginOffset,
                entry.BlockIndex - previousEntry->BlockIndex - 1,
                readGapSize);
            break;
        } else if (readGapSize > 0) {
            YT_LOG_DEBUG("Coalesced read gap ("
                "GapBlocks: %v, GapBlockOffsets: [%v,%v), "
                "GapBlockCount: %v, GapSize: %v)",
                FormatBlocks(previousEntry->BlockIndex + 1, entry.BlockIndex),
                previousEntry->EndOffset,
                entry.BeginOffset,
                entry.BlockIndex - previousEntry->BlockIndex - 1,
                readGapSize);
            for (int index = previousEntry->BlockIndex + 1; index < entry.BlockIndex; index++) {
                const auto& info = blocksExt->Blocks[index];

                auto blockId = TBlockId(Id_, index);
                auto cookie = blockCache->GetBlockCookie(blockId, session->Options.BlockType);

                EmplaceOrCrash(blockIndexToEntry, index, TReadBlockSetSession::TBlockEntry{
                    .BlockIndex = index,
                    .Cached = !cookie->IsActive(),
                    .Cookie = std::move(cookie),
                    .BeginOffset = info.Offset,
                    .EndOffset = info.Offset + info.Size,
                });
            }
        }

        previousEntry = &entry;
        ++endEntryIndex;
    }

    return {previousEntry->BlockIndex, endEntryIndex, std::move(blockIndexToEntry)};
}

void TBlobChunkBase::DoReadBlockSet(
    const TReadBlockSetSessionPtr& session)
{
    YT_ASSERT_INVOKER_AFFINITY(session->Invoker);

    YT_VERIFY(session->EntryCount > 0);

    auto readBlocksRequests = CalculateReadBlocksRequests(session);

    if (session->Options.EnableSequentialIORequests) {
        DoReadBlockSetSequentially(session, std::move(readBlocksRequests), 0);
    } else {
        DoReadBlockSetInParallel(session, std::move(readBlocksRequests));
    }
}

TFuture<void> TBlobChunkBase::ReadBlocks(
    const TReadBlockSetSessionPtr& session,
    TReadBlocksRequest readBlocksRequest)
{
    YT_LOG_DEBUG("Started reading blob chunk blocks ("
        "ChunkId: %v, Blocks: %v, "
        "LocationId: %v, LocationUuid: %v, LocationIndex: %v, WorkloadDescriptor: %v, "
        "ReadSessionId: %v, GapBlockCount: %v, "
        "LeftBorder: %v, RightBorder: %v)",
        Id_,
        FormatBlocks(readBlocksRequest.FirstBlockIndex, readBlocksRequest.FirstBlockIndex + readBlocksRequest.BlocksToRead - 1),
        Location_->GetId(),
        Location_->GetUuid(),
        Location_->GetIndex(),
        session->Options.WorkloadDescriptor,
        session->Options.ReadSessionId,
        readBlocksRequest.BlocksToRead - (readBlocksRequest.EndEntryIndex - readBlocksRequest.BeginEntryIndex),
        readBlocksRequest.FirstBlockIndex,
        readBlocksRequest.FirstBlockIndex + readBlocksRequest.BlocksToRead - 1);

    YT_VERIFY(readBlocksRequest.FirstBlockIndex >= 0);
    YT_VERIFY(readBlocksRequest.BlocksToRead > 0);

    auto reader = GetReader();

    YT_VERIFY(session->FairShareSlot);
    auto asyncBlocks = reader->ReadBlocks(
        session->Options,
        readBlocksRequest.FirstBlockIndex,
        readBlocksRequest.BlocksToRead,
        session->FairShareSlot->GetSlot()->GetSlotId(),
        session->BlocksExt);

    return asyncBlocks.Apply(
        BIND(
            &TBlobChunkBase::OnBlocksRead,
            MakeStrong(this),
            session,
            TWallTimer(),
            readBlocksRequest.FirstBlockIndex,
            readBlocksRequest.BlocksToRead,
            readBlocksRequest.BeginEntryIndex,
            readBlocksRequest.EndEntryIndex,
            Passed(std::move(readBlocksRequest.BlockIndexToEntry)))
            .AsyncVia(session->Invoker));
}

i64 TBlobChunkBase::CalculateAdditionalMemory(const TReadBlocksRequest& request)
{
    i64 additionalMemory = 0;

    for (const auto& [blockIndex, entry] : request.BlockIndexToEntry) {
        additionalMemory += entry.EndOffset - entry.BeginOffset;
    }

    return additionalMemory;
}

void TBlobChunkBase::DoReadBlockSetSequentially(
    const TReadBlockSetSessionPtr& session,
    std::vector<TReadBlocksRequest> requests,
    int currentRequestIndex)
{
    YT_ASSERT_INVOKER_AFFINITY(session->Invoker);

    if (currentRequestIndex >= std::ssize(requests)) {
        session->DiskFetchPromise.TrySet();
        return;
    }

    if (session->DiskFetchPromise.IsCanceled() || TInstant::Now() > session->Options.ReadBlocksDeadline) {
        YT_LOG_DEBUG(
            "Read session trimmed due to deadline or cancellation (Deadline: %v, IsCanceled: %v)",
            session->Options.ReadBlocksDeadline,
            session->DiskFetchPromise.IsCanceled());
        auto error = TError(NChunkClient::EErrorCode::ReaderTimeout, "Read session trimmed due to deadline");
        for (auto i = requests[currentRequestIndex].BeginEntryIndex; i < session->EntryCount; ++i) {
            if (!session->Entries[i].Cached && session->Entries[i].Cookie) {
                session->Entries[i].Cookie->SetBlock(error);
            }
        }
        session->DiskFetchPromise.TrySet();
        return;
    }

    auto& currentRequest = requests[currentRequestIndex];

    session->LocationMemoryGuard.Transform([additionalMemory = CalculateAdditionalMemory(currentRequest)] (TLocationMemoryGuard& guard) {
        if (guard) {
            guard.IncreaseSize(additionalMemory);
        }
    });

    YT_UNUSED_FUTURE(ReadBlocks(session, std::move(currentRequest))
        .Apply(
            BIND(&TBlobChunkBase::DoReadBlockSetSequentially, MakeStrong(this), session, Passed(std::move(requests)), currentRequestIndex + 1)
                .AsyncVia(session->Invoker)));
}

void TBlobChunkBase::DoReadBlockSetInParallel(
    const TReadBlockSetSessionPtr& session,
    std::vector<TReadBlocksRequest> requests)
{
    YT_ASSERT_INVOKER_AFFINITY(session->Invoker);

    std::vector<TFuture<void>> readRequests;

    for (auto& readBlocksRequest : requests) {
        readRequests.push_back(ReadBlocks(session, std::move(readBlocksRequest)));
    }

    i64 additionalMemory = 0;

    for (const auto& currentRequest : requests) {
        additionalMemory += CalculateAdditionalMemory(currentRequest);
    }

    session->LocationMemoryGuard.Transform([additionalMemory] (TLocationMemoryGuard& guard) {
        if (guard) {
            guard.IncreaseSize(additionalMemory);
        }
    });

    session->DiskFetchPromise.TrySetFrom(AllSucceeded(readRequests));
}

std::vector<TBlobChunkBase::TReadBlocksRequest> TBlobChunkBase::CalculateReadBlocksRequests(
    const TReadBlockSetSessionPtr& session)
{
    std::vector<TBlobChunkBase::TReadBlocksRequest> result;

    for (int beginEntryIndex = 0; beginEntryIndex < session->EntryCount;) {
        auto readBlocksRequest = NextReadBlocksRequest(session, beginEntryIndex);
        if (readBlocksRequest) {
            YT_VERIFY(readBlocksRequest->BeginEntryIndex >= beginEntryIndex);
            YT_VERIFY(readBlocksRequest->EndEntryIndex > beginEntryIndex);
            beginEntryIndex = readBlocksRequest->EndEntryIndex;
            result.push_back(std::move(*readBlocksRequest));
        } else {
            break;
        }
    }

    return result;
}

std::optional<TBlobChunkBase::TReadBlocksRequest> TBlobChunkBase::NextReadBlocksRequest(
    const TReadBlockSetSessionPtr& session,
    int startEntryIndex)
{
    YT_ASSERT_INVOKER_AFFINITY(session->Invoker);

    auto firstNotCachedEntryIndex = std::find_if(
        session->Entries.get() + startEntryIndex,
        session->Entries.get() + session->EntryCount,
        [] (const auto& entry) { return !entry.Cached; }) - session->Entries.get();

    if (firstNotCachedEntryIndex >= session->EntryCount) {
        return std::nullopt;
    }

    YT_VERIFY(firstNotCachedEntryIndex < session->EntryCount);
    YT_VERIFY(!session->Entries[firstNotCachedEntryIndex].Cached);

    const int beginEntryIndex = firstNotCachedEntryIndex;
    const int firstBlockIndex = session->Entries[beginEntryIndex].BlockIndex;

    // Extract the maximum run of block. Blocks should be contiguous or at least have pretty small gap between them
    // (if gap is small enough, coalesced read including gap blocks is more efficient than making two separate runs).
    auto [lastBlockIndex, endEntryIndex, blockIndexToEntry] = FindLastEntryWithinReadGap(session, beginEntryIndex);
    YT_VERIFY(endEntryIndex <= session->EntryCount && endEntryIndex > beginEntryIndex && lastBlockIndex >= firstBlockIndex);

    const int blocksToRead = lastBlockIndex - firstBlockIndex + 1;
    return TReadBlocksRequest{
        .FirstBlockIndex = firstBlockIndex,
        .BlocksToRead = blocksToRead,
        .BeginEntryIndex = beginEntryIndex,
        .EndEntryIndex = endEntryIndex,
        .BlockIndexToEntry = std::move(blockIndexToEntry),
    };
}

void TBlobChunkBase::OnBlocksRead(
    const TReadBlockSetSessionPtr& session,
    NProfiling::TWallTimer readTimer,
    int firstBlockIndex,
    int blocksToRead,
    int beginEntryIndex,
    int endEntryIndex,
    THashMap<int, TReadBlockSetSession::TBlockEntry> blockIndexToEntry,
    const TErrorOr<std::vector<TBlock>>& blocksOrError)
{
    YT_ASSERT_INVOKER_AFFINITY(session->Invoker);

    if (!blocksOrError.IsOK()) {
        auto error = TError(
            NChunkClient::EErrorCode::IOError,
            "Error reading blob chunk %v",
            Id_)
            << TError(blocksOrError);
        if (blocksOrError.FindMatching(NChunkClient::EErrorCode::IncorrectChunkFileChecksum)) {
            if (ShouldSyncOnClose()) {
                Location_->ScheduleDisable(error);
            } else {
                YT_LOG_WARNING("Block in chunk without \"sync_on_close\" has checksum mismatch, removing it (ChunkId: %v, LocationId: %v, LocationUuid: %v, LocationIndex: %v)",
                    Id_,
                    Location_->GetId(),
                    Location_->GetUuid(),
                    Location_->GetIndex());

                if (const auto& chunkStore = Location_->GetChunkStore()) {
                    YT_UNUSED_FUTURE(chunkStore->RemoveChunk(this));
                } else {
                    YT_UNUSED_FUTURE(ScheduleRemove());
                }
            }
        } else if (blocksOrError.FindMatching(NFS::EErrorCode::IOError)) {
            Location_->ScheduleDisable(error);
        }

        session->DiskFetchPromise.TrySet(error);
        return;
    }

    auto readTime = readTimer.GetElapsedTime();

    const auto& blocks = blocksOrError.Value();
    YT_VERIFY(std::ssize(blocks) == blocksToRead);

    i64 bytesRead = GetByteSize(blocks);

    TWallTimer populateCacheTimer;
    i64 usefulBlockSize = 0;
    int usefulBlockCount = 0;

    auto takeBlock = [&] (auto& entry) {
        if (!entry.Cached) {
            auto relativeBlockIndex = entry.BlockIndex - firstBlockIndex;
            auto block = blocks[relativeBlockIndex];
            YT_VERIFY(block.Size() > 0);

            entry.Block = block;

            YT_VERIFY(entry.Block);

            ++usefulBlockCount;
            usefulBlockSize += block.Size();
            if (entry.Cookie) {
                entry.Cookie->SetBlock(TCachedBlock(std::move(block)));
            }
        }
    };

    for (int entryIndex = beginEntryIndex; entryIndex < endEntryIndex; ++entryIndex) {
        auto& entry = session->Entries[entryIndex];
        takeBlock(entry);
    }

    for (auto& [blockIndex, entry] : blockIndexToEntry) {
        takeBlock(entry);
    }

    auto populateCacheTime = populateCacheTimer.GetElapsedTime();

    auto gapBlockCount = blocksToRead - usefulBlockCount;
    auto gapBlockSize = bytesRead - usefulBlockSize;

    YT_LOG_DEBUG("Finished reading blob chunk blocks ("
        "ChunkId: %v, Blocks: %v, LocationId: %v, LocationUuid: %v, LocationIndex: %v, BytesRead: %v, "
        "ReadTime: %v, UsefulBlockSize: %v, UsefulBlockCount: %v, PopulateCacheTime: %v, ReadSessionId: %v, "
        "GapBlockSize: %v, GapBlockCount: %v)",
        Id_,
        FormatBlocks(firstBlockIndex, firstBlockIndex + blocksToRead - 1),
        Location_->GetId(),
        Location_->GetUuid(),
        Location_->GetIndex(),
        bytesRead,
        readTime,
        usefulBlockSize,
        usefulBlockCount,
        populateCacheTime,
        session->Options.ReadSessionId,
        gapBlockSize,
        gapBlockCount);

    auto& chunkReaderStatistics = session->Options.ChunkReaderStatistics;

    chunkReaderStatistics->WastedDataBytesReadFromDisk.fetch_add(
        gapBlockSize, std::memory_order::relaxed);

    chunkReaderStatistics->DataBlocksReadFromDisk.fetch_add(
        blocksToRead, std::memory_order::relaxed);

    chunkReaderStatistics->WastedDataBlocksReadFromDisk.fetch_add(
        gapBlockCount, std::memory_order::relaxed);

    auto& performanceCounters = Location_->GetPerformanceCounters();
    auto category = session->Options.WorkloadDescriptor.Category;
    performanceCounters.BlobBlockReadSize[category].Record(bytesRead);
    performanceCounters.BlobBlockReadTime[category].Record(readTime);
    performanceCounters.BlobBlockReadBytes.Increment(bytesRead);
    performanceCounters.BlobBlockReadCount.Increment(blocksToRead);

    Location_->IncreaseCompletedIOSize(EIODirection::Read, session->Options.WorkloadDescriptor, bytesRead);
}

bool TBlobChunkBase::ShouldSyncOnClose()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto blocksExt = FindCachedBlocksExt();
    if (!blocksExt) {
        return true;
    }

    return blocksExt->SyncOnClose;
}

bool TBlobChunkBase::IsReadable()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return !IsArtifactChunkId(GetId());
}

TFuture<std::vector<TBlock>> TBlobChunkBase::ReadBlockSet(
    const std::vector<int>& blockIndexes,
    const TChunkReadOptions& options)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (!IsReadable()) {
        return MakeFuture<std::vector<TBlock>>(TError("Chunk %v is not readable",
            GetId()));
    }

    auto session = New<TReadBlockSetSession>();
    try {
        // Initialize session.
        StartReadSession(session, options);
        session->Invoker = CreateFixedPriorityInvoker(
            Context_->StorageHeavyInvoker,
            options.WorkloadDescriptor.GetPriority());
        session->EntryCount = std::ssize(blockIndexes);
        session->Entries.reset(new TReadBlockSetSession::TBlockEntry[session->EntryCount]);
        for (int entryIndex = 0; entryIndex < session->EntryCount; ++entryIndex) {
            auto& entry = session->Entries[entryIndex];
            entry.BlockIndex = blockIndexes[entryIndex];
            entry.EntryIndex = entryIndex;
        }
        session->Options.MemoryUsageTracker = options.MemoryUsageTracker;
        session->Options.UseDedicatedAllocations = true;

        auto dynamicLongLiveReadSessionThreshold = Context_->DynamicConfigManager->GetConfig()->DataNode->LongLiveReadSessionThreshold;
        auto longLiveReadSessionThreshold = dynamicLongLiveReadSessionThreshold.value_or(Context_->DataNodeConfig->LongLiveReadSessionThreshold);

        session->SessionAliveCheckFuture = TDelayedExecutor::MakeDelayed(longLiveReadSessionThreshold)
            .Apply(BIND([weakSession = MakeWeak(session), chunkId = GetId()] (const TError& error) {
                if (error.IsOK()) {
                    if (auto session = weakSession.Lock()) {
                        YT_LOG_ALERT(
                            "Long live read session ("
                            "ChunkId: %v, FutureCount: %v, "
                            "DiskPromise: %v, DiskPromiseIsSet: %v, DiskPromiseIsCanceled: %v, "
                            "EntryCount: %v, BlocksExtLoaded: %v, "
                            "SessionPromiseIsCanceled: %v, Finished: %v, "
                            "ReadLockCounter: %v)",
                            chunkId,
                            session->Futures.size(),
                            static_cast<bool>(session->DiskFetchPromise),
                            session->DiskFetchPromise && session->DiskFetchPromise.IsSet(),
                            session->DiskFetchPromise && session->DiskFetchPromise.IsCanceled(),
                            session->EntryCount,
                            session->BlocksExt != nullptr,
                            session->SessionPromise.IsCanceled(),
                            session->Finished,
                            session->ChunkReadGuard->GetChunk()->GetReadLockCounter());
                    }
                } else {
                    YT_LOG_DEBUG(error,
                        "Session completed before timeout (ChunkId: %v)",
                        chunkId);
                }
            }));
    } catch (const std::exception& ex) {
        return MakeFuture<std::vector<TBlock>>(ex);
    }

    // Sort entries by block index to make read coalescing possible.
    std::sort(
        session->Entries.get(),
        session->Entries.get() + session->EntryCount,
        [] (const TReadBlockSetSession::TBlockEntry& lhs, const TReadBlockSetSession::TBlockEntry& rhs) {
            return lhs.BlockIndex < rhs.BlockIndex;
        });

    // Run sync cache lookup.
    bool allCached = true;
    if (options.FetchFromCache && options.BlockCache) {
        for (int entryIndex = 0; entryIndex < std::ssize(blockIndexes); ++entryIndex) {
            auto& entry = session->Entries[entryIndex];
            auto blockId = TBlockId(Id_, entry.BlockIndex);
            auto block = options.BlockCache->FindBlock(blockId, options.BlockType);
            if (block) {
                session->Options.ChunkReaderStatistics->DataBytesReadFromCache.fetch_add(
                    block.Size(),
                    std::memory_order::relaxed);
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
        return BIND([=, this, this_ = MakeStrong(this)] (const TReadBlockSetSessionPtr session) {
            CompleteSession(session);
            return session->SessionPromise.ToFuture();
        })
            .AsyncVia(session->Invoker)
            .Run(session);
    }

    // Need blocks ext.
    auto blocksExt = FindCachedBlocksExt();
    if (blocksExt) {
        session->Invoker->Invoke(BIND(&TBlobChunkBase::OnBlocksExtLoaded, MakeStrong(this), session, blocksExt));
    } else {
        auto cookie = Context_->ChunkMetaManager->BeginInsertCachedBlocksExt(Id_);
        auto asyncBlocksExt = cookie.GetValue();
        if (cookie.IsActive()) {
            ReadMeta(options)
                .Subscribe(BIND([=, this, this_ = MakeStrong(this), cookie = std::move(cookie)] (const TErrorOr<TRefCountedChunkMetaPtr>& result) mutable {
                    if (result.IsOK()) {
                        auto blocksExt = New<NIO::TBlocksExt>(GetProtoExtension<NChunkClient::NProto::TBlocksExt>(result.Value()->extensions()));
                        {
                            auto guard = WriterGuard(BlocksExtLock_);
                            WeakBlocksExt_ = blocksExt;
                        }
                        Context_->ChunkMetaManager->EndInsertCachedBlocksExt(std::move(cookie), blocksExt);
                    } else {
                        cookie.Cancel(TError(result));
                    }
                }));
        }
        asyncBlocksExt.Subscribe(
            BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TCachedBlocksExtPtr>& cachedBlocksExtOrError) {
                if (cachedBlocksExtOrError.IsOK()) {
                    const auto& cachedBlocksExt = cachedBlocksExtOrError.Value();
                    OnBlocksExtLoaded(session, cachedBlocksExt->GetBlocksExt());
                } else {
                    FailSession(session, cachedBlocksExtOrError);
                }
            }).Via(session->Invoker));
    }

    return session->SessionPromise.ToFuture();
}

TFuture<std::vector<TBlock>> TBlobChunkBase::ReadBlockRange(
    int firstBlockIndex,
    int blockCount,
    const TChunkReadOptions& options)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    YT_VERIFY(firstBlockIndex >= 0);
    YT_VERIFY(blockCount >= 0);

    if (!IsReadable()) {
        return MakeFuture<std::vector<TBlock>>(TError("Chunk %v is not readable",
            GetId()));
    }

    std::vector<int> blockIndexes;
    for (int blockIndex = firstBlockIndex; blockIndex < firstBlockIndex + blockCount; ++blockIndex) {
        blockIndexes.push_back(blockIndex);
    }

    return ReadBlockSet(blockIndexes, options);
}

TFuture<void> TBlobChunkBase::PrepareToReadChunkFragments(
    const TClientChunkReadOptions& options,
    bool useDirectIO)
{
    auto readerGuard = ReaderGuard(LifetimeLock_);

    YT_VERIFY(ReadLockCounter_.load() > 0);

    if (PreparedReader_) {
        YT_UNUSED_FUTURE(PreparedReader_->PrepareToReadChunkFragments(options, useDirectIO));
        return {};
    }

    auto reader = CachedWeakReader_.Lock();
    if (reader && !reader->PrepareToReadChunkFragments(options, useDirectIO)) {
        PreparedReader_ = std::move(reader);
        return {};
    }

    readerGuard.Release();

    if (!reader) {
        reader = Context_->BlobReaderCache->GetReader(this);
    }

    auto prepareFuture = reader->PrepareToReadChunkFragments(options, useDirectIO);

    auto writerGuard = WriterGuard(LifetimeLock_);

    YT_VERIFY(ReadLockCounter_.load() > 0);

    CachedWeakReader_ = reader;

    if (!prepareFuture) {
        PreparedReader_ = std::move(reader);
    }

    if (PreparedReader_) {
        return {};
    }

    writerGuard.Release();

    return prepareFuture
        .Apply(BIND([=, this, this_ = MakeStrong(this)] {
            auto writerGuard = WriterGuard(LifetimeLock_);

            if (ReadLockCounter_.load() == 0 || PreparedReader_) {
                return;
            }

            PreparedReader_ = reader;

            writerGuard.Release();

            YT_LOG_DEBUG("Chunk reader prepared to read fragments (ChunkId: %v, LocationId: %v, LocationUuid: %v, LocationIndex: %v)",
                Id_,
                Location_->GetId(),
                Location_->GetUuid(),
                Location_->GetIndex());
        }).AsyncVia(Context_->StorageLightInvoker));
}

TReadRequest TBlobChunkBase::MakeChunkFragmentReadRequest(
    const TChunkFragmentDescriptor& fragmentDescriptor,
    bool useDirectIO)
{
    YT_VERIFY(ReadLockCounter_.load() > 0);
    YT_VERIFY(PreparedReader_);

    if (!IsReadable()) {
        THROW_ERROR_EXCEPTION("Chunk %v is not readable",
            GetId());
    }

    return PreparedReader_->MakeChunkFragmentReadRequest(fragmentDescriptor, useDirectIO);
}

void TBlobChunkBase::SyncRemove(bool force)
{
    YT_ASSERT_INVOKER_AFFINITY(Location_->GetAuxPoolInvoker());

    Context_->BlobReaderCache->EvictReader(this);

    Location_->RemoveChunkFiles(Id_, force);
}

TFuture<void> TBlobChunkBase::AsyncRemove()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return BIND(&TBlobChunkBase::SyncRemove, MakeStrong(this), false)
        .AsyncVia(Location_->GetAuxPoolInvoker())
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

TStoredBlobChunk::TStoredBlobChunk(
    TChunkContextPtr context,
    TChunkLocationPtr location,
    const TChunkDescriptor& descriptor,
    TRefCountedChunkMetaPtr meta)
    : TBlobChunkBase(
        std::move(context),
        std::move(location),
        descriptor,
        std::move(meta))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
