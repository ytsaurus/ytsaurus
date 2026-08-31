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

#include <library/cpp/iterator/zip.h>

#include <util/system/align.h>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NIO;
using namespace NNode;
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

    YT_TLOG_DEBUG("Per-chunk blocks ext populated from cache");

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

    YT_TLOG_DEBUG("Chunk reader released")
        .With("ChunkId", Id_)
        .With("LocationId", Location_->GetId())
        .With("LocationUuid", Location_->GetUuid())
        .With("LocationIndex", Location_->GetIndex());
}

TSharedRef TBlobChunkBase::WrapBlockWithDelayedReferenceHolder(TSharedRef rawReference, TDuration delayBeforeFree)
{
    YT_TLOG_DEBUG("Simulate delay before blob read session block free")
        .With("BlockSize", rawReference.Size())
        .With("Delay", delayBeforeFree);
    return WrapWithDelayedReferenceHolder(
        std::move(rawReference),
        delayBeforeFree,
        GetCurrentInvoker());
}

std::vector<NChunkClient::TBlock> TBlobChunkBase::CollectBlocks(const TReadBlockSetSessionPtr& session)
{
    YT_ASSERT_INVOKER_AFFINITY(session->Invoker);

    auto delayBeforeFree = Location_->GetDelayBeforeBlobSessionBlockFree();

    std::vector<TBlock> blocks;
    for (int entryIndex = 0; entryIndex < session->EntryCount; ++entryIndex) {
        auto& entry = session->Entries[entryIndex];
        auto originalEntryIndex = entry.EntryIndex;
        if (std::ssize(blocks) <= originalEntryIndex) {
            blocks.resize(originalEntryIndex + 1);
        }

        auto block = entry.Block.Exchange(TBlock());
        block.Data = TrackMemory(session->Options.MemoryUsageTracker, std::move(block.Data), true);

        if (delayBeforeFree) {
            block.Data = WrapBlockWithDelayedReferenceHolder(std::move(block.Data), *delayBeforeFree);
        }

        blocks[originalEntryIndex] = std::move(block);
    }

    return blocks;
}

void TBlobChunkBase::CompleteSession(const TReadBlockSetSessionPtr& session)
{
    YT_ASSERT_INVOKER_AFFINITY(session->Invoker);

    if (session->Finished.exchange(true)) {
        return;
    }

    YT_TLOG_DEBUG("Read session completed")
        .With("ChunkId", Id_)
        .With("LocationId", Location_->GetId())
        .With("LocationUuid", Location_->GetUuid())
        .With("LocationIndex", Location_->GetIndex());

    session->SessionAliveCheckFuture.Cancel(TError("Session completed"));
    if (session->SessionDeadlineFuture) {
        session->SessionDeadlineFuture.Cancel(TError("Session completed"));
    }

    ProfileReadBlockSetLatency(session);

    session->SessionPromise.TrySet(CollectBlocks(session));
    session->LocationMemoryGuard.Transform(std::mem_fn(&TLocationMemoryGuard::Release));
}

void TBlobChunkBase::FailSession(const TReadBlockSetSessionPtr& session, const TError& error)
{
    YT_ASSERT_INVOKER_AFFINITY(session->Invoker);

    if (session->Finished.exchange(true)) {
        return;
    }

    YT_TLOG_DEBUG("Read session failed")
        .With("ChunkId", Id_)
        .With("LocationId", Location_->GetId())
        .With("LocationUuid", Location_->GetUuid())
        .With("LocationIndex", Location_->GetIndex())
        .With(error);

    session->SessionAliveCheckFuture.Cancel(TError("Session failed"));
    if (session->SessionDeadlineFuture) {
        session->SessionDeadlineFuture.Cancel(TError("Session failed"));
    }

    for (int entryIndex = 0; entryIndex < session->EntryCount; ++entryIndex) {
        auto& entry = session->Entries[entryIndex];
        if (!entry.Cached && entry.Cookie) {
            entry.Cookie->SetBlock(error);
        }
    }

    for (const auto& future : session->Futures) {
        future.Cancel(error);
    }

    if (session->Options.ReturnBlocksIfSessionFails) {
        auto blocks = CollectBlocks(session);
        auto blocksWithData = std::count_if(blocks.begin(), blocks.end(), [] (const auto& block) { return static_cast<bool>(block); });
        if (blocksWithData > 0) {
            session->SessionPromise.TrySet(blocks);
        } else {
            session->SessionPromise.TrySet(error);
        }
    } else {
        session->SessionPromise.TrySet(error);
    }

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

    YT_TLOG_DEBUG("Started reading chunk meta")
        .With("ChunkId", Id_)
        .With("LocationId", Location_->GetId())
        .With("LocationUuid", Location_->GetUuid())
        .With("LocationIndex", Location_->GetIndex())
        .With("WorkloadDescriptor", session->Options.WorkloadDescriptor)
        .With("ReadSessionId", session->Options.ReadSessionId);

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

        // TODO(don-dron): Add resource acquiring (memory, cpu, net etc).
        auto fairShareQueueSlot = Location_->AddFairShareQueueSlot(
            metaSize,
            {},
            CreateHierarchyLevels(Location_->BuildFairShareTags(
                session->Options.WorkloadDescriptor.Category,
                session->Options.FairShareState)));

        YT_VERIFY(fairShareQueueSlot.IsOK());

        meta = WaitFor(reader->GetMeta(
            session->Options,
            fairShareQueueSlot.Value()->GetSlot()->GetSlotId(),
            session->Options.FairShareState)
            .WithDeadline(session->Options.ReadMetaDeadLine))
            .ValueOrThrow();
    } catch (const std::exception& ex) {
        auto error = TError(ex);
        if (error.FindMatching(NChunkClient::EErrorCode::BrokenChunkFileMeta)) {
            if (ShouldSyncOnClose()) {
                Location_->ScheduleDisable(error);
            } else {
                YT_TLOG_WARNING("Error reading chunk meta, removing it")
                    .With("ChunkId", Id_)
                    .With("LocationId", Location_->GetId())
                    .With("LocationUuid", Location_->GetUuid())
                    .With("LocationIndex", Location_->GetIndex())
                    .With(error);

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
                .With("chunk_id", Id_)
                .With("read_time", readTimer.GetElapsedTime());
        }

        cookie.Cancel(error);
        return;
    }

    readTimer.Stop();
    YT_TLOG_DEBUG("Finished reading chunk meta")
        .With("ChunkId", Id_)
        .With("LocationId", Location_->GetId())
        .With("LocationUuid", Location_->GetUuid())
        .With("LocationIndex", Location_->GetIndex())
        .With("ReadSessionId", session->Options.ReadSessionId)
        .With("ReadTime", readTimer.GetElapsedTime());

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
    const auto dynamicConfig = Context_->DynamicConfigManager->GetConfig()->DataNode;
    const auto maxBytesPerRead = dynamicConfig->MaxBytesPerRead.value_or(config->MaxBytesPerRead);
    const auto maxBlocksPerRead = dynamicConfig->MaxBlocksPerRead.value_or(config->MaxBlocksPerRead);

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

        YT_TLOG_TRACE("Block entry")
            .With("EntryIndex", entryIndex)
            .With("Block", entry.BlockIndex)
            .With("Cached", entry.Cached)
            .With("BeginOffset", entry.BeginOffset)
            .With("EndOffset", entry.EndOffset);

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
                        session->Entries[entryIndex].Block.Store(std::move(block));
                    })));
                continue;
            }
        }

        diskFetchNeeded = true;
        pendingDataSize += blockInfo.Size;
        pendingBlockCount += 1;

        if (pendingDataSize >= maxBytesPerRead ||
            pendingBlockCount >= maxBlocksPerRead)
        {
            session->EntryCount = entryIndex + 1;
            YT_TLOG_DEBUG("Read session trimmed due to read constraints")
                .With("PendingDataSize", pendingDataSize)
                .With("PendingBlockCount", pendingBlockCount)
                .With("TrimmedBlockCount", session->EntryCount);
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
            YT_TLOG_DEBUG("Disk read throttling is active")
                .With("PendingDataSize", pendingDataSize)
                .With("WorkloadDescriptor", session->Options.WorkloadDescriptor);
            auto throttleFuture = outThrottler->Throttle(pendingDataSize);
            session->Futures.push_back(throttleFuture.Apply(readCallback.AsyncVia(session->Invoker)));
        }
    }

    if (session->Options.FailSessionAtReadBlocksDeadline) {
        session->SessionDeadlineFuture = TDelayedExecutor::MakeDelayed(
            session->Options.ReadBlocksDeadline - TInstant::Now(),
            session->Invoker)
            .Apply(BIND([
                weakSession = MakeWeak(session),
                weakThis = MakeWeak(this)
            ] (const TError& error) {
                if (!error.IsOK()) {
                    return;
                }

                auto session = weakSession.Lock();
                auto this_ = weakThis.Lock();
                if (!session || !this_) {
                    return;
                }

                this_->FailSession(session, TError(NYT::EErrorCode::Timeout, "Session timeouted"));
            }));
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
            TError(NYT::EErrorCode::Canceled, "Session canceled").With(error));
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
        YT_TLOG_DEBUG("Read session aborted due to memory pressure");
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

    // TODO(don-dron): Add resource acquiring (memory, cpu, net etc).
    auto fairShareSlotOrError = Location_->AddFairShareQueueSlot(
        alignedPendingDataSize,
        {},
        CreateHierarchyLevels(Location_->BuildFairShareTags(
            session->Options.WorkloadDescriptor.Category,
            session->Options.FairShareState)));

    YT_VERIFY(fairShareSlotOrError.IsOK());

    session->FairShareSlot = fairShareSlotOrError.Value();

    session->LocationMemoryGuard.Store(Location_->AcquireLocationMemory(
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
            YT_TLOG_DEBUG("Stopping run due to large gap")
                .With("GapBlocks", FormatBlocks(previousEntry->BlockIndex + 1, entry.BlockIndex + 1))
                .WithFormat("GapBlockOffsets", "[%v,%v)", previousEntry->EndOffset, entry.BeginOffset)
                .With("GapBlockCount", entry.BlockIndex - previousEntry->BlockIndex - 1)
                .With("GapSize", readGapSize);
            break;
        } else if (readGapSize > 0) {
            YT_TLOG_DEBUG("Coalesced read gap")
                .With("GapBlocks", FormatBlocks(previousEntry->BlockIndex + 1, entry.BlockIndex))
                .WithFormat("GapBlockOffsets", "[%v,%v)", previousEntry->EndOffset, entry.BeginOffset)
                .With("GapBlockCount", entry.BlockIndex - previousEntry->BlockIndex - 1)
                .With("GapSize", readGapSize);
            for (int index = previousEntry->BlockIndex + 1; index < entry.BlockIndex; index++) {
                const auto& info = blocksExt->Blocks[index];

                auto blockId = TBlockId(Id_, index);
                auto cookie = blockCache->GetBlockCookie(blockId, session->Options.BlockType);

                TReadBlockSetSession::TBlockEntry blockEntry;
                blockEntry.BlockIndex = index;
                blockEntry.Cached = !cookie->IsActive();
                blockEntry.Cookie = std::move(cookie);
                blockEntry.BeginOffset = info.Offset;
                blockEntry.EndOffset = info.Offset + info.Size;

                EmplaceOrCrash(blockIndexToEntry, index, std::move(blockEntry));
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

    switch (session->Options.ReadIORequestsMode) {
        case EReadIORequestsMode::Sequential:
            DoReadBlockSetSequentially(session, std::move(readBlocksRequests), 0);
            break;
        case EReadIORequestsMode::Batched:
            DoReadBlockSetInBatches(session, std::move(readBlocksRequests));
            break;
        case EReadIORequestsMode::Parallel:
            DoReadBlockSetInParallel(session, std::move(readBlocksRequests));
            break;
    }
}

TFuture<void> TBlobChunkBase::ReadBlocks(
    const TReadBlockSetSessionPtr& session,
    TReadBlocksRequest readBlocksRequest)
{
    YT_TLOG_DEBUG("Started reading blob chunk blocks")
        .With("ChunkId", Id_)
        .With("Blocks", FormatBlocks(
            readBlocksRequest.FirstBlockIndex,
            readBlocksRequest.FirstBlockIndex + readBlocksRequest.BlocksToRead - 1))
        .With("LocationId", Location_->GetId())
        .With("LocationUuid", Location_->GetUuid())
        .With("LocationIndex", Location_->GetIndex())
        .With("WorkloadDescriptor", session->Options.WorkloadDescriptor)
        .With("ReadSessionId", session->Options.ReadSessionId)
        .With("GapBlockCount", readBlocksRequest.BlocksToRead - (readBlocksRequest.EndEntryIndex - readBlocksRequest.BeginEntryIndex))
        .With("LeftBorder", readBlocksRequest.FirstBlockIndex)
        .With("RightBorder", readBlocksRequest.FirstBlockIndex + readBlocksRequest.BlocksToRead - 1);

    YT_VERIFY(readBlocksRequest.FirstBlockIndex >= 0);
    YT_VERIFY(readBlocksRequest.BlocksToRead > 0);

    if (auto delay = Location_->GetDelayBeforeBlobChunkRead()) {
        YT_TLOG_DEBUG("Delaying blob chunk read")
            .With("ChunkId", Id_)
            .With("Delay", *delay);
        return TDelayedExecutor::MakeDelayed(*delay).Apply(
            BIND(
                &TBlobChunkBase::DoReadBlocks,
                MakeStrong(this),
                session,
                Passed(std::move(readBlocksRequest)))
                .AsyncVia(session->Invoker));
    }

    return DoReadBlocks(session, std::move(readBlocksRequest));
}

TFuture<void> TBlobChunkBase::DoReadBlocks(
    const TReadBlockSetSessionPtr& session,
    TReadBlocksRequest readBlocksRequest)
{
    YT_ASSERT_INVOKER_AFFINITY(session->Invoker);

    auto reader = GetReader();

    YT_VERIFY(session->FairShareSlot);
    auto asyncBlocks = reader->ReadBlocks(
        session->Options,
        readBlocksRequest.FirstBlockIndex,
        readBlocksRequest.BlocksToRead,
        session->FairShareSlot->GetSlot()->GetSlotId(),
        session->Options.FairShareState,
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

i64 TBlobChunkBase::CalculateReadDataSize(
    const TReadBlockSetSessionPtr& session,
    const TReadBlocksRequest& request)
{
    i64 readDataSize = 0;

    for (int blockIndex = request.FirstBlockIndex;
        blockIndex < request.FirstBlockIndex + request.BlocksToRead;
        ++blockIndex)
    {
        readDataSize += session->BlocksExt->Blocks[blockIndex].Size;
    }

    return readDataSize;
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
        YT_TLOG_DEBUG("Read session trimmed due to deadline or cancellation")
            .With("Deadline", session->Options.ReadBlocksDeadline)
            .With("IsCanceled", session->DiskFetchPromise.IsCanceled());
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

    session->DiskFetchPromise.TrySetFrom(
        AllSucceeded(RunReadBlocksRequests(session, std::move(requests))));
}

void TBlobChunkBase::DoReadBlockSetInBatches(
    const TReadBlockSetSessionPtr& session,
    std::vector<TReadBlocksRequest> requests)
{
    YT_ASSERT_INVOKER_AFFINITY(session->Invoker);

    auto state = New<TReadBlockSetBatchState>();
    state->Requests = std::move(requests);
    TryScheduleReadBlocks(session, state);
}

TBlobChunkBase::TReadBlocksRequestBatch TBlobChunkBase::GetReadBlocksRequestsToRun(
    const TReadBlockSetSessionPtr& session,
    const TReadBlockSetBatchStatePtr& state)
{
    YT_VERIFY(session->Options.MaxInFlightReadRequestCount > 0);
    YT_VERIFY(session->Options.MaxInFlightReadDataSize > 0);

    auto guard = Guard(state->SpinLock);
    TReadBlocksRequestBatch batch;

    if (!state->Stopped &&
        state->NextRequestIndex < std::ssize(state->Requests) &&
        (session->DiskFetchPromise.IsCanceled() || TInstant::Now() > session->Options.ReadBlocksDeadline))
    {
        state->Stopped = true;
        batch.FirstEntryIndexToFail = state->Requests[state->NextRequestIndex].BeginEntryIndex;
    }

    while (!state->Stopped && state->NextRequestIndex < std::ssize(state->Requests)) {
        auto& request = state->Requests[state->NextRequestIndex];
        auto fitsRequestCount =
            state->InFlightRequestCount < session->Options.MaxInFlightReadRequestCount;
        auto fitsDataSize =
            state->InFlightReadDataSize + request.ReadDataSize <= session->Options.MaxInFlightReadDataSize;
        auto canStartOversizedRequest = state->InFlightRequestCount == 0;

        if (!fitsRequestCount || (!fitsDataSize && !canStartOversizedRequest)) {
            break;
        }

        ++state->NextRequestIndex;
        ++state->InFlightRequestCount;
        state->InFlightReadDataSize += request.ReadDataSize;
        batch.Requests.push_back(std::move(request));
    }

    return batch;
}

bool TBlobChunkBase::IsReadBlockSetBatchFinished(
    const TReadBlockSetBatchStatePtr& state)
{
    auto guard = Guard(state->SpinLock);
    return
        (state->Stopped || state->NextRequestIndex == std::ssize(state->Requests)) &&
        state->InFlightRequestCount == 0;
}

std::vector<TFuture<void>> TBlobChunkBase::RunReadBlocksRequests(
    const TReadBlockSetSessionPtr& session,
    std::vector<TReadBlocksRequest> requests)
{
    i64 additionalMemory = 0;
    for (const auto& request : requests) {
        additionalMemory += CalculateAdditionalMemory(request);
    }

    session->LocationMemoryGuard.Transform([additionalMemory] (TLocationMemoryGuard& guard) {
        if (guard) {
            guard.IncreaseSize(additionalMemory);
        }
    });

    std::vector<TFuture<void>> readRequests;
    readRequests.reserve(requests.size());
    for (auto& request : requests) {
        readRequests.push_back(ReadBlocks(session, std::move(request)));
    }

    return readRequests;
}

void TBlobChunkBase::TryScheduleReadBlocks(
    const TReadBlockSetSessionPtr& session,
    const TReadBlockSetBatchStatePtr& state)
{
    YT_ASSERT_INVOKER_AFFINITY(session->Invoker);

    auto batch = GetReadBlocksRequestsToRun(session, state);

    if (batch.FirstEntryIndexToFail) {
        YT_TLOG_DEBUG("Read session trimmed due to deadline or cancellation")
            .With("Deadline", session->Options.ReadBlocksDeadline)
            .With("IsCanceled", session->DiskFetchPromise.IsCanceled());

        auto error = TError(NChunkClient::EErrorCode::ReaderTimeout, "Read session trimmed due to deadline");
        for (auto index = *batch.FirstEntryIndexToFail; index < session->EntryCount; ++index) {
            if (!session->Entries[index].Cached && session->Entries[index].Cookie) {
                session->Entries[index].Cookie->SetBlock(error);
            }
        }
    }

    std::vector<i64> readDataSizes;
    readDataSizes.reserve(batch.Requests.size());
    for (const auto& request : batch.Requests) {
        readDataSizes.push_back(request.ReadDataSize);
    }

    auto readRequests = RunReadBlocksRequests(session, std::move(batch.Requests));
    for (auto&& [readRequest, readDataSize] : Zip(readRequests, readDataSizes)) {
        YT_UNUSED_FUTURE(readRequest.Apply(
            BIND(
                &TBlobChunkBase::OnBatchedReadBlocksCompleted,
                MakeStrong(this),
                session,
                state,
                readDataSize)
                .AsyncVia(session->Invoker)));
    }

    if (IsReadBlockSetBatchFinished(state)) {
        session->DiskFetchPromise.TrySet();
    }
}

void TBlobChunkBase::OnBatchedReadBlocksCompleted(
    const TReadBlockSetSessionPtr& session,
    const TReadBlockSetBatchStatePtr& state,
    i64 readDataSize)
{
    YT_ASSERT_INVOKER_AFFINITY(session->Invoker);

    {
        auto guard = Guard(state->SpinLock);
        YT_VERIFY(state->InFlightRequestCount > 0);
        YT_VERIFY(state->InFlightReadDataSize >= readDataSize);

        --state->InFlightRequestCount;
        state->InFlightReadDataSize -= readDataSize;
    }

    TryScheduleReadBlocks(session, state);
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
            readBlocksRequest->ReadDataSize = CalculateReadDataSize(session, *readBlocksRequest);
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
            .With(TError(blocksOrError));
        if (blocksOrError.FindMatching(NChunkClient::EErrorCode::IncorrectChunkFileChecksum)) {
            if (ShouldSyncOnClose()) {
                Location_->ScheduleDisable(error);
            } else {
                YT_TLOG_WARNING("Block in chunk without \"sync_on_close\" has checksum mismatch, removing it")
                    .With("ChunkId", Id_)
                    .With("LocationId", Location_->GetId())
                    .With("LocationUuid", Location_->GetUuid())
                    .With("LocationIndex", Location_->GetIndex());

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

            entry.Block.Store(block);
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

    YT_TLOG_DEBUG("Finished reading blob chunk blocks")
        .With("ChunkId", Id_)
        .With("Blocks", FormatBlocks(firstBlockIndex, firstBlockIndex + blocksToRead - 1))
        .With("LocationId", Location_->GetId())
        .With("LocationUuid", Location_->GetUuid())
        .With("LocationIndex", Location_->GetIndex())
        .With("BytesRead", bytesRead)
        .With("ReadTime", readTime)
        .With("UsefulBlockSize", usefulBlockSize)
        .With("UsefulBlockCount", usefulBlockCount)
        .With("PopulateCacheTime", populateCacheTime)
        .With("ReadSessionId", session->Options.ReadSessionId)
        .With("GapBlockSize", gapBlockSize)
        .With("GapBlockCount", gapBlockCount);

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
                        YT_TLOG_ALERT("Long live read session")
                            .With("ChunkId", chunkId)
                            .With("FutureCount", session->Futures.size())
                            .With("DiskPromise", static_cast<bool>(session->DiskFetchPromise))
                            .With("DiskPromiseIsSet", session->DiskFetchPromise && session->DiskFetchPromise.IsSet())
                            .With("DiskPromiseIsCanceled", session->DiskFetchPromise && session->DiskFetchPromise.IsCanceled())
                            .With("EntryCount", session->EntryCount)
                            .With("BlocksExtLoaded", session->BlocksExt != nullptr)
                            .With("SessionPromiseIsCanceled", session->SessionPromise.IsCanceled())
                            .With("Finished", session->Finished)
                            .With("ReadLockCounter", session->ChunkReadGuard->GetChunk()->GetReadLockCounter());
                    }
                } else {
                    YT_TLOG_DEBUG("Session completed before timeout")
                        .With("ChunkId", chunkId)
                        .With(error);
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
                entry.Block.Store(std::move(block));
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
        return {};
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

            YT_TLOG_DEBUG("Chunk reader prepared to read fragments")
                .With("ChunkId", Id_)
                .With("LocationId", Location_->GetId())
                .With("LocationUuid", Location_->GetUuid())
                .With("LocationIndex", Location_->GetIndex());
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
