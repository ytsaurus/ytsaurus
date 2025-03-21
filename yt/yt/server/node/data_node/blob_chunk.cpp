#include "blob_chunk.h"

#include "blob_reader_cache.h"
#include "chunk_meta_manager.h"
#include "chunk_store.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/io/chunk_file_reader.h>
#include <yt/yt/server/lib/io/chunk_file_writer.h>
#include <yt/yt/server/lib/io/io_engine_base.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/memory/memory_usage_tracker.h>

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

static constexpr auto& Logger = DataNodeLogger;

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
            session->SessionAliveCheckFuture.Cancel(TError("Read meta session completed"));
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

    YT_LOG_DEBUG("Chunk reader released (ChunkId: %v, LocationId: %v)",
        Id_,
        Location_->GetId());
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
    session->LocationMemoryGuard.Release();

    if (session->FairShareSlot) {
        Location_->RemoveFairShareQueueSlot(session->FairShareSlot);
    }
}

void TBlobChunkBase::FailSession(const TReadBlockSetSessionPtr& session, const TError& error)
{
    YT_ASSERT_INVOKER_AFFINITY(session->Invoker);

    if (session->Finished.exchange(true)) {
        return;
    }

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

    session->LocationMemoryGuard.Release();

    if (session->FairShareSlot) {
        Location_->RemoveFairShareQueueSlot(session->FairShareSlot);
    }
}

void TBlobChunkBase::DoReadMeta(
    const TReadMetaSessionPtr& session,
    TCachedChunkMetaCookie cookie)
{
    YT_ASSERT_INVOKER_AFFINITY(Context_->StorageHeavyInvoker);

    YT_LOG_DEBUG("Started reading chunk meta (ChunkId: %v, LocationId: %v, WorkloadDescriptor: %v, ReadSessionId: %v)",
        Id_,
        Location_->GetId(),
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

        auto finally = Finally([fairShareQueueSlot = fairShareQueueSlot, location = Location_] {
            location->RemoveFairShareQueueSlot(fairShareQueueSlot.Value());
        });

        meta = WaitFor(reader->GetMeta(session->Options, {})
            .WithDeadline(session->Options.ReadMetaDeadLine))
            .ValueOrThrow();
    } catch (const std::exception& ex) {
        auto error = TError(ex);
        if (error.FindMatching(NChunkClient::EErrorCode::BrokenChunkFileMeta)) {
            if (ShouldSyncOnClose()) {
                Location_->ScheduleDisable(error);
            } else {
                YT_LOG_WARNING(error, "Error reading chunk meta, removing it (ChunkId: %v, LocationId: %v)",
                    Id_,
                    Location_->GetId());

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
    YT_LOG_DEBUG("Finished reading chunk meta (ChunkId: %v, LocationId: %v, ReadSessionId: %v, ReadTime: %v)",
        Id_,
        Location_->GetId(),
        session->Options.ReadSessionId,
        readTimer.GetElapsedTime());

    Context_->ChunkMetaManager->EndInsertCachedMeta(std::move(cookie), std::move(meta));
}

void TBlobChunkBase::OnBlocksExtLoaded(
    const TReadBlockSetSessionPtr& session,
    const TBlocksExtPtr& blocksExt) noexcept
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

        YT_LOG_TRACE("Block entry (EntryIndex: %v, BlockIndex: %v, Cached: %v, BeginOffset: %v, EndOffset: %v)",
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
            entry.Cookie = blockCache->GetBlockCookie(blockId, EBlockType::CompressedData);

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

    auto alignedPendingDataSize = AlignUp<i64>(pendingDataSize, DefaultPageSize);

    if (diskFetchNeeded) {
        session->DiskFetchPromise = NewPromise<void>();
        session->Futures.push_back(session->DiskFetchPromise.ToFuture());

        auto readCallback = BIND([=, this, this_ = MakeStrong(this)] {
            DoReadSession(session, alignedPendingDataSize);
        });

        const auto& outThrottler = Location_->GetOutThrottler(session->Options.WorkloadDescriptor);
        if (outThrottler->TryAcquire(alignedPendingDataSize)) {
            session->Invoker->Invoke(std::move(readCallback));
        } else {
            YT_LOG_DEBUG("Disk read throttling is active (PendingDataSize: %v, WorkloadDescriptor: %v)",
                alignedPendingDataSize,
                session->Options.WorkloadDescriptor);
            auto throttleFuture = outThrottler->Throttle(alignedPendingDataSize);
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

    session->SessionPromise.OnCanceled(BIND([this, this_ = MakeStrong(this), weak = MakeWeak(session)] (const TError& error) {
        if (auto session = weak.Lock()) {
            FailSession(
                session,
                TError(NYT::EErrorCode::Canceled, "Session canceled") << error);
        }
    }).Via(session->Invoker));
}

void TBlobChunkBase::DoReadSession(
    const TBlobChunkBase::TReadBlockSetSessionPtr& session,
    i64 pendingDataSize)
{
    YT_ASSERT_INVOKER_AFFINITY(session->Invoker);

    const auto& memoryTracker = Location_->GetReadMemoryTracker();
    auto memoryGuardOrError = TMemoryUsageTrackerGuard::TryAcquire(memoryTracker, pendingDataSize);
    if (!memoryGuardOrError.IsOK()) {
        YT_LOG_DEBUG("Read session aborted due to memory pressure");
        Location_->ReportThrottledRead();
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
        pendingDataSize,
        {},
        CreateHierarchyLevels(session->Options.FairShareTags));

    YT_VERIFY(fairShareSlotOrError.IsOK());

    session->FairShareSlot = fairShareSlotOrError.Value();

    session->LocationMemoryGuard = Location_->AcquireLocationMemory(
        std::move(memoryGuardOrError.Value()),
        EIODirection::Read,
        session->Options.WorkloadDescriptor,
        pendingDataSize);

    DoReadBlockSet(session);
}

void TBlobChunkBase::DoReadBlockSet(const TReadBlockSetSessionPtr& session) noexcept
{
    YT_ASSERT_INVOKER_AFFINITY(session->Invoker);

    if (session->CurrentEntryIndex > 0 && TInstant::Now() > session->Options.ReadBlocksDeadline) {
        YT_LOG_DEBUG(
            "Read session trimmed due to deadline (Deadline: %v, TrimmedBlockCount: %v)",
            session->Options.ReadBlocksDeadline,
            session->CurrentEntryIndex);
        session->DiskFetchPromise.TrySet();
        return;
    }

    while (true) {
        if (session->CurrentEntryIndex >= session->EntryCount) {
            session->DiskFetchPromise.TrySet();
            return;
        }

        if (!session->Entries[session->CurrentEntryIndex].Cached) {
            break;
        }

        ++session->CurrentEntryIndex;
    }

    YT_VERIFY(session->CurrentEntryIndex < session->EntryCount);
    YT_VERIFY(!session->Entries[session->CurrentEntryIndex].Cached);

    // Extract the maximum run of block. Blocks should be contiguous or at least have pretty small gap between them
    // (if gap is small enough, coalesced read including gap blocks is more efficient than making two separate runs).

    int beginEntryIndex = session->CurrentEntryIndex;
    int endEntryIndex = session->CurrentEntryIndex + 1;
    int firstBlockIndex = session->Entries[beginEntryIndex].BlockIndex;
    const TReadBlockSetSession::TBlockEntry* previousEntry = &session->Entries[beginEntryIndex];

    YT_LOG_TRACE("Starting run at block (EntryIndex: %v, BlockIndex: %v)",
        beginEntryIndex,
        firstBlockIndex);

    THashMap<int, TReadBlockSetSession::TBlockEntry> blockIndexToEntry;

    i64 readSize = previousEntry->EndOffset - previousEntry->BeginOffset;

    const auto& blocksExt = session->BlocksExt;
    const auto& blockCache = session->Options.BlockCache;

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
            YT_LOG_TRACE("Stopping run due to large gap (GapBlockIndexes: %v-%v, GapBlockOffsets: [%v,%v), GapBlockCount: %v, GapSize: %v)",
                previousEntry->BlockIndex + 1,
                entry.BlockIndex - 1,
                previousEntry->EndOffset,
                entry.BeginOffset,
                entry.BlockIndex - previousEntry->BlockIndex - 1,
                readGapSize);
            break;
        } else if (readGapSize > 0) {
            YT_LOG_TRACE("Coalesced read gap (GapBlockIndexes: %v-%v, GapBlockOffsets: [%v,%v), GapBlockCount: %v, GapSize: %v)",
                previousEntry->BlockIndex + 1,
                entry.BlockIndex,
                previousEntry->EndOffset,
                entry.BeginOffset,
                entry.BlockIndex - previousEntry->BlockIndex - 1,
                readGapSize);
            for (int index = previousEntry->BlockIndex + 1; index < entry.BlockIndex; index++) {
                const auto& info = blocksExt->Blocks[index];

                auto blockId = TBlockId(Id_, index);
                auto cookie = blockCache->GetBlockCookie(blockId, EBlockType::CompressedData);

                EmplaceOrCrash(blockIndexToEntry, index, TReadBlockSetSession::TBlockEntry{
                    .BlockIndex = index,
                    .Cached = !cookie->IsActive(),
                    .Cookie = std::move(cookie),
                    .BeginOffset = info.Offset,
                    .EndOffset = info.Offset + info.Size,
                });

                readSize += info.Size;
            }
        }

        readSize += entry.EndOffset - entry.BeginOffset;
        previousEntry = &entry;
        ++endEntryIndex;
    }

    int blocksToRead = previousEntry->BlockIndex - firstBlockIndex + 1;

    int leftBlock = firstBlockIndex;
    int rightBlock = previousEntry->BlockIndex;

    if (readSize < Location_->GetCoalescedReadMaxGapSize()) {
        auto diff = Location_->GetCoalescedReadMaxGapSize() - readSize;
        auto cachedBlocks = blockCache->GetCachedBlocksByChunkId(Id_, NChunkClient::EBlockType::CompressedData);

        THashSet<int> cachedBlockIndexes;
        cachedBlockIndexes.reserve(cachedBlocks.size());

        for (const auto& info : cachedBlocks) {
            cachedBlockIndexes.emplace(info.BlockIndex);
        }

        int leftSize = 0;
        int rightSize = 0;

        while (leftSize < diff) {
            if (leftBlock - 1 >= 0) {
                leftBlock--;
                leftSize += blocksExt->Blocks[leftBlock].Size;
            } else {
                break;
            }
        }

        while (rightSize < diff) {
            if (rightBlock + 1 < std::ssize(blocksExt->Blocks)) {
                rightBlock++;
                rightSize += blocksExt->Blocks[rightBlock].Size;
            } else {
                break;
            }
        }

        auto collapseLeft = [&] {
            leftSize -= blocksExt->Blocks[leftBlock].Size;
            leftBlock++;
        };

        auto collapseRight = [&] {
            rightSize += blocksExt->Blocks[rightBlock].Size;
            rightBlock--;
        };

        while (diff < leftSize + rightSize && (leftBlock < firstBlockIndex || previousEntry->BlockIndex < rightBlock)) {
            if (leftBlock >= firstBlockIndex) {
                collapseRight();
            } else if (rightBlock <= previousEntry->BlockIndex) {
                collapseLeft();
            } else if (cachedBlockIndexes.contains(rightBlock)) {
                collapseRight();
            } else if (cachedBlockIndexes.contains(leftBlock)) {
                collapseLeft();
            } else if (blocksExt->Blocks[leftBlock].Size < blocksExt->Blocks[rightBlock].Size) {
                collapseRight();
            } else {
                collapseLeft();
            }
        }

        for (int index = leftBlock; index <= rightBlock; index++) {
            if (index >= firstBlockIndex && index <= previousEntry->BlockIndex) {
                continue;
            }

            const auto& info = blocksExt->Blocks[index];

            auto blockId = TBlockId(Id_, index);
            auto cookie = blockCache->GetBlockCookie(blockId, EBlockType::CompressedData);

            EmplaceOrCrash(blockIndexToEntry, index, TReadBlockSetSession::TBlockEntry{
                .BlockIndex = index,
                .Cached = !cookie->IsActive(),
                .Cookie = std::move(cookie),
                .BeginOffset = info.Offset,
                .EndOffset = info.Offset + info.Size,
            });

            readSize += info.Size;
        }
    }

    i64 additionalMemory = 0;

    for (auto& [blockIndex, entry] : blockIndexToEntry) {
        additionalMemory += entry.EndOffset - entry.BeginOffset;
    }

    if (session->LocationMemoryGuard) {
        session->LocationMemoryGuard.IncreaseSize(additionalMemory);
    }

    YT_LOG_DEBUG(
        "Started reading blob chunk blocks (BlockIds: %v:%v-%v, "
        "LocationId: %v, WorkloadDescriptor: %v, "
        "ReadSessionId: %v, GapBlockCount: %v, "
        "LeftBorder: %v, RightBorder: %v, Size: %v)",
        Id_,
        firstBlockIndex,
        previousEntry->BlockIndex,
        Location_->GetId(),
        session->Options.WorkloadDescriptor,
        session->Options.ReadSessionId,
        blocksToRead - (endEntryIndex - beginEntryIndex),
        leftBlock,
        rightBlock,
        readSize);

    firstBlockIndex = leftBlock;
    blocksToRead = rightBlock - leftBlock + 1;

    session->ReadTimer.emplace();

    try {
        auto reader = GetReader();

        YT_VERIFY(session->FairShareSlot);
        auto asyncBlocks = reader->ReadBlocks(
            session->Options,
            firstBlockIndex,
            blocksToRead,
            session->FairShareSlot->GetSlotId(),
            session->BlocksExt);

        asyncBlocks.Subscribe(
            BIND(
                &TBlobChunkBase::OnBlocksRead,
                MakeStrong(this),
                session,
                firstBlockIndex,
                blocksToRead,
                beginEntryIndex,
                endEntryIndex,
                Passed(std::move(blockIndexToEntry)))
                .Via(session->Invoker));
    } catch (const std::exception& ex) {
        FailSession(session, ex);
    }
}

void TBlobChunkBase::OnBlocksRead(
    const TReadBlockSetSessionPtr& session,
    int firstBlockIndex,
    int blocksToRead,
    int beginEntryIndex,
    int endEntryIndex,
    THashMap<int, TReadBlockSetSession::TBlockEntry> blockIndexToEntry,
    const TErrorOr<std::vector<TBlock>>& blocksOrError) noexcept
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
                YT_LOG_WARNING("Block in chunk without \"sync_on_close\" has checksum mismatch, removing it (ChunkId: %v, LocationId: %v)",
                    Id_,
                    Location_->GetId());

                if (const auto& chunkStore = Location_->GetChunkStore()) {
                    YT_UNUSED_FUTURE(chunkStore->RemoveChunk(this));
                } else {
                    YT_UNUSED_FUTURE(ScheduleRemove());
                }
            }
        } else if (blocksOrError.FindMatching(NFS::EErrorCode::IOError)) {
            Location_->ScheduleDisable(error);
        }
        FailSession(session, error);
        return;
    }

    auto readTime = session->ReadTimer->GetElapsedTime();

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

    YT_LOG_DEBUG("Finished reading blob chunk blocks (BlockIds: %v:%v-%v, LocationId: %v, BytesRead: %v, "
        "ReadTime: %v, UsefulBlockSize: %v, UsefulBlockCount: %v, PopulateCacheTime: %v, ReadSessionId: %v, "
        "GapBlockSize: %v, GapBlockCount: %v)",
        Id_,
        firstBlockIndex,
        firstBlockIndex + blocksToRead - 1,
        Location_->GetId(),
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

    session->CurrentEntryIndex = endEntryIndex;

    DoReadBlockSet(session);
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
            auto block = options.BlockCache->FindBlock(blockId, EBlockType::CompressedData);
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
        OnBlocksExtLoaded(session, blocksExt);
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

            YT_LOG_DEBUG("Chunk reader prepared to read fragments (ChunkId: %v, LocationId: %v)",
                Id_,
                Location_->GetId());
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
