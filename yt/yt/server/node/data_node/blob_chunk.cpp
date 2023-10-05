#include "blob_chunk.h"

#include "private.h"
#include "blob_reader_cache.h"
#include "chunk_meta_manager.h"
#include "chunk_store.h"

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/io/chunk_file_reader.h>
#include <yt/yt/server/lib/io/chunk_file_writer.h>
#include <yt/yt/server/lib/io/io_engine_base.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>
#include <yt/yt/ytlib/misc/memory_reference_tracker.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/memory_reference_tracker.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/profiling/timing.h>

#include <util/system/align.h>

namespace NYT::NDataNode {

using namespace NConcurrency;
using namespace NThreading;
using namespace NClusterNode;
using namespace NNodeTrackerClient;
using namespace NIO;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

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
    VERIFY_THREAD_AFFINITY_ANY();

    return Info_;
}

bool TBlobChunkBase::IsActive() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return false;
}

TFuture<TRefCountedChunkMetaPtr> TBlobChunkBase::ReadMeta(
    const TChunkReadOptions& options,
    const std::optional<std::vector<int>>& extensionTags)
{
    VERIFY_THREAD_AFFINITY_ANY();

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
    VERIFY_THREAD_AFFINITY_ANY();

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
    VERIFY_THREAD_AFFINITY_ANY();
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
    VERIFY_WRITER_SPINLOCK_AFFINITY(LifetimeLock_);
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

TSharedRef TBlobChunkBase::WrapBlockWithDelayedReferenceHolder(TSharedRef&& rawReference, TDuration delayBeforeFree)
{
    YT_LOG_DEBUG("Simulate delay before blob read session block free (BlockSize: %v, Delay: %v)", rawReference.Size(), delayBeforeFree);

    auto underlyingHolder = rawReference.GetHolder();
    auto underlyingReference = TSharedRef(rawReference, std::move(underlyingHolder));
    return TSharedRef(
        rawReference,
        New<TDelayedReferenceHolder>(std::move(underlyingReference), delayBeforeFree, GetCurrentInvoker()));
}

void TBlobChunkBase::CompleteSession(const TReadBlockSetSessionPtr& session)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (session->Finished.exchange(true)) {
        return;
    }

    ProfileReadBlockSetLatency(session);

    auto guard = session->PendingIOGuard.MoveMemoryTrackerGuard();
    auto delayBeforeFree = Location_->GetDelayBeforeBlobSessionBlockFree();

    std::vector<TBlock> blocks;
    for (int entryIndex = 0; entryIndex < session->EntryCount; ++entryIndex) {
        auto& entry = session->Entries[entryIndex];
        auto originalEntryIndex = entry.EntryIndex;
        if (std::ssize(blocks) <= originalEntryIndex) {
            blocks.resize(originalEntryIndex + 1);
        }

        auto block = std::move(entry.Block);

        if (session->Options.TrackMemoryAfterSessionCompletion) {
            block.Data = TrackMemory(session->Options.MemoryReferenceTracker, std::move(block.Data), true);

            if (delayBeforeFree) {
                block.Data = WrapBlockWithDelayedReferenceHolder(std::move(block.Data), *delayBeforeFree);
            }
        }

        blocks[originalEntryIndex] = std::move(block);
    }

    session->SessionPromise.TrySet(std::move(blocks));
    session->PendingIOGuard.Release();
}

void TBlobChunkBase::FailSession(const TReadBlockSetSessionPtr& session, const TError& error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (session->Finished.exchange(true)) {
        return;
    }

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

    session->PendingIOGuard.Release();
}

void TBlobChunkBase::DoReadMeta(
    const TReadMetaSessionPtr& session,
    TCachedChunkMetaCookie cookie)
{
    VERIFY_INVOKER_AFFINITY(Context_->StorageHeavyInvoker);

    YT_LOG_DEBUG("Started reading chunk meta (ChunkId: %v, LocationId: %v, WorkloadDescriptor: %v, ReadSessionId: %v)",
        Id_,
        Location_->GetId(),
        session->Options.WorkloadDescriptor,
        session->Options.ReadSessionId);

    TRefCountedChunkMetaPtr meta;
    TWallTimer readTimer;
    try {
        auto reader = GetReader();
        meta = WaitFor(reader->GetMeta(session->Options))
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
        cookie.Cancel(error);
        return;
    }

    auto readTime = readTimer.GetElapsedTime();

    session->Options.ChunkReaderStatistics->MetaReadFromDiskTime.fetch_add(
        DurationToValue(readTime),
        std::memory_order::relaxed);

    auto& performanceCounters = Location_->GetPerformanceCounters();
    performanceCounters.BlobChunkMetaReadTime.Record(readTime);

    YT_LOG_DEBUG("Finished reading chunk meta (ChunkId: %v, LocationId: %v, ReadSessionId: %v, ReadTime: %v)",
        Id_,
        Location_->GetId(),
        session->Options.ReadSessionId,
        readTime);

    Context_->ChunkMetaManager->EndInsertCachedMeta(std::move(cookie), std::move(meta));
}

void TBlobChunkBase::OnBlocksExtLoaded(
    const TReadBlockSetSessionPtr& session,
    const NIO::TBlocksExtPtr& blocksExt)
{
    VERIFY_THREAD_AFFINITY_ANY();

    // Run async cache lookup.
    i64 pendingDataSize = 0;
    int pendingBlockCount = 0;
    bool diskFetchNeeded = false;

    const auto& config = Context_->DataNodeConfig;

    session->BlocksExt = blocksExt;

    for (int entryIndex = 0; entryIndex < session->EntryCount; ++entryIndex) {
        auto& entry = session->Entries[entryIndex];
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
        }));

    session->SessionPromise.OnCanceled(BIND([session] (const TError& error) {
        FailSession(
            session,
            TError(NYT::EErrorCode::Canceled, "Session canceled") << error);
    }));
}

void TBlobChunkBase::DoReadSession(
    const TBlobChunkBase::TReadBlockSetSessionPtr& session,
    i64 pendingDataSize)
{
    VERIFY_INVOKER_AFFINITY(session->Invoker);

    const auto& memoryTracker = Location_->GetReadMemoryTracker();
    auto memoryGuardOrError = TMemoryUsageTrackerGuard::TryAcquire(memoryTracker, pendingDataSize);
    if (!memoryGuardOrError.IsOK()) {
        YT_LOG_DEBUG("Read session aborted due to memory pressure");
        Location_->ReportThrottledRead();
        session->DiskFetchPromise.TrySet();
        return;
    }

    session->PendingIOGuard = Location_->AcquirePendingIO(
        std::move(memoryGuardOrError.Value()),
        EIODirection::Read,
        session->Options.WorkloadDescriptor,
        pendingDataSize);

    DoReadBlockSet(session);
}

void TBlobChunkBase::DoReadBlockSet(const TReadBlockSetSessionPtr& session)
{
    VERIFY_INVOKER_AFFINITY(session->Invoker);

    if (session->CurrentEntryIndex > 0 && TInstant::Now() > session->Options.Deadline) {
        YT_LOG_DEBUG(
            "Read session trimmed due to deadline (Deadline: %v, TrimmedBlockCount: %v)",
            session->Options.Deadline,
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
        }

        previousEntry = &entry;
        ++endEntryIndex;
    }
    int blocksToRead = previousEntry->BlockIndex - firstBlockIndex + 1;

    YT_LOG_DEBUG(
        "Started reading blob chunk blocks (BlockIds: %v:%v-%v, LocationId: %v, WorkloadDescriptor: %v, ReadSessionId: %v, GapBlockCount: %v)",
        Id_,
        firstBlockIndex,
        previousEntry->BlockIndex,
        Location_->GetId(),
        session->Options.WorkloadDescriptor,
        session->Options.ReadSessionId,
        blocksToRead - (endEntryIndex - beginEntryIndex));

    session->ReadTimer.emplace();

    try {
        auto reader = GetReader();

        auto asyncBlocks = reader->ReadBlocks(
            session->Options,
            firstBlockIndex,
            blocksToRead,
            session->BlocksExt);

        asyncBlocks.Subscribe(
            BIND(
                &TBlobChunkBase::OnBlocksRead,
                MakeStrong(this),
                session,
                firstBlockIndex,
                blocksToRead,
                beginEntryIndex,
                endEntryIndex)
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
    const TErrorOr<std::vector<TBlock>>& blocksOrError)
{
    VERIFY_INVOKER_AFFINITY(session->Invoker);

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
    for (int entryIndex = beginEntryIndex; entryIndex < endEntryIndex; ++entryIndex) {
        auto& entry = session->Entries[entryIndex];
        if (!entry.Cached) {
            auto relativeBlockIndex = entry.BlockIndex - firstBlockIndex;
            auto block = blocks[relativeBlockIndex];

            entry.Block = block;

            ++usefulBlockCount;
            usefulBlockSize += block.Size();
            if (entry.Cookie) {
                entry.Cookie->SetBlock(TCachedBlock(std::move(block)));
            }
        }
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
    VERIFY_THREAD_AFFINITY_ANY();

    auto blocksExt = FindCachedBlocksExt();
    if (!blocksExt) {
        return true;
    }

    return blocksExt->SyncOnClose;
}

bool TBlobChunkBase::IsReadable()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return !IsArtifactChunkId(GetId());
}

TFuture<std::vector<TBlock>> TBlobChunkBase::ReadBlockSet(
    const std::vector<int>& blockIndexes,
    const TChunkReadOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

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
        session->Options.MemoryReferenceTracker = options.MemoryReferenceTracker;
        session->Options.UseDedicatedAllocations = true;
        session->Options.TrackMemoryAfterSessionCompletion = options.TrackMemoryAfterSessionCompletion;
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
        CompleteSession(session);
        return session->SessionPromise.ToFuture();
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
            }));
    }

    return session->SessionPromise.ToFuture();
}

TFuture<std::vector<TBlock>> TBlobChunkBase::ReadBlockRange(
    int firstBlockIndex,
    int blockCount,
    const TChunkReadOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();
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

IIOEngine::TReadRequest TBlobChunkBase::MakeChunkFragmentReadRequest(
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
    VERIFY_INVOKER_AFFINITY(Location_->GetAuxPoolInvoker());

    Context_->BlobReaderCache->EvictReader(this);

    Location_->RemoveChunkFiles(Id_, force);
}

TFuture<void> TBlobChunkBase::AsyncRemove()
{
    VERIFY_THREAD_AFFINITY_ANY();

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
