#include "journal_chunk.h"

#include "config.h"
#include "private.h"
#include "journal_dispatcher.h"
#include "location.h"
#include "session.h"

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/hydra/changelog.h>
#include <yt/yt/server/lib/hydra/file_changelog.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/ref_counted_proto.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NDataNode {

using namespace NConcurrency;
using namespace NThreading;
using namespace NClusterNode;
using namespace NNode;
using namespace NIO;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NJournalClient;
using namespace NHydra;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

void UpdateMax(std::atomic<i64>& value, i64 candidate)
{
    auto current = value.load();
    while (current < candidate) {
        if (value.compare_exchange_weak(current, candidate)) {
            break;
        }
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TJournalChunk::TJournalChunk(
    TChunkContextPtr context,
    TStoreLocationPtr location,
    const TChunkDescriptor& descriptor)
    : TChunkBase(
        context,
        location,
        descriptor.Id)
    , StoreLocation_(location)
{
    FlushedRowCount_.store(descriptor.RowCount);
    DataSize_.store(descriptor.DiskSpace);
    Sealed_.store(descriptor.Sealed);
}

const TStoreLocationPtr& TJournalChunk::GetStoreLocation() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return StoreLocation_;
}

void TJournalChunk::SetActive(bool value)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    Active_.store(value);
}

bool TJournalChunk::IsActive() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Active_.load();
}

TChunkInfo TJournalChunk::GetInfo() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    TChunkInfo info;
    info.set_sealed(IsSealed());
    info.set_disk_space(GetDataSize());
    return info;
}

TFuture<TRefCountedChunkMetaPtr> TJournalChunk::ReadMeta(
    const TChunkReadOptions& options,
    const std::optional<std::vector<int>>& extensionTags)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto session = New<TReadMetaSession>();
    try {
        StartReadSession(session, options);
    } catch (const std::exception& ex) {
        return MakeFuture<TRefCountedChunkMetaPtr>(ex);
    }

    TMiscExt miscExt;
    miscExt.set_row_count(GetFlushedRowCount());
    miscExt.set_uncompressed_data_size(GetDataSize());
    miscExt.set_compressed_data_size(miscExt.uncompressed_data_size());
    miscExt.set_sealed(IsSealed());

    auto meta = New<TRefCountedChunkMeta>();
    meta->set_type(ToProto(EChunkType::Journal));
    meta->set_format(ToProto(EChunkFormat::JournalDefault));
    SetProtoExtension(meta->mutable_extensions(), miscExt);

    ProfileReadMetaLatency(session);

    return MakeFuture(FilterMeta(meta, extensionTags));
}

TFuture<std::vector<TBlock>> TJournalChunk::ReadBlockSet(
    const std::vector<int>& blockIndexes,
    const TChunkReadOptions& options)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (blockIndexes.empty()) {
        return MakeFuture(std::vector<TBlock>());
    }

    if (options.ReadCompleteBlockSetAndCache) {
        return BIND(
            &TJournalChunk::ReadCompleteBlockSetAndCache,
            MakeStrong(this),
            blockIndexes,
            options)
            .AsyncVia(Context_->StorageHeavyInvoker)
            .Run();
    }

    // Extract the initial contiguous segment of blocks.
    int firstBlockIndex = blockIndexes.front();
    int blockCount = 1;
    while (blockCount < std::ssize(blockIndexes) && blockIndexes[blockCount] == blockIndexes[blockCount - 1] + 1) {
        ++blockCount;
    }

    return ReadBlockRange(firstBlockIndex, blockCount, options);
}

TFuture<std::vector<TBlock>> TJournalChunk::OnBlockRangeReadFromDisk(
    const TChunkReadOptions& options,
    std::vector<std::unique_ptr<ICachedBlockCookie>> blockCookies,
    int firstBlockIndex,
    int blockCount,
    std::vector<TBlock> alreadyReadBlocks,
    TErrorOr<std::vector<TBlock>>&& blocksOrError)
{
    YT_VERIFY(
        blockCookies.empty() ||
        std::ssize(alreadyReadBlocks) + blockCount == std::ssize(blockCookies));

    if (!blocksOrError.IsOK()) {
        auto error = TError("Error occured while reading block range %v:%v of journal chunk %v",
            firstBlockIndex,
            firstBlockIndex + blockCount,
            Id_)
            << blocksOrError;
        YT_LOG_DEBUG(error);

        if (!blockCookies.empty()) {
            // Just try to propagate error to each cookie, even if some had already been set.
            for (auto& blockCookie : blockCookies) {
                blockCookie->SetBlock(error);
            }
        }

        THROW_ERROR(error);
    }

    auto blocks = std::move(blocksOrError.Value());
    int blocksLeftToRead = blockCount - std::ssize(blocks);
    YT_VERIFY(!blocks.empty());
    YT_VERIFY(blocksLeftToRead >= 0);

    YT_LOG_DEBUG("Successfully read block range of journal chunk "
        "(ChunkId: %v, FirstBlockIndex: %v, BlockCount: %v, NewlyReadBlockCount: %v, "
        "AlreadyReadBlockCount: %v, CookieCount: %v)",
        Id_,
        firstBlockIndex,
        blockCount,
        std::ssize(blocks),
        std::ssize(alreadyReadBlocks),
        std::ssize(blockCookies));

    if (!blockCookies.empty()) {
        for (int localIndex = 0; localIndex < std::ssize(blocks); ++localIndex) {
            int index = std::ssize(alreadyReadBlocks) + localIndex;
            blockCookies[index]->SetBlock(TCachedBlock(blocks[localIndex]));
        }
    }

    for (auto& block : blocks) {
        YT_VERIFY(block);
        alreadyReadBlocks.push_back(std::move(block));
    }

    if (blocksLeftToRead == 0) {
        return MakeFuture<std::vector<TBlock>>(std::move(alreadyReadBlocks));
    }

    firstBlockIndex += std::ssize(blocks);

    return ReadBlockRange(firstBlockIndex, blocksLeftToRead, options)
        .AsUnique()
        .Apply(BIND(&TJournalChunk::OnBlockRangeReadFromDisk,
            MakeStrong(this),
            options,
            Passed(std::move(blockCookies)),
            firstBlockIndex,
            blocksLeftToRead,
            Passed(std::move(alreadyReadBlocks)))
        .AsyncVia(GetCurrentInvoker()));
}

void TJournalChunk::OnBlockReadFromDiskForPrecache(
    TJournalChunk::TPrecachedBlockInfo precachedBlockInfo,
    TErrorOr<std::vector<TBlock>>&& blocksOrError)
{
    YT_VERIFY(precachedBlockInfo.Cookie->IsActive());

    if (!blocksOrError.IsOK()) {
        auto error = TError("Error occured while reading block %v of chunk %v for precache",
            precachedBlockInfo.BlockIndex,
            Id_)
            << blocksOrError;
        YT_LOG_DEBUG(error);

        precachedBlockInfo.Cookie->SetBlock(std::move(error));
        return;
    }

    auto blocks = std::move(blocksOrError.Value());
    YT_VERIFY(blocks.size() <= 1);

    YT_LOG_DEBUG("Successfully read block of journal chunk for precache "
        "(ChunkId: %v, BlockIndex: %v, IsBlockPresent: %v)",
        Id_,
        precachedBlockInfo.BlockIndex,
        !blocks.empty());

    if (blocks.empty()) {
        // NB: Block index is out of bounds, just cache sentinel to avoid trying to read it again.
        precachedBlockInfo.Cookie->SetBlock({});
    } else {
        YT_VERIFY(blocks[0]);
        precachedBlockInfo.Cookie->SetBlock(std::move(blocks[0]));
    }
}

TFuture<std::vector<TBlock>> TJournalChunk::ReadCompleteBlockSetAndCache(
    const std::vector<int>& blockIndexes,
    const TChunkReadOptions& options)
{
    if (!options.FetchFromDisk) {
        THROW_ERROR_EXCEPTION("FetchFromDisk flag must be set when reading complete block set");
    }

    std::vector<std::unique_ptr<ICachedBlockCookie>> blockCookies;
    std::vector<TPrecachedBlockInfo> precachedBlockInfos;
    int alreadyPrecachedBlockCount = 0;

    if (options.FetchFromCache &&
        options.BlockCache &&
        options.BlockCache->IsBlockTypeActive(options.BlockType))
    {
        blockCookies.reserve(blockIndexes.size());

        for (int indexInRequest = 0; indexInRequest < std::ssize(blockIndexes); ++indexInRequest) {
            auto blockIndex = blockIndexes[indexInRequest];

            TBlockId blockId(Id_, blockIndex);
            blockCookies.push_back(options.BlockCache->GetBlockCookie(blockId, options.BlockType));

            YT_VERIFY(indexInRequest == 0 || blockIndexes[indexInRequest] > blockIndexes[indexInRequest - 1]);

            if (options.BlockCountToPrecache > 0) {
                for (int offset = 1; offset <= options.BlockCountToPrecache; ++offset) {
                    if (indexInRequest + 1 == std::ssize(blockIndexes) ||
                        blockIndex + offset < blockIndexes[indexInRequest + 1])
                    {
                        // NB: We do not know total block count of the chunk here because
                        // we need to lock chunk which is done later on. So in case chunk
                        // does not contain block with such index a sentinel empty block will be cached.
                        TBlockId blockId(Id_, blockIndex + offset);
                        auto cookie = options.BlockCache->GetBlockCookie(blockId, options.BlockType);
                        if (cookie->IsActive()) {
                            precachedBlockInfos.emplace_back(
                                blockIndex + offset,
                                std::move(cookie));
                        } else {
                            ++alreadyPrecachedBlockCount;
                        }
                    }
                }
            }
        }
    }

    int indexInRequest = 0;
    int cachedBlockCount = 0;
    std::vector<TFuture<std::vector<TBlock>>> futures;

    while (indexInRequest < std::ssize(blockIndexes)) {
        if (!blockCookies.empty() && !blockCookies[indexInRequest]->IsActive()) {
            auto cookieFuture = blockCookies[indexInRequest]->GetBlockFuture();
            auto cachedBlockFuture = cookieFuture.Apply(BIND([
                statistics = options.ChunkReaderStatistics,
                cookie = std::move(blockCookies[indexInRequest])
            ] {
                auto block = cookie->GetBlock();
                YT_VERIFY(block);
                statistics->DataBytesReadFromCache.fetch_add(
                    block.Size(),
                    std::memory_order::relaxed);

                return std::vector<TBlock>{std::move(block)};
            }));

            futures.push_back(std::move(cachedBlockFuture));
            ++indexInRequest;
            ++cachedBlockCount;

            continue;
        }

        std::vector<std::unique_ptr<ICachedBlockCookie>> localBlockCookies;
        if (!blockCookies.empty()) {
            localBlockCookies.push_back(std::move(blockCookies[indexInRequest]));
        }

        int firstIndexInRequest = indexInRequest;
        while (indexInRequest + 1 < std::ssize(blockIndexes) &&
            blockIndexes[indexInRequest + 1] == blockIndexes[indexInRequest] + 1 &&
            (blockCookies.empty() || blockCookies[indexInRequest + 1]->IsActive()))
        {
            ++indexInRequest;
            if (!blockCookies.empty()) {
                localBlockCookies.push_back(std::move(blockCookies[indexInRequest]));
            }
        }

        int blockCount = indexInRequest - firstIndexInRequest + 1;
        auto uncachedBlocksFuture = ReadBlockRange(blockIndexes[firstIndexInRequest], blockCount, options)
            .AsUnique()
            .Apply(BIND(&TJournalChunk::OnBlockRangeReadFromDisk,
                MakeStrong(this),
                options,
                Passed(std::move(localBlockCookies)),
                blockIndexes[firstIndexInRequest],
                blockCount,
                /*alreadyReadBlocks*/ std::vector<TBlock>{})
            .AsyncVia(Context_->StorageHeavyInvoker));

        if (!blockCookies.empty()) {
            uncachedBlocksFuture = uncachedBlocksFuture
                .ToImmediatelyCancelable(/*propagateCancelation*/ false);
        }

        futures.push_back(std::move(uncachedBlocksFuture));
        ++indexInRequest;
    }

    YT_VERIFY(indexInRequest == std::ssize(blockIndexes));

    // NB: No need to coalesce block requests here.
    for (auto& precachedBlockInfo : precachedBlockInfos) {
        ReadBlockRange(precachedBlockInfo.BlockIndex, 1, options).AsUnique().Subscribe(BIND(
            &TJournalChunk::OnBlockReadFromDiskForPrecache,
            MakeStrong(this),
            Passed(std::move(precachedBlockInfo)))
            .Via(Context_->StorageHeavyInvoker));
    }

    YT_LOG_DEBUG("Started reading block set of journal chunk "
        "(ChunkId: %v, IsBlockCacheUsed: %v, CachedBlockCount: %v, RequestedBlockCount: %v, BlockRunCount: %v, "
        "BlockCountToPrecache: %v, AlreadyPrecachedBlockCount: %v)",
        Id_,
        !blockCookies.empty(),
        cachedBlockCount,
        blockIndexes.size(),
        futures.size(),
        precachedBlockInfos.size(),
        alreadyPrecachedBlockCount);

    return AllSucceeded(std::move(futures))
        .AsUnique()
        .Apply(BIND([=] (std::vector<std::vector<TBlock>>&& blockRanges) {
            std::vector<TBlock> result;
            result.reserve(indexInRequest);

            for (auto& blockRange : blockRanges) {
                std::move(blockRange.begin(), blockRange.end(), std::back_inserter(result));
            }

            YT_VERIFY(std::ssize(result) == indexInRequest);

            return result;
        }));
}

TFuture<std::vector<TBlock>> TJournalChunk::ReadBlockRange(
    int firstBlockIndex,
    int blockCount,
    const TChunkReadOptions& options)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (firstBlockIndex < 0) {
        return MakeFuture<std::vector<TBlock>>(TError("First block index %v is negative",
            firstBlockIndex));
    }
    if (blockCount < 0) {
        return MakeFuture<std::vector<TBlock>>(TError("Block count %v is negative",
            blockCount));
    }

    if (!options.FetchFromDisk) {
        return MakeFuture(std::vector<TBlock>());
    }

    auto session = New<TReadBlockRangeSession>();
    try {
        StartReadSession(session, options);
        session->FirstBlockIndex = firstBlockIndex;
        session->BlockCount = blockCount;
        session->Promise = NewPromise<std::vector<TBlock>>();
    } catch (const std::exception& ex) {
        return MakeFuture<std::vector<TBlock>>(ex);
    }

    auto callback = BIND(
        &TJournalChunk::DoReadBlockRange,
        MakeStrong(this),
        session);

    Context_->StorageHeavyInvoker->Invoke(std::move(callback), options.WorkloadDescriptor.GetPriority());

    return session->Promise.ToFuture();
}

void TJournalChunk::DoReadBlockRange(const TReadBlockRangeSessionPtr& session)
{
    try {
        auto changelog = GetChangelog();

        int firstBlockIndex = session->FirstBlockIndex;
        int lastBlockIndex = session->FirstBlockIndex + session->BlockCount - 1; // inclusive
        int blockCount = session->BlockCount;

        YT_LOG_DEBUG("Started reading journal chunk blocks ("
            "ChunkId: %v, Blocks: %v, LocationId: %v, LocationUuid: %v, LocationIndex: %v)",
            Id_,
            FormatBlocks(firstBlockIndex, lastBlockIndex),
            Location_->GetId(),
            Location_->GetUuid(),
            Location_->GetIndex());

        TWallTimer timer;

        auto blocksFuture = changelog->Read(
            firstBlockIndex,
            std::min(blockCount, Context_->DataNodeConfig->MaxBlocksPerRead),
            Context_->DataNodeConfig->MaxBytesPerRead);
        auto blocksOrError = WaitFor(blocksFuture);
        if (!blocksOrError.IsOK()) {
            auto error = TError(
                NChunkClient::EErrorCode::IOError,
                "Error reading journal chunk %v",
                Id_)
                << blocksOrError;
            if (!blocksOrError.FindMatching(NHydra::EErrorCode::InvalidChangelogState)) {
                Location_->ScheduleDisable(error);
            }
            THROW_ERROR error;
        }

        auto readTime = timer.GetElapsedTime();
        const auto& blocks = blocksOrError.Value();
        int blocksRead = std::ssize(blocks);
        i64 bytesRead = GetByteSize(blocks);
        session->Options.ChunkReaderStatistics->DataBytesReadFromDisk.fetch_add(bytesRead, std::memory_order::relaxed);
        // TODO(ngc224): propagate proper value in YT-23540
        session->Options.ChunkReaderStatistics->DataIORequests.fetch_add(1, std::memory_order::relaxed);

        YT_LOG_DEBUG("Finished reading journal chunk blocks ("
            "ChunkId: %v, Blocks: %v, LocationId: %v, LocationUuid: %v, LocationIndex: %v, BlocksReadActually: %v, "
            "BytesReadActually: %v, Time: %v)",
            Id_,
            FormatBlocks(firstBlockIndex, lastBlockIndex),
            Location_->GetId(),
            Location_->GetUuid(),
            Location_->GetIndex(),
            blocksRead,
            bytesRead,
            readTime);

        auto& performanceCounters = Location_->GetPerformanceCounters();
        performanceCounters.JournalBlockReadSize.Record(bytesRead);
        performanceCounters.JournalBlockReadTime.Record(readTime);
        performanceCounters.JournalBlockReadBytes.Increment(bytesRead);

        ProfileReadBlockSetLatency(session);

        session->Promise.Set(TBlock::Wrap(blocks));
    } catch (const std::exception& ex) {
        session->Promise.Set(TError(ex));
    }
}

TFuture<void> TJournalChunk::PrepareToReadChunkFragments(
    const TClientChunkReadOptions& /*options*/,
    bool /*useDirectIO*/)
{
    auto guard = ReaderGuard(LifetimeLock_);

    YT_VERIFY(ReadLockCounter_.load() > 0);

    if (Changelog_) {
        return {};
    }

    if (auto changelog = WeakChangelog_.Lock()) {
        Changelog_ = std::move(changelog);
        return {};
    }

    if (OpenChangelogPromise_) {
        return OpenChangelogPromise_.ToFuture();
    }

    auto promise = OpenChangelogPromise_ = NewPromise<void>();

    guard.Release();

    promise.SetFrom(
        Context_->JournalDispatcher->OpenJournal(StoreLocation_, Id_)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const IFileChangelogPtr& changelog) {
                auto writerGuard = WriterGuard(LifetimeLock_);

                OpenChangelogPromise_.Reset();

                if (ReadLockCounter_.load() == 0) {
                    return;
                }

                Changelog_ = changelog;
                WeakChangelog_ = changelog;

                writerGuard.Release();

                YT_LOG_DEBUG("Changelog prepared to read fragments (ChunkId: %v, LocationId: %v, LocationUuid: %v, LocationIndex: %v)",
                    Id_,
                    Location_->GetId(),
                    Location_->GetUuid(),
                    Location_->GetIndex());
            }).AsyncVia(Context_->StorageLightInvoker)));

    return promise.ToFuture();
}

TReadRequest TJournalChunk::MakeChunkFragmentReadRequest(
    const TChunkFragmentDescriptor& fragmentDescriptor,
    bool /*useDirectIO*/)
{
    YT_VERIFY(ReadLockCounter_.load() > 0);
    YT_VERIFY(Changelog_);

    return Changelog_->MakeChunkFragmentReadRequest(fragmentDescriptor);
}

void TJournalChunk::SyncRemove(bool force)
{
    Location_->RemoveChunkFiles(Id_, force);
}

TFuture<void> TJournalChunk::AsyncRemove()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Context_->JournalDispatcher->RemoveJournal(this, true);
}

i64 TJournalChunk::GetFlushedRowCount() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return FlushedRowCount_.load();
}

void TJournalChunk::UpdateFlushedRowCount(i64 rowCount)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    UpdateMax(FlushedRowCount_, rowCount);
}

i64 TJournalChunk::GetDataSize() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return DataSize_.load();
}

void TJournalChunk::UpdateDataSize(i64 dataSize)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    UpdateMax(DataSize_, dataSize);
}

bool TJournalChunk::IsSealed() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Sealed_.load();
}

TFuture<void> TJournalChunk::Seal()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Context_->JournalDispatcher->SealJournal(this).Apply(
        BIND([this, this_ = MakeStrong(this)] {
            YT_LOG_DEBUG("Chunk is marked as sealed (ChunkId: %v)",
                Id_);
            Sealed_.store(true);
        }));
}

IFileChangelogPtr TJournalChunk::GetChangelog()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    YT_VERIFY(ReadLockCounter_.load() > 0);

    {
        auto guard = ReaderGuard(LifetimeLock_);

        if (auto changelog = Changelog_) {
            return changelog;
        }

        if (auto changelog = WeakChangelog_.Lock()) {
            return changelog;
        }
    }

    auto changelog = WaitFor(Context_->JournalDispatcher->OpenJournal(StoreLocation_, Id_))
        .ValueOrThrow();

    {
        auto guard = WriterGuard(LifetimeLock_);

        Changelog_ = changelog;
        WeakChangelog_ = changelog;
    }

    return changelog;
}

void TJournalChunk::ReleaseReader(TWriterGuard<TReaderWriterSpinLock>& writerGuard)
{
    YT_ASSERT_WRITER_SPINLOCK_AFFINITY(LifetimeLock_);
    YT_VERIFY(ReadLockCounter_.load() == 0);

    if (!Changelog_) {
        return;
    }

    auto changelog = std::exchange(Changelog_, nullptr);

    writerGuard.Release();

    YT_LOG_DEBUG("Changelog released (ChunkId: %v, LocationId: %v, LocationUuid: %v, LocationIndex: %v)",
        Id_,
        Location_->GetId(),
        Location_->GetUuid(),
        Location_->GetIndex());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
