#include "journal_chunk.h"

#include "private.h"
#include "journal_dispatcher.h"
#include "location.h"
#include "session.h"

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/hydra/changelog.h>
#include <yt/yt/server/lib/hydra/file_changelog.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/ref_counted_proto.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NDataNode {

using namespace NConcurrency;
using namespace NThreading;
using namespace NClusterNode;
using namespace NIO;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NJournalClient;
using namespace NHydra;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

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
    VERIFY_THREAD_AFFINITY_ANY();

    return StoreLocation_;
}

void TJournalChunk::SetActive(bool value)
{
    VERIFY_THREAD_AFFINITY_ANY();

    Active_.store(value);
}

bool TJournalChunk::IsActive() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Active_.load();
}

TChunkInfo TJournalChunk::GetInfo() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TChunkInfo info;
    info.set_sealed(IsSealed());
    info.set_disk_space(GetDataSize());
    return info;
}

TFuture<TRefCountedChunkMetaPtr> TJournalChunk::ReadMeta(
    const TChunkReadOptions& options,
    const std::optional<std::vector<int>>& extensionTags)
{
    VERIFY_THREAD_AFFINITY_ANY();

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
    meta->set_type(ToProto<int>(EChunkType::Journal));
    meta->set_format(ToProto<int>(EChunkFormat::JournalDefault));
    SetProtoExtension(meta->mutable_extensions(), miscExt);

    ProfileReadMetaLatency(session);

    return MakeFuture(FilterMeta(meta, extensionTags));
}

TFuture<std::vector<TBlock>> TJournalChunk::ReadBlockSet(
    const std::vector<int>& blockIndexes,
    const TChunkReadOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    // Extract the initial contiguous segment of blocks.
    if (blockIndexes.empty()) {
        return MakeFuture(std::vector<TBlock>());
    }

    int firstBlockIndex = blockIndexes.front();
    int blockCount = 1;
    while (blockCount < std::ssize(blockIndexes) && blockIndexes[blockCount] == blockIndexes[blockCount - 1] + 1) {
        ++blockCount;
    }

    return ReadBlockRange(firstBlockIndex, blockCount, options);
}

TFuture<std::vector<TBlock>> TJournalChunk::ReadBlockRange(
    int firstBlockIndex,
    int blockCount,
    const TChunkReadOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

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

    return session->Promise;
}

void TJournalChunk::DoReadBlockRange(const TReadBlockRangeSessionPtr& session)
{
    try {
        auto changelog = GetChangelog();

        int firstBlockIndex = session->FirstBlockIndex;
        int lastBlockIndex = session->FirstBlockIndex + session->BlockCount - 1; // inclusive
        int blockCount = session->BlockCount;

        YT_LOG_DEBUG("Started reading journal chunk blocks (BlockIds: %v:%v-%v, LocationId: %v)",
            Id_,
            firstBlockIndex,
            lastBlockIndex,
            Location_->GetId());

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
            if (blocksOrError.FindMatching(NHydra::EErrorCode::InvalidChangelogState)) {
                THROW_ERROR error;
            }
            Location_->ScheduleDisable(error);
        }

        auto readTime = timer.GetElapsedTime();
        const auto& blocks = blocksOrError.Value();
        int blocksRead = static_cast<int>(blocks.size());
        i64 bytesRead = GetByteSize(blocks);
        session->Options.ChunkReaderStatistics->DataBytesReadFromDisk.fetch_add(bytesRead, std::memory_order::relaxed);

        YT_LOG_DEBUG("Finished reading journal chunk blocks (BlockIds: %v:%v-%v, LocationId: %v, BlocksReadActually: %v, "
            "BytesReadActually: %v, Time: %v)",
            Id_,
            firstBlockIndex,
            lastBlockIndex,
            Location_->GetId(),
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

                YT_LOG_DEBUG("Changelog prepared to read fragments (ChunkId: %v, LocationId: %v)",
                    Id_,
                    Location_->GetId());
            }).AsyncVia(Context_->StorageLightInvoker)));

    return promise.ToFuture();
}

IIOEngine::TReadRequest TJournalChunk::MakeChunkFragmentReadRequest(
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
    VERIFY_THREAD_AFFINITY_ANY();

    return Context_->JournalDispatcher->RemoveJournal(this, true);
}

i64 TJournalChunk::GetFlushedRowCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return FlushedRowCount_.load();
}

void TJournalChunk::UpdateFlushedRowCount(i64 rowCount)
{
    VERIFY_THREAD_AFFINITY_ANY();

    UpdateMax(FlushedRowCount_, rowCount);
}

i64 TJournalChunk::GetDataSize() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return DataSize_.load();
}

void TJournalChunk::UpdateDataSize(i64 dataSize)
{
    VERIFY_THREAD_AFFINITY_ANY();

    UpdateMax(DataSize_, dataSize);
}

bool TJournalChunk::IsSealed() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Sealed_.load();
}

TFuture<void> TJournalChunk::Seal()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Context_->JournalDispatcher->SealJournal(this).Apply(
        BIND([this, this_ = MakeStrong(this)] {
            YT_LOG_DEBUG("Chunk is marked as sealed (ChunkId: %v)",
                Id_);
            Sealed_.store(true);
        }));
}

IFileChangelogPtr TJournalChunk::GetChangelog()
{
    VERIFY_THREAD_AFFINITY_ANY();
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
    VERIFY_WRITER_SPINLOCK_AFFINITY(LifetimeLock_);
    YT_VERIFY(ReadLockCounter_.load() == 0);

    if (!Changelog_) {
        return;
    }

    auto changelog = std::exchange(Changelog_, nullptr);

    writerGuard.Release();

    YT_LOG_DEBUG("Changelog released (ChunkId: %v, LocationId: %v)",
        Id_,
        Location_->GetId());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
