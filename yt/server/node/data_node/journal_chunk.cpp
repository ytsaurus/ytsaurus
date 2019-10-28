#include "journal_chunk.h"
#include "private.h"
#include "journal_dispatcher.h"
#include "location.h"
#include "session.h"

#include <yt/server/node/cell_node/bootstrap.h>
#include <yt/server/node/cell_node/config.h>

#include <yt/server/lib/hydra/changelog.h>
#include <yt/server/lib/hydra/sync_file_changelog.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/ref_counted_proto.h>

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/fs.h>

#include <yt/core/profiling/timing.h>

namespace NYT::NDataNode {

using namespace NConcurrency;
using namespace NCellNode;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NHydra;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;
static TMonotonicCounter DiskJournalReadByteCounter("/disk_journal_read_bytes");

////////////////////////////////////////////////////////////////////////////////

TJournalChunk::TJournalChunk(
    TBootstrap* bootstrap,
    TStoreLocationPtr location,
    const TChunkDescriptor& descriptor)
    : TChunkBase(
        bootstrap,
        location,
        descriptor.Id)
    , StoreLocation_(location)
    , Meta_(New<TRefCountedChunkMeta>())
{
    CachedRowCount_ = descriptor.RowCount;
    CachedDataSize_ = descriptor.DiskSpace;
    Sealed_ = descriptor.Sealed;

    Meta_->set_type(static_cast<int>(EChunkType::Journal));
    Meta_->set_version(0);
}

TStoreLocationPtr TJournalChunk::GetStoreLocation() const
{
    return StoreLocation_;
}

void TJournalChunk::SetActive(bool value)
{
    Active_ = value;
}

bool TJournalChunk::IsActive() const
{
    return Active_;
}

TChunkInfo TJournalChunk::GetInfo() const
{
    UpdateCachedParams();

    TChunkInfo info;
    info.set_sealed(Sealed_);
    info.set_disk_space(CachedDataSize_);
    return info;
}

TFuture<TRefCountedChunkMetaPtr> TJournalChunk::ReadMeta(
    const TBlockReadOptions& options,
    const std::optional<std::vector<int>>& extensionTags)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto session = New<TReadMetaSession>();
    try {
        StartReadSession(session, options);
    } catch (const std::exception& ex) {
        return MakeFuture<TRefCountedChunkMetaPtr>(ex);
    }

    return BIND(&TJournalChunk::DoReadMeta, MakeStrong(this), session, extensionTags)
        .AsyncVia(Bootstrap_->GetControlInvoker())
        .Run();
}

TRefCountedChunkMetaPtr TJournalChunk::DoReadMeta(
    const TReadMetaSessionPtr& session,
    const std::optional<std::vector<int>>& extensionTags)
{
    UpdateCachedParams();

    TMiscExt miscExt;
    miscExt.set_row_count(CachedRowCount_);
    miscExt.set_uncompressed_data_size(CachedDataSize_);
    miscExt.set_compressed_data_size(CachedDataSize_);
    miscExt.set_sealed(Sealed_);
    SetProtoExtension(Meta_->mutable_extensions(), miscExt);

    ProfileReadMetaLatency(session);

    return FilterMeta(Meta_, extensionTags);
}

TFuture<std::vector<TBlock>> TJournalChunk::ReadBlockSet(
    const std::vector<int>& blockIndexes,
    const TBlockReadOptions& options)
{
    // Extract the initial contiguous segment of blocks.
    if (blockIndexes.empty()) {
        return MakeFuture(std::vector<TBlock>());
    }

    int firstBlockIndex = blockIndexes.front();
    int blockCount = 0;
    while (blockCount < blockIndexes.size() && blockIndexes[blockCount + 1] == blockIndexes[blockCount] + 1) {
        ++blockCount;
    }

    return ReadBlockRange(firstBlockIndex, blockCount, options);
}

TFuture<std::vector<TBlock>> TJournalChunk::ReadBlockRange(
    int firstBlockIndex,
    int blockCount,
    const TBlockReadOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YT_VERIFY(firstBlockIndex >= 0);
    YT_VERIFY(blockCount >= 0);

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

    Bootstrap_
        ->GetStorageHeavyInvoker()
        ->Invoke(std::move(callback), options.WorkloadDescriptor.GetPriority());

    return session->Promise;
}

void TJournalChunk::DoReadBlockRange(const TReadBlockRangeSessionPtr& session)
{
    auto config = Bootstrap_->GetConfig()->DataNode;
    const auto& dispatcher = Bootstrap_->GetJournalDispatcher();

    try {
        auto changelog = WaitFor(dispatcher->OpenChangelog(StoreLocation_, Id_))
            .ValueOrThrow();

        int firstBlockIndex = session->FirstBlockIndex;
        int lastBlockIndex = session->FirstBlockIndex + session->BlockCount - 1; // inclusive
        int blockCount = session->BlockCount;

        YT_LOG_DEBUG("Started reading journal chunk blocks (BlockIds: %v:%v-%v, LocationId: %v)",
            Id_,
            firstBlockIndex,
            lastBlockIndex,
            Location_->GetId());

        TWallTimer timer;

        auto asyncBlocks = changelog->Read(
            firstBlockIndex,
            std::min(blockCount, config->MaxBlocksPerRead),
            config->MaxBytesPerRead);
        auto blocksOrError = WaitFor(asyncBlocks);
        if (!blocksOrError.IsOK()) {
            auto error = TError(
                NChunkClient::EErrorCode::IOError,
                "Error reading journal chunk %v",
                Id_)
                << blocksOrError;
            Location_->Disable(error);
            YT_ABORT(); // Disable() exits the process.
        }

        auto readTime = timer.GetElapsedTime();
        const auto& blocks = blocksOrError.Value();
        int blocksRead = static_cast<int>(blocks.size());
        i64 bytesRead = GetByteSize(blocks);
        session->Options.ChunkReaderStatistics->DataBytesReadFromDisk += bytesRead;

        YT_LOG_DEBUG("Finished reading journal chunk blocks (BlockIds: %v:%v-%v, LocationId: %v, BlocksReadActually: %v, "
            "BytesReadActually: %v, Time: %v)",
            Id_,
            firstBlockIndex,
            lastBlockIndex,
            Location_->GetId(),
            blocksRead,
            bytesRead,
            readTime);

        const auto& locationProfiler = Location_->GetProfiler();
        auto& performanceCounters = Location_->GetPerformanceCounters();
        locationProfiler.Update(performanceCounters.JournalBlockReadSize, bytesRead);
        locationProfiler.Update(performanceCounters.JournalBlockReadTime, NProfiling::DurationToValue(readTime));
        locationProfiler.Update(performanceCounters.JournalBlockReadThroughput, bytesRead * 1000000 / (1 + readTime.MicroSeconds()));
        locationProfiler.Increment(performanceCounters.JournalBlockReadBytes, bytesRead);
        DataNodeProfiler.Increment(DiskJournalReadByteCounter, bytesRead);

        ProfileReadBlockSetLatency(session);

        session->Promise.Set(TBlock::Wrap(blocks));
    } catch (const std::exception& ex) {
        session->Promise.Set(TError(ex));
    }
}

void TJournalChunk::UpdateCachedParams() const
{
    if (Changelog_) {
        CachedRowCount_ = Changelog_->GetRecordCount();
        CachedDataSize_ = Changelog_->GetDataSize();
    }
}

void TJournalChunk::SyncRemove(bool force)
{
    if (Changelog_) {
        try {
            YT_LOG_DEBUG("Started closing journal chunk (ChunkId: %v)", Id_);
            WaitFor(Changelog_->Close())
                .ThrowOnError();
            YT_LOG_DEBUG("Finished closing journal chunk (ChunkId: %v)", Id_);
            Changelog_.Reset();
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            Location_->Disable(error);
            YT_ABORT(); // Disable() exits the process.
        }
    }

    Location_->RemoveChunkFiles(Id_, force);
}

TFuture<void> TJournalChunk::AsyncRemove()
{
    const auto& dispatcher = Bootstrap_->GetJournalDispatcher();
    return dispatcher->RemoveChangelog(this, true);
}

void TJournalChunk::AttachChangelog(IChangelogPtr changelog)
{
    YT_VERIFY(!Removing_);
    YT_VERIFY(!Changelog_);
    Changelog_ = changelog;

    UpdateCachedParams();
}

void TJournalChunk::DetachChangelog()
{
    YT_VERIFY(!Removing_);

    UpdateCachedParams();
    Changelog_.Reset();
}

bool TJournalChunk::HasAttachedChangelog() const
{
    YT_VERIFY(!Removing_);

    return Changelog_.operator bool();
}

IChangelogPtr TJournalChunk::GetAttachedChangelog() const
{
    YT_VERIFY(!IsRemoveScheduled());

    return Changelog_;
}

i64 TJournalChunk::GetRowCount() const
{
    UpdateCachedParams();
    return CachedRowCount_;
}

i64 TJournalChunk::GetDataSize() const
{
    UpdateCachedParams();
    return CachedDataSize_;
}

bool TJournalChunk::IsSealed() const
{
    return Sealed_;
}

TFuture<void> TJournalChunk::Seal()
{
    const auto& dispatcher = Bootstrap_->GetJournalDispatcher();
    return dispatcher->SealChangelog(this).Apply(BIND([this, this_ = MakeStrong(this)] () {
        Sealed_ = true;
    }));
}

////////////////////////////////////////////////////////////////////////////////

TJournalChunkChangelogGuard::TJournalChunkChangelogGuard(
    TJournalChunkPtr chunk,
    IChangelogPtr changelog)
    : Chunk_(chunk)
{
    Chunk_->AttachChangelog(changelog);
}

TJournalChunkChangelogGuard& TJournalChunkChangelogGuard::operator=(TJournalChunkChangelogGuard&& other)
{
    swap(*this, other);
    return *this;
}

TJournalChunkChangelogGuard::~TJournalChunkChangelogGuard()
{
    if (Chunk_) {
        Chunk_->DetachChangelog();
    }
}

void swap(TJournalChunkChangelogGuard& lhs, TJournalChunkChangelogGuard& rhs)
{
    using std::swap;
    swap(lhs.Chunk_, rhs.Chunk_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
