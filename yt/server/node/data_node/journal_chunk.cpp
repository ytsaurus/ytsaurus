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
{
    CachedRowCount_.store(descriptor.RowCount);
    CachedDataSize_.store(descriptor.DiskSpace);
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
    info.set_disk_space(GetCachedDataSize());
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

    TMiscExt miscExt;
    miscExt.set_row_count(GetCachedRowCount());
    miscExt.set_uncompressed_data_size(GetCachedDataSize());
    miscExt.set_compressed_data_size(miscExt.uncompressed_data_size());
    miscExt.set_sealed(IsSealed());

    auto meta = New<TRefCountedChunkMeta>();
    meta->set_type(ToProto<int>(EChunkType::Journal));
    meta->set_version(0);
    SetProtoExtension(meta->mutable_extensions(), miscExt);

    ProfileReadMetaLatency(session);

    return MakeFuture(FilterMeta(meta, extensionTags));
}

TFuture<std::vector<TBlock>> TJournalChunk::ReadBlockSet(
    const std::vector<int>& blockIndexes,
    const TBlockReadOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

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
    const auto& config = Bootstrap_->GetConfig()->DataNode;
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
            if (blocksOrError.FindMatching(NHydra::EErrorCode::InvalidChangelogState)) {
                THROW_ERROR error;
            }
            Location_->Disable(error);
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

void TJournalChunk::SyncRemove(bool force)
{
    Location_->RemoveChunkFiles(Id_, force);
}

TFuture<void> TJournalChunk::AsyncRemove()
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& dispatcher = Bootstrap_->GetJournalDispatcher();
    return dispatcher->RemoveChangelog(this, true);
}

void TJournalChunk::UpdateCachedParams(const IChangelogPtr& changelog)
{
    VERIFY_THREAD_AFFINITY_ANY();

    CachedRowCount_.store(changelog->GetRecordCount());
    CachedDataSize_.store(changelog->GetDataSize());
}

i64 TJournalChunk::GetCachedRowCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CachedRowCount_.load();
}

i64 TJournalChunk::GetCachedDataSize() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CachedDataSize_.load();
}

bool TJournalChunk::IsSealed() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Sealed_.load();
}

TFuture<void> TJournalChunk::Seal()
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& dispatcher = Bootstrap_->GetJournalDispatcher();
    return dispatcher->SealChangelog(this).Apply(
        BIND([this, this_ = MakeStrong(this)] {
            YT_LOG_DEBUG("Chunk is marked as sealed (ChunkId: %v)",
                Id_);
            Sealed_.store(true);
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
