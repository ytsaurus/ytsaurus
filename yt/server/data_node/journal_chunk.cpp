#include "journal_chunk.h"
#include "private.h"
#include "journal_dispatcher.h"
#include "location.h"
#include "session.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/hydra/changelog.h>
#include <yt/server/hydra/sync_file_changelog.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/ref_counted_proto.h>

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/fs.h>

#include <yt/core/profiling/timing.h>

namespace NYT {
namespace NDataNode {

using namespace NConcurrency;
using namespace NCellNode;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NHydra;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;
static TSimpleCounter DiskJournalReadByteCounter("/disk_journal_read_bytes");

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
    const TWorkloadDescriptor& /*workloadDescriptor*/,
    const TNullable<std::vector<int>>& extensionTags)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return BIND(&TJournalChunk::DoReadMeta, MakeStrong(this), extensionTags)
        .AsyncVia(Bootstrap_->GetControlInvoker())
        .Run();
}

TRefCountedChunkMetaPtr TJournalChunk::DoReadMeta(const TNullable<std::vector<int>>& extensionTags)
{
    UpdateCachedParams();

    TMiscExt miscExt;
    miscExt.set_row_count(CachedRowCount_);
    miscExt.set_uncompressed_data_size(CachedDataSize_);
    miscExt.set_compressed_data_size(CachedDataSize_);
    miscExt.set_sealed(Sealed_);
    SetProtoExtension(Meta_->mutable_extensions(), miscExt);

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
    YCHECK(firstBlockIndex >= 0);
    YCHECK(blockCount >= 0);

    if (!options.FetchFromDisk) {
        return MakeFuture(std::vector<TBlock>());
    }

    auto promise = NewPromise<std::vector<TBlock>>();
    TJournalChunk::DoReadBlockRange(firstBlockIndex, blockCount, promise);
    return promise;
}

void TJournalChunk::DoReadBlockRange(
    int firstBlockIndex,
    int blockCount,
    TPromise<std::vector<TBlock>> promise)
{
    auto config = Bootstrap_->GetConfig()->DataNode;
    const auto& dispatcher = Bootstrap_->GetJournalDispatcher();

    try {
        auto changelog = WaitFor(dispatcher->OpenChangelog(StoreLocation_, Id_))
            .ValueOrThrow();

        LOG_DEBUG("Started reading journal chunk blocks (BlockIds: %v:%v-%v, LocationId: %v)",
            Id_,
            firstBlockIndex,
            firstBlockIndex + blockCount - 1,
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
            Y_UNREACHABLE(); // Disable() exits the process.
        }

        auto readTime = timer.GetElapsedTime();
        const auto& blocks = blocksOrError.Value();
        int blocksRead = static_cast<int>(blocks.size());
        i64 bytesRead = GetByteSize(blocks);

        LOG_DEBUG("Finished reading journal chunk blocks (BlockIds: %v:%v-%v, LocationId: %v, BlocksReadActually: %v, BytesReadActually: %v)",
            Id_,
            firstBlockIndex,
            firstBlockIndex + blockCount - 1,
            Location_->GetId(),
            blocksRead,
            bytesRead);

        auto& locationProfiler = Location_->GetProfiler();
        locationProfiler.Enqueue("/journal_read_size", bytesRead, EMetricType::Gauge);
        locationProfiler.Enqueue("/journal_read_time", readTime.MicroSeconds(), EMetricType::Gauge);
        locationProfiler.Enqueue("/journal_read_throughput", bytesRead * 1000000 / (1 + readTime.MicroSeconds()), EMetricType::Gauge);
        DataNodeProfiler.Increment(DiskJournalReadByteCounter, bytesRead);

        promise.Set(TBlock::Wrap(blocks));
    } catch (const std::exception& ex) {
        promise.Set(TError(ex));
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
            LOG_DEBUG("Started closing journal chunk (ChunkId: %v)", Id_);
            WaitFor(Changelog_->Close())
                .ThrowOnError();
            LOG_DEBUG("Finished closing journal chunk (ChunkId: %v)", Id_);
            Changelog_.Reset();
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            Location_->Disable(error);
            Y_UNREACHABLE(); // Disable() exits the process.
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
    YCHECK(!Removing_);
    YCHECK(!Changelog_);
    Changelog_ = changelog;

    UpdateCachedParams();
}

void TJournalChunk::DetachChangelog()
{
    YCHECK(!Removing_);

    UpdateCachedParams();
    Changelog_.Reset();
}

bool TJournalChunk::HasAttachedChangelog() const
{
    YCHECK(!Removing_);

    return Changelog_.operator bool();
}

IChangelogPtr TJournalChunk::GetAttachedChangelog() const
{
    YCHECK(!IsRemoveScheduled());

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

} // namespace NDataNode
} // namespace NYT
