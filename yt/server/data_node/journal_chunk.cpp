#include "stdafx.h"
#include "journal_chunk.h"
#include "journal_dispatcher.h"
#include "location.h"
#include "session.h"
#include "private.h"

#include <core/misc/fs.h>

#include <core/concurrency/thread_affinity.h>

#include <core/profiling/scoped_timer.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <server/hydra/changelog.h>
#include <server/hydra/sync_file_changelog.h>

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>

namespace NYT {
namespace NDataNode {

using namespace NHydra;
using namespace NCellNode;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;
static auto& Profiler = DataNodeProfiler;

static NProfiling::TRateCounter DiskJournalReadThroughputCounter("/disk_journal_read_throughput");

////////////////////////////////////////////////////////////////////////////////

TJournalChunk::TJournalChunk(
    TBootstrap* bootstrap,
    TLocationPtr location,
    const TChunkDescriptor& descriptor)
    : TChunkBase(
        bootstrap,
        location,
        descriptor.Id)
{
    CachedSealed_ = descriptor.Sealed;
    CachedRowCount_ = descriptor.RowCount;
    CachedDataSize_ = descriptor.DiskSpace;

    Meta_ = New<TRefCountedChunkMeta>();
    Meta_->set_type(EChunkType::Journal);
    Meta_->set_version(0);
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
    info.set_sealed(CachedSealed_);
    info.set_disk_space(CachedDataSize_);
    return info;
}

IChunk::TAsyncGetMetaResult TJournalChunk::GetMeta(
    i64 /*priority*/,
    const std::vector<int>* tags /*= nullptr*/)
{
    UpdateCachedParams();

    TMiscExt miscExt;
    miscExt.set_row_count(CachedRowCount_);
    miscExt.set_uncompressed_data_size(CachedDataSize_);
    miscExt.set_compressed_data_size(CachedDataSize_);
    miscExt.set_sealed(CachedSealed_);
    SetProtoExtension(Meta_->mutable_extensions(), miscExt);

    return MakeFuture<TGetMetaResult>(FilterCachedMeta(tags));
}

IChunk::TAsyncReadBlocksResult TJournalChunk::ReadBlocks(
    int firstBlockIndex,
    int blockCount,
    i64 priority)
{
    YCHECK(firstBlockIndex >= 0);
    YCHECK(blockCount >= 0);

    auto promise = NewPromise<TReadBlocksResult>();

    auto callback = BIND(
        &TJournalChunk::DoReadBlocks,
        MakeStrong(this),
        firstBlockIndex,
        blockCount,
        promise);

    Location_
        ->GetDataReadInvoker()
        ->Invoke(callback, priority);

    return promise;
}

void TJournalChunk::DoReadBlocks(
    int firstBlockIndex,
    int blockCount,
    TPromise<TReadBlocksResult> promise)
{
    auto config = Bootstrap_->GetConfig()->DataNode;
    auto dispatcher = Bootstrap_->GetJournalDispatcher();

    try {
        auto changelog = dispatcher->OpenChangelog(Location_, Id_, false);
    
        LOG_DEBUG("Started reading journal chunk blocks (BlockIds: %s:%d-%d, LocationId: %s)",
            ~ToString(Id_),
            firstBlockIndex,
            firstBlockIndex + blockCount - 1,
            ~Location_->GetId());

        NProfiling::TScopedTimer timer;

        std::vector<TSharedRef> blocks;
        try {
            blocks = changelog->Read(
                firstBlockIndex,
                std::min(blockCount, config->MaxBlocksPerRead),
                config->MaxBytesPerRead);
        } catch (const std::exception& ex) {
            Location_->Disable();
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::IOError,
                "Error reading journal chunk %v",
                Id_)
                << ex;
        }

        auto readTime = timer.GetElapsed();
        int blocksRead = static_cast<int>(blocks.size());
        i64 bytesRead = GetTotalSize(blocks);

        LOG_DEBUG("Finished reading journal chunk blocks (BlockIds: %v:%v-%v, LocationId: %v, BlocksReadActually: %v, BytesReadActually: %v)",
            Id_,
            firstBlockIndex,
            firstBlockIndex + blockCount - 1,
            Location_->GetId(),
            blocksRead,
            bytesRead);

        auto& locationProfiler = Location_->Profiler();
        locationProfiler.Enqueue("/journal_read_size", bytesRead);
        locationProfiler.Enqueue("/journal_read_time", readTime.MicroSeconds());
        locationProfiler.Enqueue("/journal_read_throughput", bytesRead * 1000000 / (1 + readTime.MicroSeconds()));
        DataNodeProfiler.Increment(DiskJournalReadThroughputCounter, bytesRead);

        promise.Set(blocks);
    } catch (const std::exception& ex) {
        promise.Set(ex);
    }
}

void TJournalChunk::UpdateCachedParams() const
{
    if (Changelog_) {
        CachedRowCount_ = Changelog_->GetRecordCount();
        CachedDataSize_ = Changelog_->GetDataSize();
        CachedSealed_ = Changelog_->IsSealed();
    }
}

void TJournalChunk::EvictFromCache()
{
    auto dispatcher = Bootstrap_->GetJournalDispatcher();
    dispatcher->EvictChangelog(this);
}

void TJournalChunk::SyncRemove()
{
    RemoveChangelogFiles(GetFileName());
}

TFuture<void> TJournalChunk::AsyncRemove()
{
    auto location = Location_;
    auto dispatcher = Bootstrap_->GetJournalDispatcher();
    return dispatcher->RemoveChangelog(this)
        .Apply(BIND([=] (TError error) {
            if (!error.IsOK()) {
                location->Disable();
            }
        }));
}

void TJournalChunk::AttachChangelog(IChangelogPtr changelog)
{
    YCHECK(!Changelog_);
    Changelog_ = changelog;

    UpdateCachedParams();
}

void TJournalChunk::DetachChangelog()
{
    UpdateCachedParams();
    Changelog_.Reset();
}

bool TJournalChunk::HasAttachedChangelog() const
{
    return Changelog_ != nullptr;
}

i64 TJournalChunk::GetRowCount() const
{
    UpdateCachedParams();
    return CachedRowCount_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
