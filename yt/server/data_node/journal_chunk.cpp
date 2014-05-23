#include "stdafx.h"
#include "journal_chunk.h"
#include "journal_dispatcher.h"
#include "location.h"
#include "private.h"

#include <core/misc/fs.h>

#include <core/profiling/scoped_timer.h>

#include <server/hydra/changelog.h>
#include <server/hydra/private.h>

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>

namespace NYT {
namespace NDataNode {

using namespace NHydra;
using namespace NCellNode;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = DataNodeLogger;
static auto& Profiler = DataNodeProfiler;

static NProfiling::TRateCounter DiskJournalReadThroughputCounter("/disk_journal_read_throughput");

////////////////////////////////////////////////////////////////////////////////

TJournalChunk::TJournalChunk(
    TLocationPtr location,
    const TChunkId& chunkId,
    const TChunkMeta& meta,
    const TChunkInfo& info,
    TNodeMemoryTracker* memoryUsageTracker)
    : TChunk(
        location,
        chunkId,
        info,
        memoryUsageTracker)
{
    Meta_ = New<TRefCountedChunkMeta>(meta);
}

IChunk::TAsyncGetMetaResult TJournalChunk::GetMeta(
    i64 priority,
    const std::vector<int>* tags /*= nullptr*/)
{
    return MakeFuture<TGetMetaResult>(FilterCachedMeta(tags));
}

TAsyncError TJournalChunk::ReadBlocks(
    int firstBlockIndex,
    int blockCount,
    i64 priority,
    std::vector<TSharedRef>* blocks)
{
    YCHECK(firstBlockIndex >= 0);
    YCHECK(blockCount >= 0);

    auto promise = NewPromise<TError>();

    auto callback = BIND(
        &TJournalChunk::DoReadBlocks,
        MakeStrong(this),
        firstBlockIndex,
        blockCount,
        promise,
        blocks);

    Location_
        ->GetDataReadInvoker()
        ->Invoke(callback, priority);

    return promise;
}

void TJournalChunk::DoReadBlocks(
    int firstBlockIndex,
    int blockCount,
    TPromise<TError> promise,
    std::vector<TSharedRef>* blocks)
{
    auto* bootstrap = Location_->GetBootstrap();
    auto config = bootstrap->GetConfig();

    auto dispatcher = bootstrap->GetJournalDispatcher();

    try {
        auto changelog = dispatcher->GetChangelog(this);
    
        LOG_DEBUG("Started reading journal chunk blocks (BlockIds: %s:%d-%d, LocationId: %s)",
            ~ToString(Id_),
            firstBlockIndex,
            firstBlockIndex + blockCount - 1,
            ~Location_->GetId());

        NProfiling::TScopedTimer timer;

        auto readBlocks = changelog->Read(
            firstBlockIndex,
            blockCount,
            config->DataNode->MaxBytesPerJournalRead);

        auto readTime = timer.GetElapsed();

        i64 readSize = 0;
        for (int index = 0; index < readBlocks.size(); ++index) {
            auto& readBlock = readBlocks[index];
            readSize += readBlock.Size();
            auto& block = (*blocks)[index + firstBlockIndex];
            if (!block) {
                block = std::move(readBlock);
            }
        }

        LOG_DEBUG("Finished reading journal chunk blocks (BlockIds: %s:%d-%d, LocationId: %s, ActuallyReadBlocks: %d, ActuallyReadBytes: %" PRId64 ")",
            ~ToString(Id_),
            firstBlockIndex,
            firstBlockIndex + blockCount - 1,
            ~Location_->GetId(),
            static_cast<int>(readBlocks.size()),
            readSize);

        auto& locationProfiler = Location_->Profiler();
        locationProfiler.Enqueue("/journal_read_size", readSize);
        locationProfiler.Enqueue("/journal_read_time", readTime.MicroSeconds());
        locationProfiler.Enqueue("/journal_read_throughput", readSize * 1000000 / (1 + readTime.MicroSeconds()));
        DataNodeProfiler.Increment(DiskJournalReadThroughputCounter, readSize);

        promise.Set(TError());
    } catch (const std::exception& ex) {
        auto error = TError(
            NChunkClient::EErrorCode::IOError,
            "Error reading journal chunk %s",
            ~ToString(Id_))
            << ex;
        Location_->Disable();
        promise.Set(ex);
    }
}

TRefCountedChunkMetaPtr TJournalChunk::GetCachedMeta() const
{
    return Meta_;
}

void TJournalChunk::EvictFromCache()
{
    auto* bootstrap = Location_->GetBootstrap();
    auto dispatcher = bootstrap->GetJournalDispatcher();
    dispatcher->EvictChangelog(this);
}

TFuture<void> TJournalChunk::RemoveFiles()
{
    auto dataFileName = GetFileName();
    auto indexFileName = dataFileName + IndexSuffix;
    auto id = Id_;
    auto location = Location_;

    return BIND([=] () {
        LOG_DEBUG("Started removing journal chunk files (ChunkId: %s)",
            ~ToString(id));

        try {
            NFS::Remove(dataFileName);
            NFS::Remove(indexFileName);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error removing journal chunk files");
            location->Disable();
        }

        LOG_DEBUG("Finished removing journal chunk files (ChunkId: %s)",
            ~ToString(id));
    }).AsyncVia(location->GetWriteInvoker()).Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
