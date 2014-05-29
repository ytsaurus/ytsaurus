#include "stdafx.h"
#include "blob_chunk.h"
#include "private.h"
#include "location.h"
#include "blob_reader_cache.h"
#include "chunk_cache.h"
#include "block_store.h"

#include <core/profiling/scoped_timer.h>

#include <core/misc/fs.h>

#include <ytlib/chunk_client/file_reader.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

using namespace NCellNode;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = DataNodeLogger;
static auto& Profiler = DataNodeProfiler;

static NProfiling::TRateCounter DiskBlobReadThroughputCounter("/disk_blob_read_throughput");

////////////////////////////////////////////////////////////////////////////////

TBlobChunk::TBlobChunk(
    TBootstrap* bootstrap,
    TLocationPtr location,
    const TChunkId& id,
    const TChunkInfo& info,
    const TChunkMeta* meta)
    : TChunk(
        bootstrap,
        location,
        id,
        info)
{
    if (meta) {
        InitializeCachedMeta(*meta);
    }
}

IChunk::TAsyncGetMetaResult TBlobChunk::GetMeta(
    i64 priority,
    const std::vector<int>* tags)
{
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (Meta_) {
            guard.Release();
            LOG_DEBUG("Meta cache hit (ChunkId: %s)", ~ToString(Id_));
            return MakeFuture(TGetMetaResult(FilterCachedMeta(tags)));
        }
    }

    LOG_DEBUG("Meta cache miss (ChunkId: %s)", ~ToString(Id_));

    // Make a copy of tags list to pass it into the closure.
    auto tags_ = MakeNullable(tags);
    auto this_ = MakeStrong(this);
    auto invoker = Bootstrap_->GetControlInvoker();
    return ReadMeta(priority).Apply(
        BIND([=] (TError error) -> TGetMetaResult {
            if (!error.IsOK()) {
                return error;
            }
            return FilterCachedMeta(tags);
        }).AsyncVia(invoker));
}

TAsyncError TBlobChunk::ReadBlocks(
    int firstBlockIndex,
    int blockCount,
    i64 priority,
    std::vector<TSharedRef>* blocks)
{
    YCHECK(firstBlockIndex >= 0);
    YCHECK(blockCount >= 0);

    auto blockStore = Bootstrap_->GetBlockStore();

    i64 pendingSize = ComputePendingReadSize(firstBlockIndex, blockCount);
    if (pendingSize >= 0) {
        blockStore->UpdatePendingReadSize(+pendingSize);
    }

    auto promise = NewPromise<TError>();

    auto callback = BIND(
        &TBlobChunk::DoReadBlocks,
        MakeStrong(this),
        firstBlockIndex,
        blockCount,
        pendingSize,
        promise,
        blocks);

    Location_
        ->GetDataReadInvoker()
        ->Invoke(callback, priority);

    return promise;
}

void TBlobChunk::DoReadBlocks(
    int firstBlockIndex,
    int blockCount,
    i64 pendingSize,
    TPromise<TError> promise,
    std::vector<TSharedRef>* blocks)
{
    auto blockStore = Bootstrap_->GetBlockStore();
    auto readerCache = Bootstrap_->GetBlobReaderCache();

    TFileReaderPtr reader;
    try {
        reader = readerCache->GetReader(this);
    } catch (const std::exception& ex) {
        if (pendingSize >= 0) {
            blockStore->UpdatePendingReadSize(-pendingSize);
        }
        promise.Set(ex);
        return;
    }

    if (pendingSize < 0) {
        InitializeCachedMeta(reader->GetChunkMeta());
        pendingSize = ComputePendingReadSize(firstBlockIndex, blockCount);
        YCHECK(pendingSize >= 0);
    }

    if (firstBlockIndex + blockCount > BlocksExt_.blocks_size()) {
        promise.Set(TError("Chunk %s has no blocks %d-%d",
            ~ToString(Id_),
            BlocksExt_.blocks_size(),
            firstBlockIndex + blockCount - 1));
        return;
    }

    for (int blockIndex = firstBlockIndex; blockIndex < firstBlockIndex + blockCount; ++blockIndex) {
        if ((*blocks)[blockIndex - firstBlockIndex])
            continue;

        TBlockId blockId(Id_, blockIndex);
        NProfiling::TScopedTimer timer;
        TSharedRef block;
        try {
            LOG_DEBUG("Started reading blob chunk block (BlockId: %s, LocationId: %s)",
                ~ToString(blockId),
                ~Location_->GetId());
            
            block = reader->ReadBlock(blockIndex);

            LOG_DEBUG("Finished reading blob chunk block (BlockId: %s, LocationId: %s)",
                ~ToString(blockId),
                ~Location_->GetId());
        } catch (const std::exception& ex) {
            auto error = TError(
                NChunkClient::EErrorCode::IOError,
                "Error reading blob chunk block %s",
                ~ToString(blockId))
                << ex;
            Location_->Disable();
            blockStore->UpdatePendingReadSize(-pendingSize);
            promise.Set(error);
            return;
        }

        (*blocks)[blockIndex - firstBlockIndex] = block;

        auto readTime = timer.GetElapsed();
        i64 readSize = block.Size();

        auto& locationProfiler = Location_->Profiler();
        locationProfiler.Enqueue("/blob_block_read_size", readSize);
        locationProfiler.Enqueue("/blob_block_read_time", readTime.MicroSeconds());
        locationProfiler.Enqueue("/blob_block_read_throughput", readSize * 1000000 / (1 + readTime.MicroSeconds()));
        DataNodeProfiler.Increment(DiskBlobReadThroughputCounter, readSize);
    }

    blockStore->UpdatePendingReadSize(-pendingSize);
    promise.Set(TError());
}

TAsyncError TBlobChunk::ReadMeta(i64 priority)
{
    if (!TryAcquireReadLock()) {
        return MakeFuture(TError("Cannot read meta of chunk %s: chunk is scheduled for removal",
            ~ToString(Id_)));
    }

    auto promise = NewPromise<TError>();
    auto callback = BIND(&TBlobChunk::DoReadMeta, MakeStrong(this), promise);
    Location_
        ->GetMetaReadInvoker()
        ->Invoke(callback, priority);
    return promise;
}

void TBlobChunk::DoReadMeta(TPromise<TError> promise)
{
    auto& Profiler = Location_->Profiler();
    LOG_DEBUG("Started reading chunk meta (ChunkId: %s, LocationId: %s)",
        ~ToString(Id_),
        ~Location_->GetId());

    NChunkClient::TFileReaderPtr reader;
    PROFILE_TIMING ("/meta_read_time") {
        auto readerCache = Bootstrap_->GetBlobReaderCache();
        try {
            reader = readerCache->GetReader(this);
        } catch (const std::exception& ex) {
            ReleaseReadLock();
            LOG_WARNING(ex, "Error reading chunk meta (ChunkId: %s)",
                ~ToString(Id_));
            promise.Set(ex);
            return;
        }
    }

    InitializeCachedMeta(reader->GetChunkMeta());
    ReleaseReadLock();

    LOG_DEBUG("Finished reading chunk meta (ChunkId: %s, LocationId: %s)",
        ~ToString(Id_),
        ~Location_->GetId());

    promise.Set(TError());
}

void TBlobChunk::InitializeCachedMeta(const NChunkClient::NProto::TChunkMeta& meta)
{
    TGuard<TSpinLock> guard(SpinLock_);
    // This check is important since this code may get triggered
    // multiple times and readers do not use any locking.
    if (Meta_)
        return;

    BlocksExt_ = GetProtoExtension<TBlocksExt>(meta.extensions());
    Meta_ = New<TRefCountedChunkMeta>(meta);

    auto* tracker = Bootstrap_->GetMemoryUsageTracker();
    tracker->Acquire(EMemoryConsumer::ChunkMeta, Meta_->SpaceUsed());
}

i64 TBlobChunk::ComputePendingReadSize(int firstBlockIndex, int blockCount)
{
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (!Meta_) {
            return -1;
        }
    }

    i64 result = 0;
    for (int blockIndex = firstBlockIndex;
         blockIndex < firstBlockIndex + blockCount && blockIndex < BlocksExt_.blocks_size();
         ++blockIndex)
    {
        const auto& blockInfo = BlocksExt_.blocks(blockIndex);
        result += blockInfo.size();
    }

    return result;
}

void TBlobChunk::EvictFromCache()
{
    auto readerCache = Bootstrap_->GetBlobReaderCache();
    readerCache->EvictReader(this);
}

TFuture<void> TBlobChunk::RemoveFiles()
{
    auto dataFileName = GetFileName();
    auto metaFileName = dataFileName + ChunkMetaSuffix;
    auto id = Id_;
    auto location = Location_;

    return BIND([=] () {
        LOG_DEBUG("Started removing blob chunk files (ChunkId: %s)",
            ~ToString(id));

        try {
            NFS::Remove(dataFileName);
            NFS::Remove(metaFileName);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error removing blob chunk files");
            location->Disable();
        }

        LOG_DEBUG("Finished removing blob chunk files (ChunkId: %s)",
            ~ToString(id));
    }).AsyncVia(location->GetWriteInvoker()).Run();
}

////////////////////////////////////////////////////////////////////////////////

TStoredBlobChunk::TStoredBlobChunk(
    TBootstrap* bootstrap,
    TLocationPtr location,
    const TChunkId& id,
    const TChunkInfo& info,
    const TChunkMeta* meta)
    : TBlobChunk(
        bootstrap,
        location,
        id,
        info,
        meta)
{ }

////////////////////////////////////////////////////////////////////////////////

TCachedBlobChunk::TCachedBlobChunk(
    TBootstrap* bootstrap,
    TLocationPtr location,
    const TChunkId& id,
    const TChunkInfo& info,
    const TChunkMeta* meta)
    : TBlobChunk(
        bootstrap,
        location,
        id,
        info,
        meta)
    , TCacheValueBase<TChunkId, TCachedBlobChunk>(GetId())
    , ChunkCache_(Bootstrap_->GetChunkCache())
{ }

TCachedBlobChunk::~TCachedBlobChunk()
{
    // This check ensures that we don't remove any chunks from cache upon shutdown.
    if (!ChunkCache_.IsExpired()) {
        LOG_INFO("Chunk is evicted from cache (ChunkId: %s)", ~ToString(GetId()));
        EvictFromCache();
        RemoveFiles();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
