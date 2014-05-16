#include "stdafx.h"
#include "blob_chunk.h"
#include "private.h"
#include "location.h"
#include "reader_cache.h"
#include "chunk_cache.h"
#include "block_store.h"

#include <core/profiling/scoped_timer.h>

#include <ytlib/chunk_client/file_reader.h>
#include <ytlib/chunk_client/data_node_service_proxy.h>
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

static NProfiling::TRateCounter DiskReadThroughputCounter("/disk_read_throughput");

////////////////////////////////////////////////////////////////////////////////

TBlobChunk::TBlobChunk(
    TLocationPtr location,
    const TChunkId& id,
    const TChunkMeta& chunkMeta,
    const TChunkInfo& chunkInfo,
    TNodeMemoryTracker* memoryUsageTracker)
    : Id_(id)
    , Location_(location)
    , Info_(chunkInfo)
    , Meta_(New<TRefCountedChunkMeta>(chunkMeta))
    , MemoryUsageTracker_(memoryUsageTracker)
{
    MemoryUsageTracker_->Acquire(EMemoryConsumer::ChunkMeta, Meta_->SpaceUsed());
    Initialize();
}

TBlobChunk::TBlobChunk(
    TLocationPtr location,
    const TChunkDescriptor& descriptor,
    TNodeMemoryTracker* memoryUsageTracker)
    : Id_(descriptor.Id)
    , Location_(location)
    , MemoryUsageTracker_(memoryUsageTracker)
{
    Info_.set_disk_space(descriptor.DiskSpace);
    Info_.clear_meta_checksum();
    Initialize();
}

void TBlobChunk::Initialize()
{
    ReadLockCounter_ = 0;
    RemovalScheduled_ = false;
}

TBlobChunk::~TBlobChunk()
{
    if (Meta_) {
        MemoryUsageTracker_->Release(EMemoryConsumer::ChunkMeta, Meta_->SpaceUsed());
    }
}

const TChunkId& TBlobChunk::GetId() const
{
    return Id_;
}

TLocationPtr TBlobChunk::GetLocation() const
{
    return Location_;
}

const TChunkInfo& TBlobChunk::GetInfo() const
{
    return Info_;
}

Stroka TBlobChunk::GetFileName() const
{
    return Location_->GetChunkFileName(Id_);
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

            return MakeFuture(TGetMetaResult(
                tags
                ? New<TRefCountedChunkMeta>(FilterChunkMetaByExtensionTags(*Meta_, *tags))
                : Meta_));
        }
    }

    LOG_DEBUG("Meta cache miss (ChunkId: %s)", ~ToString(Id_));

    // Make a copy of tags list to pass it into the closure.
    auto tags_ = MakeNullable(tags);
    auto this_ = MakeStrong(this);
    auto invoker = Location_->GetBootstrap()->GetControlInvoker();
    return ReadMeta(priority).Apply(
        BIND([=] (TError error) -> TGetMetaResult {
            if (!error.IsOK()) {
                return error;
            }

            YCHECK(Meta_);
            return tags_
                ? New<TRefCountedChunkMeta>(FilterChunkMetaByExtensionTags(*Meta_, *tags_))
                : Meta_;
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

    auto blockStore = Location_->GetBootstrap()->GetBlockStore();

    i64 pendingSize = ComputePendingReadSize(firstBlockIndex, blockCount);
    if (pendingSize >= 0) {
        blockStore->IncrementPendingReadSize(pendingSize);
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
    auto* bootstrap = Location_->GetBootstrap();
    auto blockStore = bootstrap->GetBlockStore();
    auto readerCache = bootstrap->GetReaderCache();

    auto readerOrError = readerCache->GetReader(this);
    if (!readerOrError.IsOK()) {
        if (pendingSize >= 0) {
            blockStore->DecrementPendingReadSize(pendingSize);
        }
        promise.Set(readerOrError);
        return;
    }

    const auto& reader = readerOrError.Value();

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
            LOG_DEBUG("Started reading chunk block (BlockId: %s, LocationId: %s)",
                ~ToString(blockId),
                ~Location_->GetId());
            
            block = reader->ReadBlock(blockIndex);

            LOG_DEBUG("Finished reading chunk block (BlockId: %s, LocationId: %s)",
                ~ToString(blockId),
                ~Location_->GetId());
        } catch (const std::exception& ex) {
            auto error = TError(
                NChunkClient::EErrorCode::IOError,
                "Error reading chunk block %s",
                ~ToString(blockId))
                << ex;
            Location_->Disable();
            blockStore->DecrementPendingReadSize(pendingSize);
            promise.Set(error);
            return;
        }

        (*blocks)[blockIndex - firstBlockIndex] = block;

        auto readTime = timer.GetElapsed();
        i64 blockSize = block.Size();

        auto& locationProfiler = Location_->Profiler();
        locationProfiler.Enqueue("/block_read_size", blockSize);
        locationProfiler.Enqueue("/block_read_time", readTime.MicroSeconds());
        locationProfiler.Enqueue("/block_read_speed", blockSize * 1000000 / (1 + readTime.MicroSeconds()));
        DataNodeProfiler.Increment(DiskReadThroughputCounter, blockSize);
    }

    blockStore->DecrementPendingReadSize(pendingSize);

    promise.Set(TError());
}

TRefCountedChunkMetaPtr TBlobChunk::GetCachedMeta() const
{
    return Meta_;
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
        auto readerCache = Location_->GetBootstrap()->GetReaderCache();
        auto result = readerCache->GetReader(this);
        if (!result.IsOK()) {
            ReleaseReadLock();
            LOG_WARNING(result, "Error reading chunk meta (ChunkId: %s)",
                ~ToString(Id_));
            promise.Set(result);
            return;
        }
        reader = result.Value();
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

    MemoryUsageTracker_->Acquire(EMemoryConsumer::ChunkMeta, Meta_->SpaceUsed());
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

bool TBlobChunk::TryAcquireReadLock()
{
    int lockCount;
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (RemovedEvent_) {
            LOG_DEBUG("Chunk read lock cannot be acquired since removal is already pending (ChunkId: %s)",
                ~ToString(Id_));
            return false;
        }

        lockCount = ++ReadLockCounter_;
    }

    LOG_DEBUG("Chunk read lock acquired (ChunkId: %s, LockCount: %d)",
        ~ToString(Id_),
        lockCount);

    return true;
}

void TBlobChunk::ReleaseReadLock()
{
    bool scheduleRemoval = false;
    int lockCount;
    {
        TGuard<TSpinLock> guard(SpinLock_);
        YCHECK(ReadLockCounter_ > 0);
        lockCount = --ReadLockCounter_;
        if (ReadLockCounter_ == 0 && !RemovalScheduled_ && RemovedEvent_) {
            scheduleRemoval = RemovalScheduled_ = true;
        }
    }

    LOG_DEBUG("Chunk read lock released (ChunkId: %s, LockCount: %d)",
        ~ToString(Id_),
        lockCount);

    if (scheduleRemoval) {
        DoRemoveChunk();
    }
}

bool TBlobChunk::IsReadLockAcquired() const
{
    return ReadLockCounter_ > 0;
}

TFuture<void> TBlobChunk::ScheduleRemoval()
{
    bool scheduleRemoval = false;

    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (RemovedEvent_) {
            return RemovedEvent_;
        }

        RemovedEvent_ = NewPromise();
        if (ReadLockCounter_ == 0 && !RemovalScheduled_) {
            scheduleRemoval = RemovalScheduled_ = true;
        }
    }

    if (scheduleRemoval) {
        DoRemoveChunk();
    }

    return RemovedEvent_;
}

void TBlobChunk::DoRemoveChunk()
{
    EvictChunkReader();

    auto this_ = MakeStrong(this);
    Location_->ScheduleChunkRemoval(this).Subscribe(BIND([=] () {
        this_->RemovedEvent_.Set();
    }));
}

void TBlobChunk::EvictChunkReader()
{
    auto readerCache = Location_->GetBootstrap()->GetReaderCache();
    readerCache->EvictReader(this);
}

////////////////////////////////////////////////////////////////////////////////

TStoredBlobChunk::TStoredBlobChunk(
    TLocationPtr location,
    const TChunkId& chunkId,
    const TChunkMeta& chunkMeta,
    const TChunkInfo& chunkInfo,
    TNodeMemoryTracker* memoryUsageTracker)
    : TBlobChunk(
        location,
        chunkId,
        chunkMeta,
        chunkInfo,
        memoryUsageTracker)
{ }

TStoredBlobChunk::TStoredBlobChunk(
    TLocationPtr location,
    const TChunkDescriptor& descriptor,
    TNodeMemoryTracker* memoryUsageTracker)
    : TBlobChunk(
        location,
        descriptor,
        memoryUsageTracker)
{ }

////////////////////////////////////////////////////////////////////////////////

TCachedBlobChunk::TCachedBlobChunk(
    TLocationPtr location,
    const TChunkId& chunkId,
    const TChunkMeta& chunkMeta,
    const TChunkInfo& chunkInfo,
    TChunkCachePtr chunkCache,
    TNodeMemoryTracker* memoryUsageTracker)
    : TBlobChunk(
        location,
        chunkId,
        chunkMeta,
        chunkInfo,
        memoryUsageTracker)
    , TCacheValueBase<TChunkId, TCachedBlobChunk>(GetId())
    , ChunkCache_(chunkCache)
{ }

TCachedBlobChunk::TCachedBlobChunk(
    TLocationPtr location,
    const TChunkDescriptor& descriptor,
    TChunkCachePtr chunkCache,
    TNodeMemoryTracker* memoryUsageTracker)
    : TBlobChunk(
        location,
        descriptor,
        memoryUsageTracker)
    , TCacheValueBase<TChunkId, TCachedBlobChunk>(GetId())
    , ChunkCache_(chunkCache)
{ }

TCachedBlobChunk::~TCachedBlobChunk()
{
    // This check ensures that we don't remove any chunks from cache upon shutdown.
    if (!ChunkCache_.IsExpired()) {
        LOG_INFO("Chunk is evicted from cache (ChunkId: %s)", ~ToString(GetId()));
        EvictChunkReader();
        Location_->ScheduleChunkRemoval(this);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
