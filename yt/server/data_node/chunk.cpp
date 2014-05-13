#include "stdafx.h"
#include "chunk.h"
#include "private.h"
#include "location.h"
#include "reader_cache.h"
#include "chunk_cache.h"

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

////////////////////////////////////////////////////////////////////////////////

TRefCountedChunkMeta::TRefCountedChunkMeta()
{ }

TRefCountedChunkMeta::TRefCountedChunkMeta(const TRefCountedChunkMeta& other)
{
    CopyFrom(other);
}

TRefCountedChunkMeta::TRefCountedChunkMeta(TRefCountedChunkMeta&& other)
{
    Swap(&other);
}

TRefCountedChunkMeta::TRefCountedChunkMeta(const NChunkClient::NProto::TChunkMeta& other)
{
    CopyFrom(other);
}

TRefCountedChunkMeta::TRefCountedChunkMeta(NChunkClient::NProto::TChunkMeta&& other)
{
    Swap(&other);
}

////////////////////////////////////////////////////////////////////////////////

TChunk::TChunk(
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

TChunk::TChunk(
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

void TChunk::Initialize()
{
    ReadLockCounter_ = 0;
    RemovalScheduled_ = false;
}

TChunk::~TChunk()
{
    if (Meta_) {
        MemoryUsageTracker_->Release(EMemoryConsumer::ChunkMeta, Meta_->SpaceUsed());
    }
}

Stroka TChunk::GetFileName() const
{
    return Location_->GetChunkFileName(Id_);
}

TChunk::TAsyncGetMetaResult TChunk::GetMeta(
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

TRefCountedChunkMetaPtr TChunk::GetCachedMeta() const
{
    return Meta_;
}

TAsyncError TChunk::ReadMeta(i64 priority)
{
    if (!TryAcquireReadLock()) {
        return MakeFuture(TError("Cannot read meta of chunk %s: chunk is scheduled for removal",
            ~ToString(Id_)));
    }

    auto promise = NewPromise<TError>();
    auto action = BIND(&TChunk::DoReadMeta, MakeStrong(this), promise);
    Location_
        ->GetMetaReadInvoker()
        ->Invoke(action, priority);
    return promise;
}

void TChunk::DoReadMeta(TPromise<TError> promise)
{
    auto& Profiler = Location_->Profiler();
    LOG_DEBUG("Started reading meta (ChunkId: %s, LocationId: %s)",
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

    {
        TGuard<TSpinLock> guard(SpinLock_);
        // This check is important since this code may get triggered
        // multiple times and readers do not use any locking.
        if (!Meta_) {
            // These are very quick getters.
            Meta_ = New<TRefCountedChunkMeta>(reader->GetChunkMeta());
            MemoryUsageTracker_->Acquire(EMemoryConsumer::ChunkMeta, Meta_->SpaceUsed());
        }
    }

    ReleaseReadLock();
    LOG_DEBUG("Finished reading meta (ChunkId: %s, LocationId: %s)",
        ~ToString(Id_),
        ~Location_->GetId());

    promise.Set(TError());
}

bool TChunk::TryAcquireReadLock()
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

void TChunk::ReleaseReadLock()
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

TFuture<void> TChunk::ScheduleRemoval()
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

void TChunk::DoRemoveChunk()
{
    EvictChunkReader();

    auto this_ = MakeStrong(this);
    Location_->ScheduleChunkRemoval(this).Subscribe(BIND([=] () {
        this_->RemovedEvent_.Set();
    }));
}

void TChunk::EvictChunkReader()
{
    auto readerCache = Location_->GetBootstrap()->GetReaderCache();
    readerCache->EvictReader(this);
}

////////////////////////////////////////////////////////////////////////////////

TStoredChunk::TStoredChunk(
    TLocationPtr location,
    const TChunkId& chunkId,
    const TChunkMeta& chunkMeta,
    const TChunkInfo& chunkInfo,
    TNodeMemoryTracker* memoryUsageTracker)
    : TChunk(
        location,
        chunkId,
        chunkMeta,
        chunkInfo,
        memoryUsageTracker)
{ }

TStoredChunk::TStoredChunk(
    TLocationPtr location,
    const TChunkDescriptor& descriptor,
    TNodeMemoryTracker* memoryUsageTracker)
    : TChunk(
        location,
        descriptor,
        memoryUsageTracker)
{ }

////////////////////////////////////////////////////////////////////////////////

TCachedChunk::TCachedChunk(
    TLocationPtr location,
    const TChunkId& chunkId,
    const TChunkMeta& chunkMeta,
    const TChunkInfo& chunkInfo,
    TChunkCachePtr chunkCache,
    TNodeMemoryTracker* memoryUsageTracker)
    : TChunk(
        location,
        chunkId,
        chunkMeta,
        chunkInfo,
        memoryUsageTracker)
    , TCacheValueBase<TChunkId, TCachedChunk>(GetId())
    , ChunkCache_(chunkCache)
{ }

TCachedChunk::TCachedChunk(
    TLocationPtr location,
    const TChunkDescriptor& descriptor,
    TChunkCachePtr chunkCache,
    TNodeMemoryTracker* memoryUsageTracker)
    : TChunk(
        location,
        descriptor,
        memoryUsageTracker)
    , TCacheValueBase<TChunkId, TCachedChunk>(GetId())
    , ChunkCache_(chunkCache)
{ }

TCachedChunk::~TCachedChunk()
{
    // This check ensures that we don't remove any chunks from cache upon shutdown.
    if (!ChunkCache_.IsExpired()) {
        LOG_INFO("Chunk is evicted from cache (ChunkId: %s)", ~ToString(GetId()));
        EvictChunkReader();
        Location_->ScheduleChunkRemoval(this);
    }
}

////////////////////////////////////////////////////////////////////////////////

TJournalChunk::TJournalChunk(
    TLocationPtr location,
    const TChunkId& chunkId,
    const TChunkMeta& chunkMeta,
    const TChunkInfo& chunkInfo,
    TNodeMemoryTracker* memoryUsageTracker)
    : TChunk(
        location,
        chunkId,
        chunkMeta,
        chunkInfo,
        memoryUsageTracker)
{ }

TJournalChunk::TJournalChunk(
    TLocationPtr location,
    const TChunkDescriptor& descriptor,
    TNodeMemoryTracker* memoryUsageTracker)
    : TChunk(
        location,
        descriptor,
        memoryUsageTracker)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
