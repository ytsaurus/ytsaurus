#include "stdafx.h"
#include "chunk.h"
#include "private.h"
#include "location.h"
#include "reader_cache.h"
#include "chunk_holder_service_proxy.h"
#include "chunk_cache.h"
#include "chunk_meta_extensions.h"
#include "bootstrap.h"

#include <ytlib/chunk_client/file_reader.h>

namespace NYT {
namespace NChunkHolder {

using namespace NChunkClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = DataNodeLogger;
static NProfiling::TProfiler& Profiler = DataNodeProfiler;

////////////////////////////////////////////////////////////////////////////////

TChunk::TChunk(
    TLocationPtr location, 
    const TChunkId& id, 
    const TChunkMeta& chunkMeta, 
    const TChunkInfo& chunkInfo)
    : Id_(id)
    , Location_(location)
    , Info_(chunkInfo)
    , HasMeta(true)
    , Meta(chunkMeta)
{
    Initialize();
}

TChunk::TChunk(
    TLocationPtr location,
    const TChunkDescriptor& descriptor)
    : Id_(descriptor.Id)
    , Location_(location)
    , HasMeta(false)
{
    Info_.set_size(descriptor.Size);
    Info_.clear_meta_checksum();
    Initialize();
}

void TChunk::Initialize()
{
    ReadLockCounter = 0;
    RemovalPending = false;
    RemovalScheduled = false;
}

TChunk::~TChunk()
{ }

Stroka TChunk::GetFileName() const
{
    return Location_->GetChunkFileName(Id_);
}

TChunk::TAsyncGetMetaResult TChunk::GetMeta(const std::vector<int>* tags)
{
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (HasMeta) {
            return MakeFuture(TGetMetaResult(
                tags
                ? FilterChunkMetaExtensions(Meta, *tags)
                : Meta));
        }
    }

    // Make a copy of tags list to pass it into the closure.
    auto tags_ = MakeNullable(tags);
    auto this_ = MakeStrong(this);
    auto invoker = Location_->GetBootstrap()->GetControlInvoker();
    return ReadMeta().Apply(
        BIND([=] (TError error) -> TGetMetaResult {
            if (!error.IsOK()) {
                return error;
            }
            
            YCHECK(HasMeta);
            return tags_
                ? FilterChunkMetaExtensions(Meta, tags_.Get())
                : Meta;
        }).AsyncVia(invoker));
}

TFuture<TError> TChunk::ReadMeta()
{
    if (!AcquireReadLock()) {
        return MakeFuture(TError("Cannot read meta of chunk %s: chunk is scheduled for removal",
            ~Id_.ToString()));
    }

    auto this_ = MakeStrong(this);
    auto readerCache = Location_->GetBootstrap()->GetReaderCache();

    LOG_DEBUG("Reading chunk meta (ChunkId: %s)", ~Id_.ToString());

    auto timer = Profiler.TimingStart("/chunk_io/meta_read_time");
    return
        BIND([=] () mutable -> TError {
            auto result = readerCache->GetReader(this_);
            if (!result.IsOK()) {
                this_->ReleaseReadLock();
                LOG_WARNING("Error reading chunk meta (ChunkId: %s)\n%s",
                    ~this_->Id_.ToString(),
                    ~result.ToString());
                return TError(result);
            }

            auto reader = result.Value();

            {
                TGuard<TSpinLock> guard(SpinLock);
                // These are very quick getters.
                Meta = reader->GetChunkMeta();
                Info_ = reader->GetChunkInfo();
                HasMeta = true;
            }

            this_->ReleaseReadLock();

            Profiler.TimingStop(timer);

            LOG_DEBUG("Chunk meta is read (ChunkId: %s)", ~this_->Id_.ToString());

            return TError();
        })
        .AsyncVia(Location_->GetReadInvoker())
        .Run();
}

bool TChunk::AcquireReadLock()
{
    int lockCount;
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (RemovalPending) {
            LOG_DEBUG("Chunk read lock cannot be acquired since removal is already pending (ChunkId: %s)",
                ~ToString(Id_));
            return false;
        }

        lockCount = ++ReadLockCounter;
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
        TGuard<TSpinLock> guard(SpinLock);
        YCHECK(ReadLockCounter > 0);
        lockCount = --ReadLockCounter;
        if (ReadLockCounter == 0 && !RemovalScheduled && RemovalPending) {
            scheduleRemoval = RemovalScheduled = true;
        }
    }

    LOG_DEBUG("Chunk read lock released (ChunkId: %s, LockCount: %d)",
        ~ToString(Id_),
        lockCount);

    if (scheduleRemoval) {
        Location_->ScheduleChunkRemoval(this);
    }
}

void TChunk::ScheduleRemoval()
{
    bool scheduleRemoval = false;

    {
        TGuard<TSpinLock> guard(SpinLock);
        if (RemovalScheduled || RemovalPending) {
            return;
        }

        RemovalPending = true;
        if (ReadLockCounter == 0 && !RemovalScheduled) {
            scheduleRemoval = RemovalScheduled = true;
        }
    }

    if (scheduleRemoval) {
        Location_->ScheduleChunkRemoval(this);
    }
}

////////////////////////////////////////////////////////////////////////////////

TStoredChunk::TStoredChunk(
    TLocationPtr location,
    const TChunkId& chunkId,
    const TChunkMeta& chunkMeta,
    const TChunkInfo& chunkInfo)
    : TChunk(location, chunkId, chunkMeta, chunkInfo)
{ }

TStoredChunk::TStoredChunk(TLocationPtr location, const TChunkDescriptor& descriptor)
    : TChunk(location, descriptor)
{ }

TStoredChunk::~TStoredChunk()
{ }

////////////////////////////////////////////////////////////////////////////////

TCachedChunk::TCachedChunk(
    TLocationPtr location,
    const TChunkId& chunkId,
    const TChunkMeta& chunkMeta,
    const TChunkInfo& chunkInfo,
    TChunkCachePtr chunkCache)
    : TChunk(location, chunkId, chunkMeta, chunkInfo)
    , TCacheValueBase<TChunkId, TCachedChunk>(GetId())
    , ChunkCache(chunkCache)
{ }

TCachedChunk::TCachedChunk(
    TLocationPtr location,
    const TChunkDescriptor& descriptor,
    TChunkCachePtr chunkCache)
    : TChunk(location, descriptor)
    , TCacheValueBase<TChunkId, TCachedChunk>(GetId())
    , ChunkCache(chunkCache)
{ }

TCachedChunk::~TCachedChunk()
{
    // This check ensures that we don't remove any chunks from cache upon shutdown.
    if (!ChunkCache.IsExpired()) {
        LOG_INFO("Chunk is evicted from cache (ChunkId: %s)", ~GetId().ToString());
        Location_->ScheduleChunkRemoval(this);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
