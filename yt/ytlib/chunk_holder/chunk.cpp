#include "stdafx.h"
#include "chunk.h"
#include "private.h"
#include "location.h"
#include "reader_cache.h"
#include "chunk_holder_service_proxy.h"
#include "chunk_cache.h"
#include "chunk_meta_extensions.h"

#include <ytlib/chunk_client/file_reader.h>

namespace NYT {
namespace NChunkHolder {

using namespace NChunkClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;
static NProfiling::TProfiler& Profiler = ChunkHolderProfiler;

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
{ }

TChunk::TChunk(
    TLocationPtr location,
    const TChunkDescriptor& descriptor)
    : Id_(descriptor.Id)
    , Location_(location)
    , HasMeta(false)
{
    Info_.set_size(descriptor.Size);
    Info_.clear_meta_checksum();
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
    auto invoker = Location_->GetInvoker();
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
    auto this_ = MakeStrong(this);
    auto invoker = Location_->GetInvoker();
    auto readerCache = Location_->GetReaderCache();
    auto timer = Profiler.TimingStart(Sprintf("/chunk_io/%s/meta_read_time", ~Location_->GetId()));

    LOG_DEBUG("Reading chunk meta (ChunkId: %s)", ~Id_.ToString());

    return
        BIND([=] () mutable -> TError {
            auto result = readerCache->GetReader(this_);
            if (!result.IsOK()) {
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

            Profiler.TimingStop(timer);

            LOG_DEBUG("Chunk meta is read (ChunkId: %s)", ~this_->Id_.ToString());

            return TError();
        })
        .AsyncVia(invoker)
        .Run();
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
