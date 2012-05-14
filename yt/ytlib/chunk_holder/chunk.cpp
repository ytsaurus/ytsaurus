#include "stdafx.h"
#include "chunk.h"
#include "common.h"
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

////////////////////////////////////////////////////////////////////////////////

TChunk::TChunk(
    TLocation* location, 
    const TChunkId& id, 
    const TChunkMeta& chunkMeta, 
    const TChunkInfo& chunkInfo)
    : Id_(id)
    , Location_(location)
    , Info_(chunkInfo)
    , HasMeta(true)
    , Meta(chunkMeta)
{ }

TChunk::TChunk(TLocation* location, const TChunkDescriptor& descriptor)
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
                ? ExtractChunkMetaExtensions(Meta, *tags)
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
            YASSERT(HasMeta);
            return tags_
                ? ExtractChunkMetaExtensions(Meta, tags_.Get())
                : Meta;
        }).AsyncVia(invoker));
}

TFuture<TError> TChunk::ReadMeta()
{
    auto this_ = MakeStrong(this);
    auto invoker = Location_->GetInvoker();
    auto readerCache = Location_->GetReaderCache();
    return
        BIND([=] () -> TError {
            auto result = readerCache->GetReader(this_);
            if (!result.IsOK()) {
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

            return TError();
        })
        .AsyncVia(invoker)
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

TStoredChunk::TStoredChunk(
    TLocation* location,
    const TChunkId& chunkId,
    const TChunkMeta& chunkMeta,
    const TChunkInfo& chunkInfo)
    : TChunk(location, chunkId, chunkMeta, chunkInfo)
{ }

TStoredChunk::TStoredChunk(TLocation* location, const TChunkDescriptor& descriptor)
    : TChunk(location, descriptor)
{ }

TStoredChunk::~TStoredChunk()
{ }

////////////////////////////////////////////////////////////////////////////////

TCachedChunk::TCachedChunk(
    TLocation* location,
    const TChunkId& chunkId,
    const TChunkMeta& chunkMeta,
    const TChunkInfo& chunkInfo,
    TChunkCache* chunkCache)
    : TChunk(location, chunkId, chunkMeta, chunkInfo)
    , TCacheValueBase<TChunkId, TCachedChunk>(GetId())
    , ChunkCache(chunkCache)
{ }

TCachedChunk::TCachedChunk(TLocation* location, const TChunkDescriptor& descriptor, TChunkCache* chunkCache)
    : TChunk(location, descriptor)
    , TCacheValueBase<TChunkId, TCachedChunk>(GetId())
    , ChunkCache(chunkCache)
{ }

TCachedChunk::~TCachedChunk()
{
    // This check ensures that we don't remove any chunks from cache upon shutdown.
    if (!ChunkCache.IsExpired()) {
        LOG_INFO("Chunk is evicted from cache (ChunkId: %s)", ~GetId().ToString());
        Location_->RemoveChunk(this);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
