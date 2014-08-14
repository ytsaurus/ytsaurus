#include "stdafx.h"
#include "blob_reader_cache.h"
#include "private.h"
#include "config.h"
#include "chunk.h"
#include "location.h"

#include <ytlib/chunk_client/file_reader.h>

#include <core/misc/cache.h>

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TBlobReaderCache::TCachedReader
    : public TCacheValueBase<TChunkId, TCachedReader>
    , public TFileReader
{
public:
    TCachedReader(const TChunkId& chunkId, const Stroka& fileName)
        : TCacheValueBase<TChunkId, TCachedReader>(chunkId)
        , TFileReader(fileName)
        , ChunkId_(chunkId)
    { }

    virtual TChunkId GetChunkId() const override
    {
        return ChunkId_;
    }

private:
    TChunkId ChunkId_;

};

////////////////////////////////////////////////////////////////////////////////

class TBlobReaderCache::TImpl
    : public TSizeLimitedCache<TChunkId, TCachedReader>
{
public:
    explicit TImpl(TDataNodeConfigPtr config)
        : TSizeLimitedCache<TChunkId, TCachedReader>(config->BlobReaderCacheSize)
    { }

    TFileReaderPtr GetReader(IChunkPtr chunk)
    {
        YCHECK(chunk->IsReadLockAcquired());

        auto location = chunk->GetLocation();
        auto& Profiler = location->Profiler();

        auto chunkId = chunk->GetId();
        TInsertCookie cookie(chunkId);
        if (BeginInsert(&cookie)) {
            auto fileName = chunk->GetFileName();
            LOG_DEBUG("Started opening blob chunk reader (LocationId: %v, ChunkId: %v)",
                location->GetId(),
                chunkId);

            PROFILE_TIMING ("/blob_chunk_reader_open_time") {
                try {
                    auto reader = New<TCachedReader>(chunkId, fileName);
                    reader->Open();
                    cookie.EndInsert(reader);
                } catch (const std::exception& ex) {
                    auto error = TError(
                        NChunkClient::EErrorCode::IOError,
                        "Error opening blob chunk %v",
                        chunkId)
                        << ex;
                    cookie.Cancel(error);
                    chunk->GetLocation()->Disable(error);
                    THROW_ERROR error;
                }
            }

            LOG_DEBUG("Finished opening blob chunk reader (LocationId: %v, ChunkId: %v)",
                chunk->GetLocation()->GetId(),
                chunkId);
        }

        auto resultOrError = cookie.GetValue().Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(resultOrError);
        return resultOrError.Value();
    }

    void EvictReader(IChunk* chunk)
    {
        TCacheBase::Remove(chunk->GetId());
    }

private:
    virtual void OnAdded(TCachedReader* reader) override
    {
        LOG_DEBUG("Block chunk reader added to cache (ChunkId: %v)",
            reader->GetKey());
    }

    virtual void OnRemoved(TCachedReader* reader) override
    {
        LOG_DEBUG("Block chunk reader evicted from cache (ChunkId: %v)",
            reader->GetKey());
    }

};

////////////////////////////////////////////////////////////////////////////////

TBlobReaderCache::TBlobReaderCache(TDataNodeConfigPtr config)
    : Impl_(New<TImpl>(config))
{ }

TBlobReaderCache::~TBlobReaderCache()
{ }

TFileReaderPtr TBlobReaderCache::GetReader(IChunkPtr chunk)
{
    return Impl_->GetReader(chunk);
}

void TBlobReaderCache::EvictReader(IChunk* chunk)
{
    Impl_->EvictReader(chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
