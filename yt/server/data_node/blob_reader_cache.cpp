#include "blob_reader_cache.h"
#include "private.h"
#include "chunk.h"
#include "config.h"
#include "location.h"

#include <yt/ytlib/chunk_client/file_reader.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/async_cache.h>

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

typedef std::pair<TString, TChunkId> TReaderCacheKey;

////////////////////////////////////////////////////////////////////////////////

class TBlobReaderCache::TCachedReader
    : public TAsyncCacheValueBase<TReaderCacheKey, TCachedReader>
    , public TFileReader
{
public:
    TCachedReader(
        const IIOEnginePtr& ioEngine,
        const TString& locationId,
        const TChunkId& chunkId,
        const TString& fileName,
        bool validateBlockChecksums)
        : TAsyncCacheValueBase<TReaderCacheKey, TCachedReader>({locationId, chunkId})
        , TFileReader(ioEngine, chunkId, fileName, validateBlockChecksums)
        , ChunkId_(chunkId)
    { }

    virtual TChunkId GetChunkId() const override
    {
        return ChunkId_;
    }

    virtual bool IsValid() const override
    {
        return true;
    }

private:
    const TChunkId ChunkId_;

};

////////////////////////////////////////////////////////////////////////////////

class TBlobReaderCache::TImpl
    : public TAsyncSlruCacheBase<TReaderCacheKey, TCachedReader>
{
public:
    explicit TImpl(TDataNodeConfigPtr config)
        : TAsyncSlruCacheBase(
            config->BlobReaderCache,
            NProfiling::TProfiler(DataNodeProfiler.GetPathPrefix() + "/block_reader_cache"))
        , Config_(config)
    { }

    TFileReaderPtr GetReader(IChunkPtr chunk)
    {
        auto guard = TChunkReadGuard::AcquireOrThrow(chunk.Get());
        auto location = chunk->GetLocation();
        auto chunkId = chunk->GetId();
        auto cookie = BeginInsert({location->GetId(), chunkId});
        if (cookie.IsActive()) {
            auto fileName = chunk->GetFileName();
            LOG_TRACE("Started opening blob chunk reader (LocationId: %v, ChunkId: %v)",
                location->GetId(),
                chunkId);

            {
                NProfiling::TAggregatedTimingGuard(&location->GetProfiler(), &location->GetPerformanceCounters().BlobChunkReaderOpenTime);
                try {
                    auto reader = New<TCachedReader>(location->GetIOEngine(), location->GetId(), chunkId, fileName, Config_->ValidateBlockChecksums);
                    cookie.EndInsert(reader);
                } catch (const std::exception& ex) {
                    auto error = TError(
                        NChunkClient::EErrorCode::IOError,
                        "Error opening blob chunk %v",
                        chunkId)
                        << ex;
                    cookie.Cancel(error);
                    chunk->GetLocation()->Disable(error);
                    Y_UNREACHABLE(); // Disable() exits the process.
                }
            }

            LOG_TRACE("Finished opening blob chunk reader (LocationId: %v, ChunkId: %v)",
                chunk->GetLocation()->GetId(),
                chunkId);
        }

        return cookie.GetValue().Get().ValueOrThrow();
    }

    void EvictReader(IChunk* chunk)
    {
        TAsyncSlruCacheBase::TryRemove({chunk->GetLocation()->GetId(), chunk->GetId()});
    }

private:
    const TDataNodeConfigPtr Config_;


    virtual void OnAdded(const TCachedReaderPtr& reader) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TAsyncSlruCacheBase::OnAdded(reader);

        LOG_TRACE("Block chunk reader added to cache (Location: %v, ChunkId: %v)",
            reader->GetKey().first,
            reader->GetKey().second);
    }

    virtual void OnRemoved(const TCachedReaderPtr& reader) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TAsyncSlruCacheBase::OnRemoved(reader);

        LOG_TRACE("Block chunk reader removed from cache (Location: %v, ChunkId: %v)",
            reader->GetKey().first,
            reader->GetKey().second);
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
