#include "blob_reader_cache.h"
#include "private.h"
#include "blob_chunk.h"
#include "config.h"
#include "location.h"
#include "chunk_meta_manager.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/ytlib/chunk_client/file_reader.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/async_cache.h>

namespace NYT::NDataNode {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

using TReaderCacheKey = std::pair<TLocationPtr, TChunkId>;

namespace {

TReaderCacheKey MakeReaderCacheKey(TBlobChunkBase* chunk)
{
    return {
        chunk->GetLocation(),
        chunk->GetId()
    };
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TBlobReaderCache::TCachedReader
    : public TAsyncCacheValueBase<TReaderCacheKey, TCachedReader>
    , public TFileReader
    , public IBlocksExtCache
{
public:
    TCachedReader(
        const TChunkMetaManagerPtr& chunkMetaManager,
        const TBlobChunkBasePtr& chunk,
        const TString& fileName,
        bool validateBlockChecksums)
        : TAsyncCacheValueBase<TReaderCacheKey, TCachedReader>(
            MakeReaderCacheKey(chunk.Get()))
        , TFileReader(
            chunk->GetLocation()->GetIOEngine(),
            chunk->GetId(), fileName, validateBlockChecksums)
        , ChunkMetaManager_(chunkMetaManager)
        , Chunk_(chunk)
    { }

    // IBlocksExtCache implementation.
    virtual TRefCountedBlocksExtPtr Find() override
    {
        return Chunk_->FindCachedBlocksExt();
    }

    virtual void Put(
        const TRefCountedChunkMetaPtr& chunkMeta,
        const TRefCountedBlocksExtPtr& blocksExt) override
    {
        ChunkMetaManager_->PutCachedMeta(GetChunkId(), chunkMeta);
    }

private:
    const TChunkMetaManagerPtr ChunkMetaManager_;
    const TBlobChunkBasePtr Chunk_;
};

////////////////////////////////////////////////////////////////////////////////

class TBlobReaderCache::TImpl
    : public TAsyncSlruCacheBase<TReaderCacheKey, TCachedReader>
{
public:
    TImpl(
        const TDataNodeConfigPtr& config,
        NCellNode::TBootstrap* bootstrap)
        : TAsyncSlruCacheBase(
            config->BlobReaderCache,
            DataNodeProfiler.AppendPath("/block_reader_cache"))
        , Config_(config)
        , Bootstrap_(bootstrap)
    { }

    TFileReaderPtr GetReader(const TBlobChunkBasePtr& chunk)
    {
        auto guard = TChunkReadGuard::AcquireOrThrow(chunk);
        auto location = chunk->GetLocation();
        auto chunkId = chunk->GetId();
        auto cookie = BeginInsert(MakeReaderCacheKey(chunk.Get()));
        if (cookie.IsActive()) {
            auto fileName = chunk->GetFileName();
            LOG_TRACE("Started opening blob chunk reader (LocationId: %v, ChunkId: %v)",
                location->GetId(),
                chunkId);

            {
                NProfiling::TAggregatedTimingGuard(&location->GetProfiler(), &location->GetPerformanceCounters().BlobChunkReaderOpenTime);
                try {
                    auto reader = New<TCachedReader>(
                        Bootstrap_->GetChunkMetaManager(),
                        chunk,
                        fileName,
                        Config_->ValidateBlockChecksums);
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

    void EvictReader(TBlobChunkBase* chunk)
    {
        TAsyncSlruCacheBase::TryRemove(MakeReaderCacheKey(chunk));
    }

private:
    const TDataNodeConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

};

////////////////////////////////////////////////////////////////////////////////

TBlobReaderCache::TBlobReaderCache(
    TDataNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        std::move(config),
        bootstrap))
{ }

TBlobReaderCache::~TBlobReaderCache() = default;

TFileReaderPtr TBlobReaderCache::GetReader(const TBlobChunkBasePtr& chunk)
{
    return Impl_->GetReader(chunk);
}

void TBlobReaderCache::EvictReader(TBlobChunkBase* chunk)
{
    Impl_->EvictReader(chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
