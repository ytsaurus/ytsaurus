#include "blob_reader_cache.h"
#include "private.h"
#include "blob_chunk.h"
#include "config.h"
#include "location.h"
#include "chunk_meta_manager.h"

#include <yt/server/node/cluster_node/bootstrap.h>
#include <yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/server/node/cluster_node/config.h>

#include <yt/ytlib/chunk_client/file_reader.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/async_slru_cache.h>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NConcurrency;

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

class TCachedReader
    : public TAsyncCacheValueBase<TReaderCacheKey, TCachedReader>
    , public TFileReader
    , public IBlocksExtCache
{
public:
    TCachedReader(
        const IChunkMetaManagerPtr& chunkMetaManager,
        const TBlobChunkBasePtr& chunk,
        const TString& fileName,
        bool validateBlockChecksums)
        : TAsyncCacheValueBase<TReaderCacheKey, TCachedReader>(
            MakeReaderCacheKey(chunk.Get()))
        , TFileReader(
            chunk->GetLocation()->GetIOEngine(),
            chunk->GetId(),
            fileName,
            validateBlockChecksums,
            this)
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
        ChunkMetaManager_->PutCachedBlocksExt(GetChunkId(), blocksExt);
    }

private:
    const IChunkMetaManagerPtr ChunkMetaManager_;
    const TBlobChunkBasePtr Chunk_;
};

////////////////////////////////////////////////////////////////////////////////

class TBlobReaderCache
    : public TAsyncSlruCacheBase<TReaderCacheKey, TCachedReader>
    , public IBlobReaderCache
{
public:
    explicit TBlobReaderCache(NClusterNode::TBootstrap* bootstrap)
        : TAsyncSlruCacheBase(
            bootstrap->GetConfig()->DataNode->BlobReaderCache,
            DataNodeProfiler.WithPrefix("/block_reader_cache"))
        , Bootstrap_(bootstrap)
        , Config_(Bootstrap_->GetConfig()->DataNode)
    { }

    virtual TFileReaderPtr GetReader(const TBlobChunkBasePtr& chunk) override
    {
        auto readGuard = TChunkReadGuard::Acquire(chunk);
        const auto& location = chunk->GetLocation();
        auto chunkId = chunk->GetId();
        auto cookie = BeginInsert(MakeReaderCacheKey(chunk.Get()));
        if (cookie.IsActive()) {
            auto fileName = chunk->GetFileName();
            YT_LOG_TRACE("Started opening blob chunk reader (LocationId: %v, ChunkId: %v)",
                location->GetId(),
                chunkId);

            try {
                NProfiling::TEventTimer timingGuard(location->GetPerformanceCounters().BlobChunkReaderOpenTime);

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
            }

            YT_LOG_TRACE("Finished opening blob chunk reader (LocationId: %v, ChunkId: %v)",
                chunk->GetLocation()->GetId(),
                chunkId);
        }

        return WaitFor(cookie.GetValue())
            .ValueOrThrow();
    }

    virtual void EvictReader(TBlobChunkBase* chunk) override
    {
        TAsyncSlruCacheBase::TryRemove(MakeReaderCacheKey(chunk));
    }

private:
    NClusterNode::TBootstrap* const Bootstrap_;
    const TDataNodeConfigPtr Config_;

    void OnDynamicConfigChanged(
        const NClusterNode::TClusterNodeDynamicConfigPtr& /* oldNodeConfig */,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        const auto& config = newNodeConfig->DataNode;
        TAsyncSlruCacheBase::Reconfigure(config->BlobReaderCache);
    }
};

////////////////////////////////////////////////////////////////////////////////

IBlobReaderCachePtr CreateBlobReaderCache(NClusterNode::TBootstrap* bootstrap)
{
    return New<TBlobReaderCache>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
