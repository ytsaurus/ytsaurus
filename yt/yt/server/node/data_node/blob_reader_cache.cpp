#include "blob_reader_cache.h"
#include "bootstrap.h"
#include "private.h"
#include "blob_chunk.h"
#include "config.h"
#include "location.h"
#include "chunk_meta_manager.h"

#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/io/chunk_file_reader.h>

#include <yt/yt/core/misc/sync_cache.h>

namespace NYT::NDataNode {

using namespace NIO;
using namespace NChunkClient;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

using TBlobReaderCacheKey = std::pair<TLocationPtr, TChunkId>;

namespace {

TBlobReaderCacheKey MakeReaderCacheKey(TBlobChunkBase* chunk)
{
    return {
        chunk->GetLocation(),
        chunk->GetId()
    };
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCachedBlobReader)

class TCachedBlobReader
    : public TSyncCacheValueBase<TBlobReaderCacheKey, TCachedBlobReader>
    , public TChunkFileReader
    , public IBlocksExtCache
{
public:
    TCachedBlobReader(
        const IChunkMetaManagerPtr& chunkMetaManager,
        const TBlobChunkBasePtr& chunk,
        const TString& fileName,
        bool validateBlockChecksums)
        : TSyncCacheValueBase<TBlobReaderCacheKey, TCachedBlobReader>(
            MakeReaderCacheKey(chunk.Get()))
        , TChunkFileReader(
            chunk->GetLocation()->GetIOEngine(),
            chunk->GetId(),
            fileName,
            validateBlockChecksums,
            chunk->GetLocation()->IsDirectIOEnabled(),
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

DEFINE_REFCOUNTED_TYPE(TCachedBlobReader)

////////////////////////////////////////////////////////////////////////////////

class TBlobReaderCache
    : public TSyncSlruCacheBase<TBlobReaderCacheKey, TCachedBlobReader>
    , public IBlobReaderCache
{
public:
    explicit TBlobReaderCache(IBootstrapBase* bootstrap)
        : TSyncSlruCacheBase(
            bootstrap->GetConfig()->DataNode->BlobReaderCache,
            DataNodeProfiler.WithPrefix("/block_reader_cache"))
        , Bootstrap_(bootstrap)
        , Config_(Bootstrap_->GetConfig()->DataNode)
    { }

    virtual TChunkFileReaderPtr GetReader(const TBlobChunkBasePtr& chunk) override
    {
        auto key = MakeReaderCacheKey(chunk.Get());
        if (auto reader = Find(key)) {
            return reader;
        }

        auto fileName = chunk->GetFileName();
        auto reader = New<TCachedBlobReader>(
            Bootstrap_->GetChunkMetaManager(),
            chunk,
            fileName,
            Config_->ValidateBlockChecksums);

        TCachedBlobReaderPtr existingReader;
        if (!TryInsert(reader, &existingReader)) {
            return existingReader;
        }

        return reader;
    }

    virtual void EvictReader(TBlobChunkBase* chunk) override
    {
        TSyncSlruCacheBase::TryRemove(MakeReaderCacheKey(chunk));
    }

private:
    IBootstrapBase* const Bootstrap_;
    const TDataNodeConfigPtr Config_;

    void OnDynamicConfigChanged(
        const NClusterNode::TClusterNodeDynamicConfigPtr& /* oldNodeConfig */,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        const auto& config = newNodeConfig->DataNode;
        TSyncSlruCacheBase::Reconfigure(config->BlobReaderCache);
    }
};

////////////////////////////////////////////////////////////////////////////////

IBlobReaderCachePtr CreateBlobReaderCache(IBootstrapBase* bootstrap)
{
    return New<TBlobReaderCache>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
