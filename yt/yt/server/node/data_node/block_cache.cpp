#include "block_cache.h"
#include "private.h"
#include "chunk_block_manager.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/ytalloc/memory_zone.h>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

class TDataNodeBlockCache
    : public IClientBlockCache
{
public:
    explicit TDataNodeBlockCache(NClusterNode::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(Bootstrap_->GetConfig()->DataNode->BlockCache)
        , UnderlyingCache_(CreateClientBlockCache(
            Config_,
            EBlockType::UncompressedData,
            Bootstrap_
                ->GetMemoryUsageTracker()
                ->WithCategory(EMemoryCategory::BlockCache),
            DataNodeProfiler.WithPrefix("/block_cache")))
    { }

    virtual void Put(
        const TBlockId& id,
        EBlockType type,
        const TBlock& data,
        const std::optional<TNodeDescriptor>& source) override
    {
        if (type == EBlockType::CompressedData) {
            const auto& chunkBlockManager = Bootstrap_->GetChunkBlockManager();
            chunkBlockManager->PutCachedBlock(id, data, source);
        } else {
            UnderlyingCache_->Put(id, type, data, source);
        }
    }

    virtual TBlock Find(
        const TBlockId& id,
        EBlockType type) override
    {
        if (type == EBlockType::CompressedData) {
            const auto& chunkBlockManager = Bootstrap_->GetChunkBlockManager();
            if (auto cachedBlock = chunkBlockManager->FindCachedBlock(id)) {
                auto block = cachedBlock->GetData();
                block.BlockOrigin = EBlockOrigin::Cache;
                return block;
            } else {
                return TBlock();
            }
        } else {
            return UnderlyingCache_->Find(id, type);
        }
    }

    virtual EBlockType GetSupportedBlockTypes() const override
    {
        return EBlockType::CompressedData | EBlockType::UncompressedData;
    }

    virtual void Reconfigure(const TBlockCacheDynamicConfigPtr& config) override
    {
        UnderlyingCache_->Reconfigure(config);
    }

private:
    NClusterNode::TBootstrap* const Bootstrap_;
    const TBlockCacheConfigPtr Config_;
    const IClientBlockCachePtr UnderlyingCache_;
};

IClientBlockCachePtr CreateDataNodeBlockCache(NClusterNode::TBootstrap* bootstrap)
{
    return New<TDataNodeBlockCache>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
