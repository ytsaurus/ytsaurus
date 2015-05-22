#include "stdafx.h"
#include "block_cache.h"
#include "block_store.h"
#include "private.h"

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/client_block_cache.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <server/misc/memory_usage_tracker.h>

#include <server/cell_node/config.h>
#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

class TServerBlockCache
    : public IBlockCache
{
public:
    TServerBlockCache(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , UnderlyingCache_(CreateClientBlockCache(
            Config_->BlockCache,
            EBlockType::UncompressedData,
            NProfiling::TProfiler(DataNodeProfiler.GetPathPrefix() + "/block_cache")))
    { }

    void Initialize()
    {
        auto result = Bootstrap_->GetMemoryUsageTracker()->TryAcquire(
            NCellNode::EMemoryCategory::BlockCache,
            Config_->BlockCache->GetTotalCapacity());
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error reserving memory for block cache");
    }

    virtual void Put(
        const TBlockId& id,
        EBlockType type,
        const TSharedRef& data,
        const TNullable<TNodeDescriptor>& source) override
    {
        if (type == EBlockType::CompressedData) {
            auto blockStore = Bootstrap_->GetBlockStore();
            blockStore->PutCachedBlock(id, data, source);
        } else {
            UnderlyingCache_->Put(id, type, data, source);
        }
    }

    virtual TSharedRef Find(
        const TBlockId& id,
        EBlockType type) override
    {
        if (type == EBlockType::CompressedData) {
            auto blockStore = Bootstrap_->GetBlockStore();
            auto cachedBlock = blockStore->FindCachedBlock(id);
            return cachedBlock ? cachedBlock->GetData() : TSharedRef();
        } else {
            return UnderlyingCache_->Find(id, type);
        }
    }

    virtual EBlockType GetSupportedBlockTypes() const override
    {
        return EBlockType::CompressedData | EBlockType::UncompressedData;
    }

private:
    const TDataNodeConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    const IBlockCachePtr UnderlyingCache_;

};

IBlockCachePtr CreateServerBlockCache(
    TDataNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
{
    auto blockCache = New<TServerBlockCache>(config, bootstrap);
    blockCache->Initialize();
    return blockCache;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
