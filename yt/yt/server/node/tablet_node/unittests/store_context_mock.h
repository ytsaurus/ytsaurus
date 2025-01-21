#pragma once

#include <yt/yt/server/node/tablet_node/store.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TStoreContextMock
    : public IStoreContext
{
public:
    TStoreContextMock();

    const NChunkClient::IBlockCachePtr& GetBlockCache() final;
    const IVersionedChunkMetaManagerPtr& GetVersionedChunkMetaManager() final;
    const NQueryClient::IColumnEvaluatorCachePtr& GetColumnEvaluatorCache() final;
    const TTabletManagerConfigPtr& GetTabletManagerConfig() final;

private:
    const NChunkClient::IBlockCachePtr BlockCache_;
    const IVersionedChunkMetaManagerPtr VersionedChunkMetaManager_;
    const NQueryClient::IColumnEvaluatorCachePtr ColumnEvaluatorCache_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
