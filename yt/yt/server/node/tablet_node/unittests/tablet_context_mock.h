#pragma once

#include "sorted_store_helpers.h"

#include <yt/yt/server/node/tablet_node/tablet.h>

#include <yt/yt/library/query/engine_api/config.h>
#include <yt/yt/library/query/engine_api/column_evaluator.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTabletContextMock
    : public ITabletContext
{
public:
    TTabletContextMock() = default;
    explicit TTabletContextMock(ITabletWriteManagerHost* host);

    // ITabletContext implementation.
    TCellId GetCellId() override;
    const TString& GetTabletCellBundleName() override;
    NHydra::EPeerState GetAutomatonState() override;
    IInvokerPtr GetControlInvoker() override;
    NQueryClient::IColumnEvaluatorCachePtr GetColumnEvaluatorCache() override;
    NTabletClient::IRowComparerProviderPtr GetRowComparerProvider() override;
    NObjectClient::TObjectId GenerateId(NObjectClient::EObjectType type) override;
    IStorePtr CreateStore(
        TTablet* tablet,
        EStoreType type,
        TStoreId storeId,
        const NTabletNode::NProto::TAddStoreDescriptor* descriptor) override;
    THunkChunkPtr CreateHunkChunk(
        TTablet* tablet,
        NChunkClient::TChunkId chunkId,
        const NTabletNode::NProto::TAddHunkChunkDescriptor* descriptor) override;
    TTransactionManagerPtr GetTransactionManager() override;
    NRpc::IServerPtr GetLocalRpcServer() override;
    NNodeTrackerClient::TNodeDescriptor GetLocalDescriptor() override;
    INodeMemoryTrackerPtr GetMemoryUsageTracker() override;
    NChunkClient::IChunkReplicaCachePtr GetChunkReplicaCache() override;
    TString GetLocalHostName() override;
    IHedgingManagerRegistryPtr GetHedgingManagerRegistry() override;
    ITabletWriteManagerHostPtr GetTabletWriteManagerHost() override;
    TMockBackendChunkReadersHolderPtr GetBackendChunkReadersHolder() const;

private:
    ITabletWriteManagerHost* TabletWriteManagerHost_ = nullptr;

    const NQueryClient::IColumnEvaluatorCachePtr ColumnEvaluatorCache_ =
        NQueryClient::CreateColumnEvaluatorCache(New<NQueryClient::TColumnEvaluatorCacheConfig>());

    const NTabletClient::IRowComparerProviderPtr RowComparerProvider_ =
        NTabletClient::CreateRowComparerProvider(New<TSlruCacheConfig>());

    const TMockBackendChunkReadersHolderPtr BackendChunkReadersHolder_ = New<TMockBackendChunkReadersHolder>();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
