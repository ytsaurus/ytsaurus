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
    TCellId GetCellId() const override;
    const TString& GetTabletCellBundleName() const override;
    NHydra::EPeerState GetAutomatonState() const override;
    IInvokerPtr GetControlInvoker() const override;
    IInvokerPtr GetEpochAutomatonInvoker() const override;
    int GetAutomatonTerm() const override;
    NQueryClient::IColumnEvaluatorCachePtr GetColumnEvaluatorCache() const override;
    NQueryClient::IRowComparerProviderPtr GetRowComparerProvider() const override;
    NObjectClient::TObjectId GenerateId(NObjectClient::EObjectType type) const override;
    NApi::NNative::IClientPtr GetClient() const override;
    NClusterNode::TClusterNodeDynamicConfigManagerPtr GetDynamicConfigManager() const override;
    IStorePtr CreateStore(
        TTablet* tablet,
        EStoreType type,
        TStoreId storeId,
        const NTabletNode::NProto::TAddStoreDescriptor* descriptor) const override;
    THunkChunkPtr CreateHunkChunk(
        TTablet* tablet,
        NChunkClient::TChunkId chunkId,
        const NTabletNode::NProto::TAddHunkChunkDescriptor* descriptor) const override;
    TTransactionManagerPtr GetTransactionManager() const override;
    NRpc::IServerPtr GetLocalRpcServer() const override;
    NNodeTrackerClient::TNodeDescriptor GetLocalDescriptor() const override;
    INodeMemoryTrackerPtr GetMemoryUsageTracker() const override;
    NChunkClient::IChunkReplicaCachePtr GetChunkReplicaCache() const override;
    TString GetLocalHostName() const override;
    IHedgingManagerRegistryPtr GetHedgingManagerRegistry() const override;
    ITabletWriteManagerHostPtr GetTabletWriteManagerHost() const override;
    TMockBackendChunkReadersHolderPtr GetBackendChunkReadersHolder() const;

private:
    ITabletWriteManagerHost* TabletWriteManagerHost_ = nullptr;

    const NQueryClient::IColumnEvaluatorCachePtr ColumnEvaluatorCache_ =
        NQueryClient::CreateColumnEvaluatorCache(New<NQueryClient::TColumnEvaluatorCacheConfig>());

    const NQueryClient::IRowComparerProviderPtr RowComparerProvider_ =
        NQueryClient::CreateRowComparerProvider(New<TSlruCacheConfig>());

    const TMockBackendChunkReadersHolderPtr BackendChunkReadersHolder_ = New<TMockBackendChunkReadersHolder>();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
