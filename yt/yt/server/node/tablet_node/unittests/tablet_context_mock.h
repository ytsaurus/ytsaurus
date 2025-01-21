#pragma once

#include "sorted_store_helpers.h"
#include "store_context_mock.h"

#include <yt/yt/server/node/tablet_node/tablet.h>

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
    IInvokerPtr GetAutomatonInvoker() const override;
    int GetAutomatonTerm() const override;
    NQueryClient::IColumnEvaluatorCachePtr GetColumnEvaluatorCache() const override;
    NQueryClient::IRowComparerProviderPtr GetRowComparerProvider() const override;
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
    ITransactionManagerPtr GetTransactionManager() const override;
    NRpc::IServerPtr GetLocalRpcServer() const override;
    NNodeTrackerClient::TNodeDescriptor GetLocalDescriptor() const override;
    INodeMemoryTrackerPtr GetNodeMemoryUsageTracker() const override;
    NChunkClient::IChunkReplicaCachePtr GetChunkReplicaCache() const override;
    std::string GetLocalHostName() const override;
    IHedgingManagerRegistryPtr GetHedgingManagerRegistry() const override;
    ITabletWriteManagerHostPtr GetTabletWriteManagerHost() const override;
    IVersionedChunkMetaManagerPtr GetVersionedChunkMetaManager() const override;
    TMockBackendChunkReadersHolderPtr GetBackendChunkReadersHolder() const;

private:
    ITabletWriteManagerHost* const TabletWriteManagerHost_ = nullptr;
    const IStoreContextPtr StoreContext_ = New<TStoreContextMock>();

    const NQueryClient::IRowComparerProviderPtr RowComparerProvider_ =
        NQueryClient::CreateRowComparerProvider(New<TSlruCacheConfig>());

    const TMockBackendChunkReadersHolderPtr BackendChunkReadersHolder_ =
        New<TMockBackendChunkReadersHolder>();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
