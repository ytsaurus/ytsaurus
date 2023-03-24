#include "tablet_context_mock.h"
#include "sorted_store_helpers.h"

#include <yt/yt/server/node/tablet_node/ordered_dynamic_store.h>
#include <yt/yt/server/node/tablet_node/sorted_dynamic_store.h>
#include <yt/yt/server/node/tablet_node/sorted_chunk_store.h>
#include <yt/yt/server/node/tablet_node/versioned_chunk_meta_manager.h>

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/chunk_client/client_block_cache.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NClusterNode;
using namespace NNodeTrackerClient;
using namespace NRpc;
using namespace NTabletNode;
using namespace NHydra;
using namespace NQueryClient;
using namespace NCypressClient;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

TTabletContextMock::TTabletContextMock(ITabletWriteManagerHost* host)
    : TabletWriteManagerHost_(host)
{ }

TMockBackendChunkReadersHolderPtr TTabletContextMock::GetBackendChunkReadersHolder() const
{
    return BackendChunkReadersHolder_;
}

TCellId TTabletContextMock::GetCellId() const
{
    return NullCellId;
}

const TString& TTabletContextMock::GetTabletCellBundleName() const
{
    const static TString TabletCellBundleName;
    return TabletCellBundleName;
}

EPeerState TTabletContextMock::GetAutomatonState() const
{
    return EPeerState::Leading;
}

int TTabletContextMock::GetAutomatonTerm() const
{
    return 0;
}

IInvokerPtr TTabletContextMock::GetControlInvoker() const
{
    YT_ABORT();
}

IInvokerPtr TTabletContextMock::GetEpochAutomatonInvoker() const
{
    return GetCurrentInvoker();
}

IColumnEvaluatorCachePtr TTabletContextMock::GetColumnEvaluatorCache() const
{
    return ColumnEvaluatorCache_;
}

NTabletClient::IRowComparerProviderPtr TTabletContextMock::GetRowComparerProvider() const
{
    return RowComparerProvider_;
}

TObjectId TTabletContextMock::GenerateId(EObjectType /*type*/) const
{
    return TObjectId::Create();
}

NNative::IClientPtr TTabletContextMock::GetClient() const
{
    return nullptr;
}

TClusterNodeDynamicConfigManagerPtr TTabletContextMock::GetDynamicConfigManager() const
{
    auto config = New<TClusterNodeDynamicConfig>();
    return New<TClusterNodeDynamicConfigManager>(std::move(config));
}

IStorePtr TTabletContextMock::CreateStore(
    TTablet* tablet,
    EStoreType type,
    TStoreId storeId,
    const NTabletNode::NProto::TAddStoreDescriptor* descriptor) const
{
    switch (type) {
        case EStoreType::SortedDynamic:
            return New<TSortedDynamicStore>(
                New<TTabletManagerConfig>(),
                storeId,
                tablet);
        case EStoreType::OrderedDynamic:
            return New<TOrderedDynamicStore>(
                New<TTabletManagerConfig>(),
                storeId,
                tablet);
        case EStoreType::SortedChunk:
            return New<TSortedChunkStore>(
                New<TTabletManagerConfig>(),
                storeId,
                storeId,
                TLegacyReadRange{},
                /*overrideTimestamp*/ NullTimestamp,
                /*maxClipTimestamp*/ NullTimestamp,
                tablet,
                descriptor,
                CreateClientBlockCache(
                    New<TBlockCacheConfig>(),
                    EBlockType::UncompressedData, // | EBlockType::CompressedData,
                    GetNullMemoryUsageTracker()),
                CreateVersionedChunkMetaManager(
                    New<TSlruCacheConfig>(),
                    GetNullMemoryUsageTracker()),
                BackendChunkReadersHolder_);
        default:
            YT_ABORT();
    }
}

THunkChunkPtr TTabletContextMock::CreateHunkChunk(
    TTablet* /*tablet*/,
    TChunkId /*chunkId*/,
    const NTabletNode::NProto::TAddHunkChunkDescriptor* /*descriptor*/) const
{
    YT_ABORT();
}

TTransactionManagerPtr TTabletContextMock::GetTransactionManager() const
{
    return nullptr;
}

IServerPtr TTabletContextMock::GetLocalRpcServer() const
{
    return nullptr;
}

TNodeDescriptor TTabletContextMock::GetLocalDescriptor() const
{
    return NNodeTrackerClient::NullNodeDescriptor();
}

INodeMemoryTrackerPtr TTabletContextMock::GetMemoryUsageTracker() const
{
    return nullptr;
}

NChunkClient::IChunkReplicaCachePtr TTabletContextMock::GetChunkReplicaCache() const
{
    return nullptr;
}

IHedgingManagerRegistryPtr TTabletContextMock::GetHedgingManagerRegistry() const
{
    return nullptr;
}

TString TTabletContextMock::GetLocalHostName() const
{
    return TString();
}

ITabletWriteManagerHostPtr TTabletContextMock::GetTabletWriteManagerHost() const
{
    return MakeStrong(TabletWriteManagerHost_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
