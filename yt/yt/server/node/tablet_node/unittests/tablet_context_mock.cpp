#include "tablet_context_mock.h"
#include "sorted_store_helpers.h"

#include <yt/yt/server/node/tablet_node/ordered_dynamic_store.h>
#include <yt/yt/server/node/tablet_node/sorted_dynamic_store.h>
#include <yt/yt/server/node/tablet_node/sorted_chunk_store.h>
#include <yt/yt/server/node/tablet_node/versioned_chunk_meta_manager.h>

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

////////////////////////////////////////////////////////////////////////////////

TMockBackendChunkReadersHolderPtr TTabletContextMock::GetBackendChunkReadersHolder() const
{
    return BackendChunkReadersHolder_;
}

TCellId TTabletContextMock::GetCellId()
{
    return NullCellId;
}

const TString& TTabletContextMock::GetTabletCellBundleName()
{
    const static TString TabletCellBundleName;
    return TabletCellBundleName;
}

EPeerState TTabletContextMock::GetAutomatonState()
{
    return EPeerState::Leading;
}

IColumnEvaluatorCachePtr TTabletContextMock::GetColumnEvaluatorCache()
{
    return ColumnEvaluatorCache_;
}

NTabletClient::IRowComparerProviderPtr TTabletContextMock::GetRowComparerProvider()
{
    return RowComparerProvider_;
}

TObjectId TTabletContextMock::GenerateId(EObjectType /*type*/)
{
    return TObjectId::Create();
}

IStorePtr TTabletContextMock::CreateStore(
    TTablet* tablet,
    EStoreType type,
    TStoreId storeId,
    const NTabletNode::NProto::TAddStoreDescriptor* descriptor)
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
    const NTabletNode::NProto::TAddHunkChunkDescriptor* /*descriptor*/)
{
    YT_ABORT();
}

TTransactionManagerPtr TTabletContextMock::GetTransactionManager()
{
    return nullptr;
}

IServerPtr TTabletContextMock::GetLocalRpcServer()
{
    return nullptr;
}

TNodeDescriptor TTabletContextMock::GetLocalDescriptor()
{
    return NNodeTrackerClient::NullNodeDescriptor();
}

INodeMemoryTrackerPtr TTabletContextMock::GetMemoryUsageTracker()
{
    return nullptr;
}

NChunkClient::IChunkReplicaCachePtr TTabletContextMock::GetChunkReplicaCache()
{
    return nullptr;
}

IHedgingManagerRegistryPtr TTabletContextMock::GetHedgingManagerRegistry()
{
    return nullptr;
}

TString TTabletContextMock::GetLocalHostName()
{
    return TString();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
