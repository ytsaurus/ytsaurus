#include "tablet_context_mock.h"

#include <yt/yt/server/node/tablet_node/sorted_dynamic_store.h>
#include <yt/yt/server/node/tablet_node/ordered_dynamic_store.h>

#include <yt/yt/server/lib/tablet_node/config.h>

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
    const NTabletNode::NProto::TAddStoreDescriptor* /*descriptor*/)
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

TNodeMemoryTrackerPtr TTabletContextMock::GetMemoryUsageTracker()
{
    return nullptr;
}

TString TTabletContextMock::GetLocalHostName()
{
    return TString();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
