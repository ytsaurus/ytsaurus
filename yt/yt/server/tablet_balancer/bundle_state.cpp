#include "bundle_state.h"
#include "private.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/server/lib/tablet_balancer/config.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

namespace NYT::NTabletBalancer {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NYson;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletBalancerLogger;

////////////////////////////////////////////////////////////////////////////////

TTableMutableInfo::TTableMutableInfo(
    TTableTabletBalancerConfigPtr config,
    bool isDynamic)
    : TableConfig(std::move(config))
    , Dynamic(isDynamic)
{ }

void Deserialize(TTableMutableInfo::TTabletInfo& value, NYTree::INodePtr node)
{
    auto mapNode = node->AsMap();

    value.TabletId = ConvertTo<TTabletId>(mapNode->FindChild("tablet_id"));
    value.TabletIndex = mapNode->FindChild("index")->AsInt64()->GetValue();

    // TODO(alexelex): just a few of the necessary arguments, needs to be improved
    auto statistics = mapNode->FindChild("statistics")->AsMap();
    value.Statistics.CompressedDataSize = statistics->FindChild("compressed_data_size")->AsInt64()->GetValue();
    value.Statistics.UncompressedDataSize = statistics->FindChild("uncompressed_data_size")->AsInt64()->GetValue();
}

////////////////////////////////////////////////////////////////////////////////

TBundleState::TBundleState(
    TString name,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker)
    : Name_(name)
    , Client_(client)
    , Invoker_(invoker)
{ }

void TBundleState::UpdateBundleAttributes(const IAttributeDictionary* attributes)
{
    Health_ = attributes->Get<ETabletCellHealth>("health");
    Config_ = attributes->Get<TBundleTabletBalancerConfigPtr>("tablet_balancer_config");
    CellIds_ = attributes->Get<std::vector<TTabletCellId>>("tablet_cell_ids");
}

bool TBundleState::IsBalancingAllowed() const
{
    return Health_ == ETabletCellHealth::Good &&
        Config_ &&
        Config_->EnableTabletSizeBalancer;
}

TTableMutableInfoPtr TBundleState::GetTableMutableInfo(const TTableId& tableId) const
{
    return GetOrCrash(TableMutableInfo_, tableId);
}

const THashMap<TTableId, TBundleState::TImmutableTableInfo>& TBundleState::GetTableImmutableInfo() const
{
    return TableImmutableInfo_;
}

const THashMap<TTabletId, TBundleState::TImmutableTabletInfo>& TBundleState::GetTabletImmutableInfo() const
{
    return TabletImmutableInfo_;
}

TFuture<void> TBundleState::UpdateMetaRegistry()
{
    return BIND(&TBundleState::DoUpdateMetaRegistry, MakeStrong(this))
        .AsyncVia(Invoker_)
        .Run();
}

TFuture<void> TBundleState::FetchTableMutableInfo()
{
    return BIND(&TBundleState::DoFetchTableMutableInfo, MakeStrong(this))
        .AsyncVia(Invoker_)
        .Run();
}

void TBundleState::ExecuteBatchRequests(THashMap<TCellTag, TCellTagBatch>* batchReqs) const
{
    std::vector<TFuture<void>> futures;
    for (auto& [cellTag, obj] : *batchReqs) {
        obj.Response = obj.Request->Invoke();
        futures.push_back(obj.Response.AsVoid());
    }

    WaitFor(AllSucceeded(std::move(futures)))
        .ThrowOnError();
}

THashMap<TObjectId, IAttributeDictionaryPtr> TBundleState::FetchAttributes(
    const THashSet<TObjectId>& objectIds,
    const std::vector<TString>& attributeKeys,
    bool useExternalCellTagForTables) const
{
    // TODO(alexelex): Receive list of error codes to skip them.

    THashMap<TCellTag, TCellTagBatch> batchReqs;
    for (auto objectId : objectIds) {
        auto cellTag = CellTagFromId(objectId);
        if (useExternalCellTagForTables) {
            YT_VERIFY(IsTableType(TypeFromId(objectId)));
            cellTag = GetOrCrash(TableImmutableInfo_, objectId).ExternalCellTag;
        }

        auto req = TTableYPathProxy::Get(FromObjectId(objectId) + "/@");
        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);

        TObjectServiceProxy proxy(Client_->GetMasterChannelOrThrow(EMasterChannelKind::Follower, cellTag));
        auto it = batchReqs.emplace(cellTag, TCellTagBatch{proxy.ExecuteBatch(), {}}).first;
        it->second.Request->AddRequest(req, ToString(objectId));
    }

    ExecuteBatchRequests(&batchReqs);

    THashMap<TObjectId, IAttributeDictionaryPtr> responses;
    for (auto objectId : objectIds) {
        auto cellTag = CellTagFromId(objectId);
        if (useExternalCellTagForTables) {
            cellTag = GetOrCrash(TableImmutableInfo_, objectId).ExternalCellTag;
        }

        const auto& batchReq = batchReqs[cellTag].Response.Get().Value();
        auto rspOrError = batchReq->GetResponse<TTableYPathProxy::TRspGet>(ToString(objectId));
        if (!rspOrError.IsOK() && rspOrError.GetCode() == NYTree::EErrorCode::ResolveError) {
            continue;
        }

        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);
        auto attributes = ConvertToAttributes(TYsonString(rspOrError.Value()->value()));
        EmplaceOrCrash(responses, objectId, std::move(attributes));
    }

    return responses;
}

void TBundleState::DoUpdateMetaRegistry()
{
    YT_LOG_DEBUG("Started fetching tablet ids");
    auto tabletIds = FetchTabletIds();
    YT_LOG_DEBUG("Finished fetching tablet ids");

    DropMissingKeys(&TabletImmutableInfo_, tabletIds);

    THashSet<TTabletId> newTabletIds;
    for (auto tabletId : tabletIds) {
        if (!TabletImmutableInfo_.contains(tabletId)) {
            InsertOrCrash(newTabletIds, tabletId);
        }
    }

    YT_LOG_DEBUG("Started fetching tablet infos");
    auto tabletInfos = FetchTabletInfos(newTabletIds);
    YT_LOG_DEBUG("Finished fetching tablet infos");

    THashSet<TTableId> newTableIds;
    for (auto& [tabletId, tabletInfo] : tabletInfos) {
        EmplaceOrCrash(TabletImmutableInfo_, tabletId, std::move(tabletInfo));
        if (!TableImmutableInfo_.contains(tabletInfo.tableId)) {
            newTableIds.insert(tabletInfo.tableId);
        }
    }

    YT_LOG_DEBUG("Started fetching table infos");
    auto tableInfos = FetchTableInfos(newTableIds);
    YT_LOG_DEBUG("Finished fetching table infos");

    for (auto& [tableId, tableInfo] : tableInfos) {
        EmplaceOrCrash(TableImmutableInfo_, tableId, std::move(tableInfo));
    }
}

bool TBundleState::IsTableBalancingAllowed(const TTableMutableInfoPtr& tableInfo) const
{
    // TODO(alexelex): EnableAutoTabletMove is just an example.
    return tableInfo->Dynamic && tableInfo->TableConfig->EnableAutoTabletMove;
}

void TBundleState::DoFetchTableMutableInfo()
{
    TableMutableInfo_.clear();
    YT_LOG_DEBUG("Started fetching actual table configs");
    auto tableInfos = FetchActualTableConfigs();
    YT_LOG_DEBUG("Started fetching actual table configs");

    THashSet<TTableId> tableIds;
    for (const auto& [tableId, tableInfo] : tableInfos) {
        EmplaceOrCrash(tableIds, tableId);
    }
    DropMissingKeys(&TableImmutableInfo_, tableIds);

    THashSet<TTableId> tableIdsToFetch;
    for (auto& [tableId, tableInfo] : tableInfos) {
        if (IsTableBalancingAllowed(tableInfo)) {
            InsertOrCrash(tableIdsToFetch, tableId);
        }
        EmplaceOrCrash(TableMutableInfo_, tableId, std::move(tableInfo));
    }

    YT_LOG_DEBUG("Started fetching tablets");
    auto tableIdToTablets = FetchTablets(tableIdsToFetch);
    YT_LOG_DEBUG("Finished fetching tablets");

    THashSet<TTableId> missingTables;
    for (const auto& [tableId, tablets] : tableIdToTablets) {
        EmplaceOrCrash(missingTables, tableId);
    }

    for (auto& [tableId, tablets] : tableIdToTablets) {
        auto& tableInfo = GetOrCrash(TableMutableInfo_, tableId);
        tableInfo->Tablets = std::move(tablets);
        EraseOrCrash(missingTables, tableId);
    }

    for (auto tableId : missingTables) {
        EraseOrCrash(TableMutableInfo_, tableId);
        EraseOrCrash(TableImmutableInfo_, tableId);
    }
}

THashSet<TTabletId> TBundleState::FetchTabletIds() const
{
    TObjectServiceProxy proxy(
        Client_->GetMasterChannelOrThrow(EMasterChannelKind::Follower));
    auto batchReq = proxy.ExecuteBatch();

    for (const auto& cellId : CellIds_) {
        auto req = TYPathProxy::Get(FromObjectId(cellId) + "/@tablet_ids");
        batchReq->AddRequest(req);
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
    const auto& batchRsp = batchRspOrError.Value();

    THashSet<TTabletId> tabletIds;
    for (const auto& rspOrError : batchRsp->GetResponses<TYPathProxy::TRspGet>()) {
        auto fetchedTabletIds = ConvertTo<std::vector<TTabletId>>(TYsonString(rspOrError.Value()->value()));
        tabletIds.insert(fetchedTabletIds.begin(), fetchedTabletIds.end());
    }

    return tabletIds;
}

THashMap<TTabletId, TBundleState::TImmutableTabletInfo> TBundleState::FetchTabletInfos(
    const THashSet<TTabletId>& tabletIds) const
{
    static const std::vector<TString> attributeKeys{"table_id"};
    auto tableToAttributes = FetchAttributes(tabletIds, attributeKeys);

    THashMap<TTableId, TImmutableTabletInfo> tabletInfos;
    for (const auto& [tabletId, attributes] : tableToAttributes) {
        auto tableId = attributes->Get<TTableId>("table_id");
        EmplaceOrCrash(tabletInfos, tabletId, TImmutableTabletInfo{
            .tableId = tableId
        });
    }

    return tabletInfos;
}

THashMap<TTableId, TBundleState::TImmutableTableInfo> TBundleState::FetchTableInfos(
    const THashSet<TTableId>& tableIds) const
{
    static const std::vector<TString> attributeKeys{"path", "external", "sorted", "external_cell_tag"};
    auto tableToAttributes = FetchAttributes(tableIds, attributeKeys);

    THashMap<TTableId, TImmutableTableInfo> tableInfos;
    for (const auto& [tableId, attributes] : tableToAttributes) {
        auto cellTag = CellTagFromId(tableId);

        auto tablePath = attributes->Get<TYPath>("path");
        auto isSorted = attributes->Get<bool>("sorted");
        auto external = attributes->Get<bool>("external");
        if (external) {
            cellTag = attributes->Get<TCellTag>("external_cell_tag");
        }

        EmplaceOrCrash(tableInfos, tableId, TImmutableTableInfo{
            .Sorted = isSorted,
            .Path = tablePath,
            .ExternalCellTag = cellTag
        });
    }

    return tableInfos;
}

THashMap<TTableId, TTableMutableInfoPtr> TBundleState::FetchActualTableConfigs() const
{
    THashSet<TTableId> tableIds;
    for (const auto& [tableId, tableInfo] : TableImmutableInfo_) {
        InsertOrCrash(tableIds, tableId);
    }

    static const std::vector<TString> attributeKeys{"tablet_balancer_config", "dynamic"};
    auto tableToAttributes = FetchAttributes(
        tableIds,
        attributeKeys,
        /*useExternalCellTagForTables*/ true);

    THashMap<TTableId, TTableMutableInfoPtr> tableInfos;
    for (const auto& [tableId, attributes] : tableToAttributes) {
        auto tabletBalancerConfig = attributes->Get<TTableTabletBalancerConfigPtr>("tablet_balancer_config");
        auto dynamic = attributes->Get<bool>("dynamic");

        EmplaceOrCrash(tableInfos, tableId, New<TTableMutableInfo>(tabletBalancerConfig, dynamic));
    }

    return tableInfos;
}

THashMap<TTableId, std::vector<TTableMutableInfo::TTabletInfo>> TBundleState::FetchTablets(
    const THashSet<TTableId>& tableIds) const
{
    static const std::vector<TString> attributeKeys{"tablets"};
    auto tableToAttributes = FetchAttributes(
        tableIds,
        attributeKeys,
        /*useExternalCellTagForTables*/ true);

    THashMap<TTableId, std::vector<TTableMutableInfo::TTabletInfo>> tableInfos;
    for (const auto& [tableId, attributes] : tableToAttributes) {
        // What if between fetch dynamic and this request the table becomes dynamic?
        // TODO(alexelex): Check the error code and skip this request as well.

        auto tablets = attributes->Get<std::vector<TTableMutableInfo::TTabletInfo>>("tablets");
        EmplaceOrCrash(tableInfos, tableId, std::move(tablets));
    }

    return tableInfos;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
