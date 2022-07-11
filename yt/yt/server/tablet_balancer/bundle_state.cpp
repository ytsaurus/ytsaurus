#include "bundle_state.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/server/lib/tablet_balancer/config.h>
#include <yt/yt/server/lib/tablet_balancer/table.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

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

TBundleState::TBundleState(
    TString name,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker)
    : Bundle_(New<TTabletCellBundle>(name))
    , Logger(TabletBalancerLogger.WithTag("BundleName: %v", name))
    , Client_(client)
    , Invoker_(invoker)
{ }

void TBundleState::UpdateBundleAttributes(const IAttributeDictionary* attributes)
{
    Health_ = attributes->Get<ETabletCellHealth>("health");
    Bundle_->Config = attributes->Get<TBundleTabletBalancerConfigPtr>("tablet_balancer_config");
    CellIds_ = attributes->Get<std::vector<TTabletCellId>>("tablet_cell_ids");
}

TFuture<void> TBundleState::UpdateState()
{
    return BIND(&TBundleState::DoUpdateState, MakeStrong(this))
        .AsyncVia(Invoker_)
        .Run();
}

TFuture<void> TBundleState::FetchStatistics()
{
    return BIND(&TBundleState::DoFetchStatistics, MakeStrong(this))
        .AsyncVia(Invoker_)
        .Run();
}

void TBundleState::DoUpdateState()
{
    YT_LOG_DEBUG("Started fetching tablet cells");
    auto tabletCells = FetchTabletCells();
    YT_LOG_DEBUG("Finished fetching tablet cells");

    THashSet<TTabletId> tabletIds;
    for (const auto& [id, info] : tabletCells) {
        tabletIds.insert(info.TabletIds.begin(), info.TabletIds.end());
    }

    DropMissingKeys(&Tablets_, tabletIds);

    THashSet<TTabletId> newTabletIds;
    for (auto tabletId : tabletIds) {
        if (!Tablets_.contains(tabletId)) {
            InsertOrCrash(newTabletIds, tabletId);
        }
    }

    YT_LOG_DEBUG("Started fetching tablet table ids");
    auto tabletInfos = FetchTabletTableIds(newTabletIds);
    YT_LOG_DEBUG("Finished fetching tablet table ids");

    THashSet<TTableId> newTableIds;
    THashMap<TTableId, std::vector<TTabletId>> newTableIdToTablets;
    for (const auto& [tabletId, tableId] : tabletInfos) {
        if (auto tableIt = Bundle_->Tables.find(tableId); tableIt != Bundle_->Tables.end()) {
            EmplaceOrCrash(Tablets_, tabletId, New<TTablet>(tabletId, tableIt->second.Get()));
        } else {
            // A new table has been found.
            newTableIds.insert(tableId);
            auto it = newTableIdToTablets.emplace(tableId, std::vector<TTabletId>{}).first;
            it->second.emplace_back(tabletId);
        }
    }

    YT_LOG_DEBUG("Started fetching basic table attributes");
    auto tableInfos = FetchBasicTableAttributes(newTableIds);
    YT_LOG_DEBUG("Finished fetching basic table attributes");

    for (auto& [tableId, tableInfo] : tableInfos) {
        auto it = EmplaceOrCrash(Bundle_->Tables, tableId, std::move(tableInfo));

        auto tablets = GetOrCrash(newTableIdToTablets, tableId);
        for (auto tabletId : tablets) {
            auto tablet = New<TTablet>(tabletId, it->second.Get());
            EmplaceOrCrash(Tablets_, tabletId, tablet);
        }
    }

    Bundle_->TabletCells.clear();
    for (const auto& [cellId, tabletCellInfo] : tabletCells) {
        EmplaceOrCrash(Bundle_->TabletCells, cellId, tabletCellInfo.TabletCell);

        for (auto tabletId : tabletCellInfo.TabletIds) {
            if (!Tablets_.contains(tabletId)) {
                // Tablet was created (and found in FetchCells).
                // After that it was quickly removed before we fetched TabletTableIds
                // or it was quickly removed before we fetched BasicTableAttributes.
                // Therefore a TTablet object was not created.
                // Skip this tablet and verify that this tabletInfo was never fetched
                // in the first case and tableInfo was never fetched in the second.

                YT_VERIFY(!tabletInfos.contains(tabletId) || !tableInfos.contains(tabletInfos[tabletId]));
            }
        }
    }
}

bool TBundleState::IsTableBalancingAllowed(const TTableSettings& table) const
{
    return table.Dynamic &&
        (table.Config->EnableAutoTabletMove || table.Config->EnableAutoReshard);
}

void TBundleState::DoFetchStatistics()
{
    YT_LOG_DEBUG("Started fetching actual table settings");
    auto tableSettings = FetchActualTableSettings();
    YT_LOG_DEBUG("Finished fetching actual table settings");

    THashSet<TTableId> tableIds;
    for (const auto& [id, info] : tableSettings) {
        EmplaceOrCrash(tableIds, id);
    }
    DropMissingKeys(&Bundle_->Tables, tableIds);

    THashSet<TTableId> tableIdsToFetch;
    for (auto& [tableId, tableSettings] : tableSettings) {
        if (IsTableBalancingAllowed(tableSettings)) {
            InsertOrCrash(tableIdsToFetch, tableId);
        }

        const auto& table = GetOrCrash(Bundle_->Tables, tableId);

        table->Dynamic = tableSettings.Dynamic;
        table->TableConfig = tableSettings.Config;
        table->InMemoryMode = tableSettings.InMemoryMode;

        // Remove all tablets and write again (with statistics and other parameters).
        // This allows you to overwrite indexes correctly (Tablets[index].Index == index) and remove old tablets.
        // This must be done here because some tables may be removed before fetching @tablets attribute.
        table->Tablets.clear();
    }

    YT_LOG_DEBUG("Started fetching table statistics");
    auto tableIdToStatistics = FetchTableStatistics(tableIdsToFetch);
    YT_LOG_DEBUG("Finished fetching table statistics");

    THashSet<TTableId> missingTables;
    for (const auto& [tableId, statistics] : tableIdToStatistics) {
        EmplaceOrCrash(missingTables, tableId);
    }

    for (const auto& [id, cell] : Bundle_->TabletCells) {
        // Not filled yet.
        YT_VERIFY(cell->Tablets.empty());
    }

    for (auto& [tableId, statistics] : tableIdToStatistics) {
        auto& table = GetOrCrash(Bundle_->Tables, tableId);
        table->CompressedDataSize = statistics.CompressedDataSize;
        table->UncompressedDataSize = statistics.UncompressedDataSize;
        table->DataWeight = statistics.DataWeight;

        for (auto& tabletResponse : statistics.Tablets) {
            TTabletPtr tablet;

            if (auto it = Tablets_.find(tabletResponse.TabletId); it != Tablets_.end()) {
                tablet = it->second;
            } else {
                // Tablet is not mounted or it's a new tablet.

                tablet = New<TTablet>(tabletResponse.TabletId, table.Get());
                EmplaceOrCrash(Tablets_, tabletResponse.TabletId, tablet);
            }

            if (tabletResponse.CellId) {
                // Will fail if this is a new cell created since the last bundle/@tablet_cell_ids request.
                // Or if the table has been moved from one bundle to another.
                // In this case, it's okay to skip one iteration.

                auto cellIt = Bundle_->TabletCells.find(*tabletResponse.CellId);
                if (cellIt == Bundle_->TabletCells.end()) {
                    THROW_ERROR_EXCEPTION(
                        "Tablet %v of table %v belongs to an unknown cell %v",
                        *tabletResponse.CellId,
                        table->Id,
                        tabletResponse.TabletId);
                }
                cellIt->second->Tablets.push_back(tablet);
                tablet->Cell = cellIt->second.Get();
            } else {
                YT_VERIFY(tabletResponse.State == ETabletState::Unmounted);
                tablet->Cell = nullptr;
            }

            tablet->Index = tabletResponse.Index;
            tablet->Statistics = tabletResponse.Statistics;
            tablet->State = tabletResponse.State;

            table->Tablets.push_back(tablet);
        }
        EraseOrCrash(missingTables, tableId);
    }

    for (auto tableId : missingTables) {
        EraseOrCrash(Bundle_->Tables, tableId);
    }
}

THashMap<TTabletCellId, TBundleState::TTabletCellInfo> TBundleState::FetchTabletCells() const
{
    TObjectServiceProxy proxy(
        Client_->GetMasterChannelOrThrow(EMasterChannelKind::Follower));
    auto batchReq = proxy.ExecuteBatch();
    static const std::vector<TString> attributeKeys{"tablet_ids", "status", "total_statistics"};

    for (auto cellId : CellIds_) {
        auto req = TTableYPathProxy::Get(FromObjectId(cellId) + "/@");
        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
        batchReq->AddRequest(req, ToString(cellId));
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
    const auto& batchRsp = batchRspOrError.Value();

    THashMap<TTabletCellId, TTabletCellInfo> tabletCells;
    for (auto cellId : CellIds_) {
        auto rspOrError = batchRsp->GetResponse<TTableYPathProxy::TRspGet>(ToString(cellId));
        auto attributes = ConvertToAttributes(TYsonString(rspOrError.Value()->value()));

        auto tabletIds = attributes->Get<std::vector<TTabletId>>("tablet_ids");
        auto status = attributes->Get<TTabletCellStatus>("status");
        auto statistics = attributes->Get<TTabletCellStatistics>("total_statistics");
        auto tabletCell = New<TTabletCell>(cellId, statistics, status);

        tabletCells.emplace(cellId, TTabletCellInfo{
            .TabletCell = std::move(tabletCell),
            .TabletIds = tabletIds
        });
    }

    return tabletCells;
}

THashMap<TTabletId, TTableId> TBundleState::FetchTabletTableIds(
    const THashSet<TTabletId>& tabletIds) const
{
    static const std::vector<TString> attributeKeys{"table_id"};
    auto tableToAttributes = FetchAttributes(Client_, tabletIds, attributeKeys);

    THashMap<TTableId, TTableId> tabletInfos;
    for (const auto& [tabletId, attributes] : tableToAttributes) {
        auto tableId = attributes->Get<TTableId>("table_id");
        EmplaceOrCrash(tabletInfos, tabletId, tableId);
    }

    return tabletInfos;
}

THashMap<TTableId, TTablePtr> TBundleState::FetchBasicTableAttributes(
    const THashSet<TTableId>& tableIds) const
{
    static const std::vector<TString> attributeKeys{"path", "external", "sorted", "external_cell_tag"};
    auto tableToAttributes = FetchAttributes(Client_, tableIds, attributeKeys);

    THashMap<TTableId, TTablePtr> tableInfos;
    for (const auto& [tableId, attributes] : tableToAttributes) {
        auto cellTag = CellTagFromId(tableId);

        auto tablePath = attributes->Get<TYPath>("path");
        auto isSorted = attributes->Get<bool>("sorted");
        auto external = attributes->Get<bool>("external");
        if (external) {
            cellTag = attributes->Get<TCellTag>("external_cell_tag");
        }

        EmplaceOrCrash(tableInfos, tableId, New<TTable>(
            isSorted,
            tablePath,
            cellTag,
            tableId,
            Bundle_.Get()
        ));
    }

    return tableInfos;
}

THashMap<TTableId, TBundleState::TTableSettings> TBundleState::FetchActualTableSettings() const
{
    THashSet<TTableId> tableIds;
    for (const auto& [id, table] : Bundle_->Tables) {
        InsertOrCrash(tableIds, id);
    }

    static const std::vector<TString> attributeKeys{
        "tablet_balancer_config",
        "dynamic",
        "in_memory_mode",
    };
    auto tableToAttributes = FetchTableAttributes(
        Client_,
        tableIds,
        attributeKeys,
        Bundle_->Tables);

    THashMap<TTableId, TTableSettings> tableConfigs;
    for (const auto& [tableId, attributes] : tableToAttributes) {
        auto tabletBalancerConfig = attributes->Get<TTableTabletBalancerConfigPtr>("tablet_balancer_config");
        auto dynamic = attributes->Get<bool>("dynamic");
        auto inMemoryMode = attributes->Get<EInMemoryMode>("in_memory_mode");

        EmplaceOrCrash(tableConfigs, tableId, TTableSettings{
            .Config = tabletBalancerConfig,
            .InMemoryMode = inMemoryMode,
            .Dynamic = dynamic,
        });
    }

    return tableConfigs;
}

THashMap<TTableId, TBundleState::TTableStatisticsResponse> TBundleState::FetchTableStatistics(
    const THashSet<TTableId>& tableIds) const
{
    static const std::vector<TString> attributeKeys{
        "tablets",
        "compressed_data_size",
        "uncompressed_data_size",
        "data_weight",
    };
    auto tableToAttributes = FetchTableAttributes(
        Client_,
        tableIds,
        attributeKeys,
        Bundle_->Tables);

    THashMap<TTableId, TTableStatisticsResponse> tableStatistics;
    for (const auto& [tableId, attributes] : tableToAttributes) {
        // What if between fetch dynamic and this request the table becomes dynamic?
        // TODO(alexelex): Check the error code and skip this request as well.

        auto tablets = attributes->Get<std::vector<TTableStatisticsResponse::TTabletResponse>>("tablets");
        auto compressedSize = attributes->Get<i64>("compressed_data_size");
        auto uncompressedSize = attributes->Get<i64>("uncompressed_data_size");
        auto dataWeight = attributes->Get<i64>("data_weight");
        EmplaceOrCrash(tableStatistics, tableId, TTableStatisticsResponse{
            .Tablets = std::move(tablets),
            .CompressedDataSize = compressedSize,
            .UncompressedDataSize = uncompressedSize,
            .DataWeight = dataWeight
        });
    }

    return tableStatistics;
}

void Deserialize(
    TBundleState::TTableStatisticsResponse::TTabletResponse& value,
    const NYTree::INodePtr& node)
{
    auto mapNode = node->AsMap();

    value.Index = ConvertTo<i64>(mapNode->FindChild("index"));
    value.TabletId = ConvertTo<TTabletId>(mapNode->FindChild("tablet_id"));

    value.Statistics = ConvertTo<TTabletStatistics>(mapNode->FindChild("statistics"));
    value.State = ConvertTo<ETabletState>(mapNode->FindChild("state"));

    if (auto cellId = mapNode->FindChild("cell_id")) {
        value.CellId = ConvertTo<TTabletCellId>(cellId);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
