#include "bundle_state.h"
#include "helpers.h"
#include "private.h"
#include "table_registry.h"

#include <yt/yt/server/lib/tablet_balancer/config.h>
#include <yt/yt/server/lib/tablet_balancer/table.h>
#include <yt/yt/server/lib/tablet_balancer/tablet.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/ytlib/tablet_client/master_tablet_service_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/query_client/query_builder.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>

#include <util/string/join.h>

namespace NYT::NTabletBalancer {

using namespace NApi;
using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletClient::NProto;
using namespace NTabletNode;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

THashMap<TTabletId, TTableId> ParseTabletToTableMapping(const NYTree::IMapNodePtr& mapNode)
{
    THashMap<TTabletId, TTableId> tabletToTable;
    for (const auto& [key, value] : mapNode->GetChildren()) {
        auto mapChildNode = value->AsMap();
        EmplaceOrCrash(
            tabletToTable,
            ConvertTo<TTabletId>(key),
            ConvertTo<TTableId>(mapChildNode->FindChild("table_id")));
    }
    return tabletToTable;
}

TTabletStatistics BuildTabletStatistics(
    const TRspGetTableBalancingAttributes::TTablet::TCompressedStatistics& protoStatistics,
    const std::vector<TString>& keys,
    bool saveOriginalNode = false)
{
    auto node = BuildYsonNodeFluently()
        .DoMap([&] (TFluentMap fluent) {
            auto iter = keys.begin();
            for (const auto& value : protoStatistics.i64_fields()) {
                fluent.Item(*iter).Value(value);
                ++iter;
            }
            for (const auto& value : protoStatistics.i32_fields()) {
                fluent.Item(*iter).Value(value);
                ++iter;
            }
    });

    TTabletStatistics statistics;
    if (saveOriginalNode) {
        statistics.OriginalNode = node;
    }

    auto mapNode = node->AsMap();
    statistics.CompressedDataSize = ConvertTo<i64>(mapNode->FindChild("compressed_data_size"));
    statistics.UncompressedDataSize = ConvertTo<i64>(mapNode->FindChild("uncompressed_data_size"));
    statistics.MemorySize = ConvertTo<i64>(mapNode->FindChild("memory_size"));
    statistics.PartitionCount = ConvertTo<int>(mapNode->FindChild("partition_count"));

    return statistics;
}

struct TTabletCellPeer
{
    TNodeAddress NodeAddress;
    EPeerState State = EPeerState::None;
};

void Deserialize(TTabletCellPeer& value, NYTree::INodePtr node)
{
    auto mapNode = node->AsMap();

    if (auto address = mapNode->FindChildValue<TNodeAddress>("address")) {
        value.NodeAddress = *address;
    }

    if (auto stateNode = mapNode->FindChildValue<EPeerState>("state")) {
        value.State = *stateNode;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

TBundleProfilingCounters::TBundleProfilingCounters(const NProfiling::TProfiler& profiler)
    : TabletCellTabletsRequestCount(profiler.WithSparse().Counter("/master_requests/tablet_cell_tablets_count"))
    , BasicTableAttributesRequestCount(profiler.WithSparse().Counter("/master_requests/basic_table_attributes_count"))
    , ActualTableSettingsRequestCount(profiler.WithSparse().Counter("/master_requests/actual_table_settings_count"))
    , TableStatisticsRequestCount(profiler.WithSparse().Counter("/master_requests/table_statistics_count"))
{ }

////////////////////////////////////////////////////////////////////////////////

const std::vector<TString> DefaultPerformanceCountersKeys{
    #define XX(name, Name) #name,
    ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
    #undef XX
};

const std::vector<TString> AdditionalPerformanceCountersKeys{
    #define XX(name, Name) #name,
    ITERATE_NODE_TABLET_PERFORMANCE_COUNTERS(XX)
    #undef XX
};

TBundleState::TBundleState(
    TString name,
    TTableRegistryPtr tableRegistry,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker)
    : Bundle_(New<TTabletCellBundle>(name))
    , Logger(TabletBalancerLogger().WithTag("BundleName: %v", name))
    , Profiler_(TabletBalancerProfiler().WithTag("tablet_cell_bundle", name))
    , Client_(client)
    , Invoker_(invoker)
    , TableRegistry_(std::move(tableRegistry))
    , Counters_(New<TBundleProfilingCounters>(Profiler_))
{ }

void TBundleState::UpdateBundleAttributes(
    const IAttributeDictionary* attributes)
{
    Health_ = attributes->Get<ETabletCellHealth>("health");
    CellIds_ = attributes->Get<std::vector<TTabletCellId>>("tablet_cell_ids");
    HasUntrackedUnfinishedActions_ = false;

    try {
        Bundle_->Config = attributes->Get<TBundleTabletBalancerConfigPtr>("tablet_balancer_config");
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Error parsing bundle attribute \"tablet_balancer_config\", skip bundle balancing iteration");
        Bundle_->Config.Reset();
    }
}

TFuture<void> TBundleState::UpdateState(bool fetchTabletCellsFromSecondaryMasters, int iterationIndex)
{
    return BIND(
            &TBundleState::DoUpdateState,
            MakeStrong(this),
            fetchTabletCellsFromSecondaryMasters,
            iterationIndex)
        .AsyncVia(Invoker_)
        .Run();
}

TFuture<void> TBundleState::FetchStatistics(
    const IListNodePtr& nodeStatistics,
    bool useStatisticsReporter,
    const TYPath& statisticsTablePath)
{
    return BIND(
            &TBundleState::DoFetchStatistics,
            MakeStrong(this),
            nodeStatistics,
            useStatisticsReporter,
            statisticsTablePath)
        .AsyncVia(Invoker_)
        .Run();
}

void TBundleState::DoUpdateState(bool fetchTabletCellsFromSecondaryMasters, int iterationIndex)
{
    THashMap<TTabletCellId, TTabletCellInfo> tabletCells;

    YT_LOG_DEBUG("Started fetching tablet cells (CellCount: %v)", CellIds_.size());
    Counters_->TabletCellTabletsRequestCount.Increment(CellIds_.size());

    auto secondaryCellTags = Client_->GetNativeConnection()->GetSecondaryMasterCellTags();
    if (fetchTabletCellsFromSecondaryMasters && !secondaryCellTags.empty()) {
        tabletCells = FetchTabletCells(secondaryCellTags);
    } else {
        tabletCells = FetchTabletCells({Client_->GetNativeConnection()->GetPrimaryMasterCellTag()});
    }
    YT_LOG_DEBUG("Finished fetching tablet cells");

    THashSet<TTabletId> tabletIds;
    THashSet<TTableId> newTableIds;
    THashMap<TTableId, THashSet<TTabletId>> newTableIdToTablets;

    for (const auto& [id, info] : tabletCells) {
        for (auto [tabletId, tableId] : info.TabletToTableId) {
            auto [it, inserted] = tabletIds.insert(tabletId);
            if (!inserted) {
                YT_LOG_DEBUG("Tablet was moved between fetches for different cells (TabletId: %v, NewCellId: %v)",
                    tabletId,
                    id);
            }

            if (!Tablets_.contains(tabletId)) {
                if (auto tableIt = Bundle_->Tables.find(tableId); tableIt != Bundle_->Tables.end()) {
                    EmplaceOrCrash(Tablets_, tabletId, New<TTablet>(tabletId, tableIt->second.Get()));
                } else {
                    // A new table has been found.
                    newTableIds.insert(tableId);
                    auto it = newTableIdToTablets.emplace(tableId, THashSet<TTabletId>{}).first;

                    // A tablet can be found mounted on two cells at the same time
                    // if the old cell's fetch was before the unmount and the new cell's fetch was after the mount.
                    // The correct cell will be picked later in DoFetchStatistics.
                    it->second.insert(tabletId);
                }
            }
        }
    }

    DropMissingKeys(Tablets_, tabletIds);

    YT_LOG_DEBUG("Started fetching basic table attributes (NewTableCount: %v)", newTableIds.size());
    Counters_->BasicTableAttributesRequestCount.Increment(newTableIds.size());
    auto tableInfos = FetchBasicTableAttributes(newTableIds);
    YT_LOG_DEBUG("Finished fetching basic table attributes (NewTableCount: %v)", tableInfos.size());

    for (auto& [tableId, tableInfo] : tableInfos) {
        if (iterationIndex > 0) {
            YT_LOG_DEBUG("New table has been found (TableId: %v, TablePath: %v)",
                tableId,
                tableInfo->Path);
        }

        TableRegistry_->AddTable(tableInfo);
        auto it = EmplaceOrCrash(Bundle_->Tables, tableId, std::move(tableInfo));

        const auto& tablets = GetOrCrash(newTableIdToTablets, tableId);
        for (auto tabletId : tablets) {
            auto tablet = New<TTablet>(tabletId, it->second.Get());
            EmplaceOrCrash(Tablets_, tabletId, tablet);
        }
    }

    Bundle_->TabletCells.clear();
    for (const auto& [cellId, tabletCellInfo] : tabletCells) {
        EmplaceOrCrash(Bundle_->TabletCells, cellId, tabletCellInfo.TabletCell);

        for (const auto& [tabletId, tableId] : tabletCellInfo.TabletToTableId) {
            if (!Tablets_.contains(tabletId)) {
                // Tablet was created (and found in FetchCells).
                // After that it was quickly removed before we fetched BasicTableAttributes.
                // Therefore a TTablet object was not created.
                // Skip this tablet and verify that this tabletId was fetched in FetchCells
                // and tableInfo was never fetched in BasicTableAttributes.

                YT_VERIFY(tabletIds.contains(tabletId) && !tableInfos.contains(tableId));
            }
        }
    }
}

bool TBundleState::IsTableBalancingAllowed(const TTableSettings& table) const
{
    if (!table.Dynamic) {
        return false;
    }

    return table.Config->EnableAutoTabletMove ||
        table.Config->EnableAutoReshard;
}

bool TBundleState::IsParameterizedBalancingEnabled() const
{
    for (const auto& [id, table] : Bundle_->Tables) {
        if (table->InMemoryMode == EInMemoryMode::None) {
            // Limits are not checked for ordinary tables.
            continue;
        }

        auto groupName = table->GetBalancingGroup();
        if (!groupName) {
            continue;
        }

        const auto& groupConfig = GetOrCrash(Bundle_->Config->Groups, *groupName);
        if (groupConfig->Type == EBalancingType::Parameterized && groupConfig->EnableMove) {
            return true;
        }
    }
    return false;
}

void TBundleState::DoFetchStatistics(
    const IListNodePtr& nodeStatistics,
    bool useStatisticsReporter,
    const TYPath& statisticsTablePath)
{
    YT_LOG_DEBUG("Started fetching actual table settings (TableCount: %v)", Bundle_->Tables.size());
    Counters_->ActualTableSettingsRequestCount.Increment(Bundle_->Tables.size());
    auto tableSettings = FetchActualTableSettings();
    YT_LOG_DEBUG("Finished fetching actual table settings (TableCount: %v)", tableSettings.size());

    THashSet<TTableId> tableIds;
    for (const auto& [id, info] : tableSettings) {
        EmplaceOrCrash(tableIds, id);
    }
    auto droppedTables = DropAndReturnMissingKeys(Bundle_->Tables, tableIds);
    for (auto table : droppedTables) {
        TableRegistry_->RemoveTable(table);
    }

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

    PerformanceCountersKeys_ = DefaultPerformanceCountersKeys;

    YT_LOG_DEBUG("Started fetching table statistics (TableCount: %v)", tableIdsToFetch.size());
    Counters_->TableStatisticsRequestCount.Increment(tableIdsToFetch.size());

    auto tableIdToStatistics = FetchTableStatistics(tableIdsToFetch, !useStatisticsReporter);

    YT_LOG_DEBUG("Finished fetching table statistics (TableCount: %v)", tableIdToStatistics.size());

    if (useStatisticsReporter) {
        FetchPerformanceCountersFromTable(&tableIdToStatistics, statisticsTablePath);
        PerformanceCountersKeys_.insert(
            PerformanceCountersKeys_.end(),
            AdditionalPerformanceCountersKeys.begin(),
            AdditionalPerformanceCountersKeys.end());
    }

    THashSet<TTableId> missingTables;
    for (const auto& tableId : tableIdsToFetch) {
        EmplaceOrCrash(missingTables, tableId);
    }

    for (const auto& [id, cell] : Bundle_->TabletCells) {
        // Not filled yet.
        YT_VERIFY(cell->Tablets.empty());
    }

    THashSet<TTableId> tablesFromAnotherBundle;
    for (auto& [tableId, statistics] : tableIdToStatistics) {
        auto& table = GetOrCrash(Bundle_->Tables, tableId);
        SetTableStatistics(table, statistics);

        for (auto& tabletResponse : statistics) {
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

                auto cellIt = Bundle_->TabletCells.find(tabletResponse.CellId);
                if (cellIt == Bundle_->TabletCells.end()) {
                    tablesFromAnotherBundle.insert(table->Id);
                    continue;
                }

                EmplaceOrCrash(cellIt->second->Tablets, tablet->Id, tablet);
                tablet->Cell = cellIt->second;
            } else {
                YT_VERIFY(tabletResponse.State == ETabletState::Unmounted);
                tablet->Cell = nullptr;
            }

            tablet->Index = tabletResponse.Index;
            tablet->Statistics = std::move(tabletResponse.Statistics);
            tablet->State = tabletResponse.State;
            tablet->MountTime = tabletResponse.MountTime;

            Visit(std::move(tabletResponse.PerformanceCounters),
                [&] (TTablet::TPerformanceCountersProtoList&& performanceCounters) {
                    tablet->PerformanceCounters = std::move(performanceCounters);
                },
                [&] (TUnversionedOwningRow&& performanceCounters) {
                    tablet->PerformanceCounters = std::move(performanceCounters);
                });

            YT_VERIFY(tablet->Index == std::ssize(table->Tablets));

            table->Tablets.push_back(tablet);
        }
        EraseOrCrash(missingTables, tableId);
    }

    for (auto tableId : missingTables) {
        EraseOrCrash(Bundle_->Tables, tableId);
        TableRegistry_->RemoveTable(tableId);
    }

    for (auto tableId : tablesFromAnotherBundle) {
        auto it = GetIteratorOrCrash(Bundle_->Tables, tableId);
        YT_LOG_DEBUG("Table from another bundle was found (TableId: %v, TablePath: %v)",
            tableId,
            it->second->Path);

        Bundle_->Tables.erase(it);
        TableRegistry_->RemoveTable(tableId);
    }

    THashSet<TTabletId> tabletIds;
    THashSet<TTableId> finalTableIds;
    for (const auto& [tableId, table] : Bundle_->Tables) {
        for (const auto& tablet : table->Tablets) {
            InsertOrCrash(tabletIds, tablet->Id);
        }
        InsertOrCrash(finalTableIds, tableId);
    }

    DropMissingKeys(Tablets_, tabletIds);
    DropMissingKeys(ProfilingCounters_, finalTableIds);

    Bundle_->NodeStatistics.clear();

    if (!nodeStatistics) {
        THROW_ERROR_EXCEPTION(
            "Failed to get node statistics because node fetch "
            "failed earlier during the current iteration");
    }

    THashSet<TNodeAddress> addresses;
    for (const auto& [id, cell] : Bundle_->TabletCells) {
        if (cell->NodeAddress) {
            addresses.insert(*cell->NodeAddress);
        }
    }

    Bundle_->NodeStatistics = GetNodeStatistics(nodeStatistics, addresses);
}

TBundleState::TTabletCellInfo TBundleState::TabletCellInfoFromAttributes(
    TTabletCellId cellId,
    const IAttributeDictionaryPtr& attributes) const
{
    auto tablets = attributes->Get<IMapNodePtr>("tablets");
    auto status = attributes->Get<TTabletCellStatus>("status");
    auto statistics = attributes->Get<TTabletCellStatistics>("total_statistics");
    auto peers = attributes->Get<std::vector<TTabletCellPeer>>("peers");
    auto lifeStage = attributes->Get<ETabletCellLifeStage>("tablet_cell_life_stage");

    std::optional<TNodeAddress> address;
    for (const auto& peer : peers) {
        if (peer.State == EPeerState::Leading) {
            if (address.has_value()) {
                YT_LOG_WARNING("Cell has two leading peers (Cell: %v, Peers: [%v, %v])",
                    cellId,
                    address,
                    peer.NodeAddress);

                address.reset();
                break;
            }
            address = peer.NodeAddress;
        }
    }

    auto tabletCell = New<TTabletCell>(cellId, statistics, status, std::move(address), lifeStage);

    return TTabletCellInfo{
        .TabletCell = std::move(tabletCell),
        .TabletToTableId = ParseTabletToTableMapping(tablets),
    };
}

THashMap<TTabletCellId, TBundleState::TTabletCellInfo> TBundleState::FetchTabletCells(
    const NObjectClient::TCellTagList& cellTags) const
{
    THashMap<TCellTag, TCellTagBatch> batchRequests;

    static const std::vector<TString> attributeKeys{"tablets", "status", "total_statistics", "peers", "tablet_cell_life_stage"};
    for (auto cellTag : cellTags) {
        auto proxy = CreateObjectServiceReadProxy(
            Client_,
            EMasterChannelKind::Follower,
            cellTag);
        auto it = EmplaceOrCrash(batchRequests, cellTag, TCellTagBatch{proxy.ExecuteBatch(), {}});

        for (auto cellId : CellIds_) {
            auto req = TYPathProxy::Get(FromObjectId(cellId) + "/@");
            ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
            it->second.Request->AddRequest(req, ToString(cellId));
        }
    }

    ExecuteRequestsToCellTags(&batchRequests);

    THashMap<TTabletCellId, TTabletCellInfo> tabletCells;
    for (auto cellTag : cellTags) {
        for (auto cellId : CellIds_) {
            const auto& batchReq = batchRequests[cellTag].Response.Get().Value();
            auto rspOrError = batchReq->GetResponse<TYPathProxy::TRspGet>(ToString(cellId));
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);

            auto attributes = ConvertToAttributes(TYsonString(rspOrError.Value()->value()));
            auto cellInfo = TabletCellInfoFromAttributes(cellId, attributes);

            if (auto it = tabletCells.find(cellId); it != tabletCells.end()) {
                it->second.TabletToTableId.insert(cellInfo.TabletToTableId.begin(), cellInfo.TabletToTableId.end());
            } else {
                EmplaceOrCrash(tabletCells, cellId, std::move(cellInfo));
            }
        }
    }

    return tabletCells;
}

THashMap<TNodeAddress, TTabletCellBundle::TNodeStatistics> TBundleState::GetNodeStatistics(
    const IListNodePtr& nodeStatisticsList,
    const THashSet<TNodeAddress>& addresses) const
{
    YT_VERIFY(nodeStatisticsList);

    static const TString TabletStaticPath = "/statistics/memory/tablet_static";
    static const TString TabletSlotsPath = "/tablet_slots";

    THashMap<TNodeAddress, TTabletCellBundle::TNodeStatistics> nodeStatistics;
    for (const auto& node : nodeStatisticsList->GetChildren()) {
        const auto& address = node->AsString()->GetValue();
        if (!addresses.contains(address)) {
            continue;
        }

        try {
            auto statistics = ConvertTo<TTabletCellBundle::TNodeStatistics>(
                SyncYPathGet(node->Attributes().ToMap(), TabletStaticPath));
            statistics.TabletSlotCount = ConvertTo<IListNodePtr>(
                SyncYPathGet(node->Attributes().ToMap(), TabletSlotsPath))->GetChildCount();

            EmplaceOrCrash(nodeStatistics, address, std::move(statistics));
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Failed to get \"statistics\" or \"tablet_slots\" attribute for node %Qv",
                address);
        }
    }

    if (std::ssize(nodeStatistics) != std::ssize(addresses)) {
        THROW_ERROR_EXCEPTION(
            "Failed to fetch statistics for some nodes of bundle %Qv",
            Bundle_->Name)
            << TErrorAttribute("fetched_count", std::ssize(nodeStatistics))
            << TErrorAttribute("expected_count", std::ssize(addresses));
    }

    return nodeStatistics;
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
            Bundle_.Get()));
    }

    return tableInfos;
}

THashMap<TTableId, TBundleState::TTableSettings> TBundleState::FetchActualTableSettings() const
{
    THashSet<TTableId> tableIds;
    for (const auto& [id, table] : Bundle_->Tables) {
        InsertOrCrash(tableIds, id);
    }

    auto cellTagToBatch = FetchTableAttributes(
        Client_,
        tableIds,
        Bundle_->Tables,
        [] (const NTabletClient::TMasterTabletServiceProxy::TReqGetTableBalancingAttributesPtr& request) {
            request->set_fetch_balancing_attributes(true);
        });

    THashMap<TTableId, TTableSettings> tableConfigs;
    for (const auto& [cellTag, batch] : cellTagToBatch) {
        THROW_ERROR_EXCEPTION_IF_FAILED(
            batch.Response.Get(),
            "Failed to fetch actual table settings from cell %v",
            cellTag);
        auto responseBatch = batch.Response.Get().Value();

        for (int index = 0; index < batch.Request->table_ids_size(); ++index) {
            auto tableId = FromProto<TTableId>(batch.Request->table_ids()[index]);

            const auto& response = responseBatch->tables()[index];
            if (!response.has_balancing_attributes()) {
                // The table has already been deleted.
                continue;
            }

            const auto& attributes = response.balancing_attributes();
            EmplaceOrCrash(tableConfigs, tableId, TTableSettings{
                .Config = ConvertTo<TTableTabletBalancerConfigPtr>(
                    TYsonStringBuf(attributes.tablet_balancer_config())),
                .InMemoryMode = FromProto<EInMemoryMode>(attributes.in_memory_mode()),
                .Dynamic = attributes.dynamic(),
            });
        }
    }

    return tableConfigs;
}

THashMap<TTableId, std::vector<TBundleState::TTabletStatisticsResponse>> TBundleState::FetchTableStatistics(
    const THashSet<TTableId>& tableIds,
    bool fetchPerformanceCounters) const
{
    auto cellTagToBatch = FetchTableAttributes(
        Client_,
        tableIds,
        Bundle_->Tables,
        [fetchPerformanceCounters] (const NTabletClient::TMasterTabletServiceProxy::TReqGetTableBalancingAttributesPtr& request) {
            request->set_fetch_statistics(true);
            if (fetchPerformanceCounters) {
                ToProto(request->mutable_requested_performance_counters(), DefaultPerformanceCountersKeys);
            }
    });

    THashMap<TTableId, std::vector<TTabletStatisticsResponse>> tableStatistics;
    for (const auto& [cellTag, batch] : cellTagToBatch) {
        THROW_ERROR_EXCEPTION_IF_FAILED(
            batch.Response.Get(),
            "Failed to fetch tablets from cell %v",
            cellTag);

        auto responseBatch = batch.Response.Get().ValueOrThrow();
        auto statisticsFieldNames = FromProto<std::vector<TString>>(responseBatch->statistics_field_names());

        for (int index = 0; index < batch.Request->table_ids_size(); ++index) {
            auto tableId = FromProto<TTableId>(batch.Request->table_ids()[index]);
            auto table = GetOrCrash(Bundle_->Tables, tableId);

            const auto& response = responseBatch->tables()[index];
            if (response.tablets_size() == 0) {
                // The table has already been deleted.
                continue;
            }

            auto parameterizedBalancingEnabled = table->IsParameterizedMoveBalancingEnabled() ||
                table->IsParameterizedReshardBalancingEnabled(/*enableParameterizedReshardByDefault*/ true);

            std::vector<TTabletStatisticsResponse> tablets;
            for (const auto& tablet : response.tablets()) {
                tablets.push_back(TTabletStatisticsResponse{
                    .Index = tablet.index(),
                    .TabletId = FromProto<TTabletId>(tablet.tablet_id()),
                    .State = FromProto<ETabletState>(tablet.state()),
                    .Statistics = BuildTabletStatistics(
                        tablet.statistics(),
                        statisticsFieldNames,
                        /*saveOriginalNode*/ parameterizedBalancingEnabled),
                });

                if (fetchPerformanceCounters) {
                    tablets.back().PerformanceCounters = tablet.performance_counters();
                }

                if (tablet.has_cell_id()) {
                    tablets.back().CellId = FromProto<TTabletCellId>(tablet.cell_id());
                }

                if (tablet.has_mount_time()) {
                    tablets.back().MountTime = FromProto<TInstant>(tablet.mount_time());
                }
            }

            EmplaceOrCrash(tableStatistics, tableId, std::move(tablets));
        }
    }

    return tableStatistics;
}

void TBundleState::FetchPerformanceCountersFromTable(
    THashMap<TTableId, std::vector<TTabletStatisticsResponse>>* tableIdToStatistics,
    const NYPath::TYPath& statisticsTablePath)
{
    YT_VERIFY(!statisticsTablePath.empty());

    THashSet<TTableId> tableIdsToFetch;
    for (const auto& [tableId, tablets] : *tableIdToStatistics) {
        const auto& table = GetOrCrash(Bundle_->Tables, tableId);
        if (table->IsParameterizedMoveBalancingEnabled() ||
            table->IsParameterizedReshardBalancingEnabled(/*enableParameterizedReshardByDefault*/ true))
        {
            tableIdsToFetch.insert(tableId);
        }
    }

    if (tableIdsToFetch.empty()) {
        return;
    }

    YT_LOG_DEBUG("Started fetching table performance counters from archive (TableCount: %v)",
        tableIdsToFetch.size());

    std::vector<TString> quotedTableIds;
    for (auto tableId : tableIdsToFetch) {
        quotedTableIds.push_back("\"" + ToString(tableId) + "\"");
    }

    NQueryClient::TQueryBuilder builder;
    builder.SetSource(statisticsTablePath);
    builder.AddSelectExpression("*");
    builder.AddWhereConjunct(Format(
        "table_id in (%v)",
        JoinSeq(", ", quotedTableIds)));

    auto selectResult = WaitFor(Client_->SelectRows(builder.Build()))
        .ValueOrThrow();
    PerformanceCountersTableSchema_ = selectResult.Rowset->GetSchema();

    THashMap<TTableId, THashMap<TTabletId, TUnversionedOwningRow>> tableToPerformanceCounters;
    for (const auto& row : selectResult.Rowset->GetRows()) {
        auto tableId = TGuid::FromString(FromUnversionedValue<TString>(
            row[PerformanceCountersTableSchema_->GetColumnIndexOrThrow("table_id")]));
        auto tabletId = TGuid::FromString(FromUnversionedValue<TString>(
            row[PerformanceCountersTableSchema_->GetColumnIndexOrThrow("tablet_id")]));
        tableToPerformanceCounters[tableId][tabletId] = TUnversionedOwningRow(row);
    }


    THashSet<TTableId> aliveTables;
    for (auto& [tableId, tablets] : *tableIdToStatistics) {
        if (!tableIdsToFetch.contains(tableId)) {
            aliveTables.insert(tableId);
            continue;
        }

        auto it = tableToPerformanceCounters.find(tableId);
        if (it == tableToPerformanceCounters.end()) {
            // The table has already been deleted or
            // the table is too new, then we just pretend that we haven’t seen it yet.
            continue;
        }

        aliveTables.insert(tableId);

        auto& tabletToPerformanceCounters = it->second;
        for (auto& tablet : tablets) {
            // There might be no such tablet in select result if the tablet has been unmounted for a long time
            // and performance counters have already been removed by ttl from the table.
            if (auto performanceCountersIt = tabletToPerformanceCounters.find(tablet.TabletId);
                performanceCountersIt != tabletToPerformanceCounters.end())
            {
                tablet.PerformanceCounters = std::move(performanceCountersIt->second);
            } else if (tablet.State == ETabletState::Mounted) {
                THROW_ERROR_EXCEPTION(
                    "Performance counters for tablet %v of table %v were not found in statistics table",
                    tablet.TabletId,
                    tableId);
            }
        }
    }

    DropMissingKeys(*tableIdToStatistics, aliveTables);

    YT_LOG_DEBUG("Finished fetching table performance counters from archive (TableCount: %v)",
        tableToPerformanceCounters.size());
}

TTableProfilingCounters& TBundleState::GetProfilingCounters(
    const TTable* table,
    const TString& groupName)
{
    auto it = ProfilingCounters_.find(table->Id);
    if (it == ProfilingCounters_.end()) {
        auto profilingCounters = InitializeProfilingCounters(table, groupName);
        return EmplaceOrCrash(
            ProfilingCounters_,
            table->Id,
            std::move(profilingCounters))->second;
    }

    if (it->second.GroupName != groupName) {
        it->second = InitializeProfilingCounters(table, groupName);
    }

    return it->second;
}

TTableProfilingCounters TBundleState::InitializeProfilingCounters(
    const TTable* table,
    const TString& groupName) const
{
    TTableProfilingCounters profilingCounters{.GroupName = groupName};
    auto profiler = Profiler_
        .WithSparse()
        .WithTag("group", groupName)
        .WithTag("table_path", table->Path);

    profilingCounters.InMemoryMoves = profiler.Counter("/tablet_balancer/in_memory_moves");
    profilingCounters.OrdinaryMoves = profiler.Counter("/tablet_balancer/ordinary_moves");
    profilingCounters.TabletMerges = profiler.Counter("/tablet_balancer/tablet_merges");
    profilingCounters.TabletSplits = profiler.Counter("/tablet_balancer/tablet_splits");
    profilingCounters.NonTrivialReshards = profiler.Counter("/tablet_balancer/non_trivial_reshards");
    profilingCounters.ParameterizedMoves = profiler.Counter("/tablet_balancer/parameterized_moves");
    profilingCounters.ParameterizedReshardMerges = profiler.Counter(
        "/tablet_balancer/parameterized_reshard_merges");
    profilingCounters.ParameterizedReshardSplits = profiler.Counter(
        "/tablet_balancer/parameterized_reshard_splits");

    return profilingCounters;
}

void TBundleState::SetTableStatistics(
    const TTablePtr& table,
    const std::vector<TTabletStatisticsResponse>& tablets)
{
    table->CompressedDataSize = 0;
    table->UncompressedDataSize = 0;

    for (const auto& tablet : tablets) {
        table->CompressedDataSize += tablet.Statistics.CompressedDataSize;
        table->UncompressedDataSize += tablet.Statistics.UncompressedDataSize;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
