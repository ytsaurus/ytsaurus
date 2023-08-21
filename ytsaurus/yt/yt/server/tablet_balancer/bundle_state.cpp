#include "bundle_state.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/server/lib/tablet_balancer/config.h>
#include <yt/yt/server/lib/tablet_balancer/table.h>
#include <yt/yt/server/lib/tablet_balancer/tablet.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/ytlib/tablet_client/master_tablet_service_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NTabletBalancer {

using namespace NApi;
using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletClient::NProto;
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
    , NodeStatisticsRequestCount(profiler.WithSparse().Counter("/master_requests/node_statistics_count"))
{ }

////////////////////////////////////////////////////////////////////////////////

const std::vector<TString> TBundleState::DefaultPerformanceCountersKeys_{
    #define XX(name, Name) #name,
    ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
    #undef XX
};

TBundleState::TBundleState(
    TString name,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker)
    : Bundle_(New<TTabletCellBundle>(name))
    , Logger(TabletBalancerLogger.WithTag("BundleName: %v", name))
    , Profiler_(TabletBalancerProfiler.WithTag("tablet_cell_bundle", name))
    , Client_(client)
    , Invoker_(invoker)
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
        // TODO(alexelexa): show such errors in orchid
        YT_LOG_ERROR(ex, "Error parsing bundle attribute \"tablet_balancer_config\", skip bundle balancing iteration");
        Bundle_->Config.Reset();
    }
}

TFuture<void> TBundleState::UpdateState(bool fetchTabletCellsFromSecondaryMasters)
{
    return BIND(&TBundleState::DoUpdateState, MakeStrong(this), fetchTabletCellsFromSecondaryMasters)
        .AsyncVia(Invoker_)
        .Run();
}

TFuture<void> TBundleState::FetchStatistics()
{
    return BIND(&TBundleState::DoFetchStatistics, MakeStrong(this))
        .AsyncVia(Invoker_)
        .Run();
}

void TBundleState::DoUpdateState(bool fetchTabletCellsFromSecondaryMasters)
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
        auto it = EmplaceOrCrash(Bundle_->Tables, tableId, std::move(tableInfo));
        InitializeProfilingCounters(it->second);

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

void TBundleState::DoFetchStatistics()
{
    YT_LOG_DEBUG("Started fetching actual table settings (TableCount: %v)", Bundle_->Tables.size());
    Counters_->ActualTableSettingsRequestCount.Increment(Bundle_->Tables.size());
    auto tableSettings = FetchActualTableSettings();
    YT_LOG_DEBUG("Finished fetching actual table settings (TableCount: %v)", tableSettings.size());

    THashSet<TTableId> tableIds;
    for (const auto& [id, info] : tableSettings) {
        EmplaceOrCrash(tableIds, id);
    }
    DropMissingKeys(Bundle_->Tables, tableIds);

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

    YT_LOG_DEBUG("Started fetching table statistics (TableCount: %v)", tableIdsToFetch.size());
    Counters_->TableStatisticsRequestCount.Increment(tableIdsToFetch.size());
    auto tableIdToStatistics = FetchTableStatistics(tableIdsToFetch);
    YT_LOG_DEBUG("Finished fetching table statistics (TableCount: %v)", tableIdToStatistics.size());

    THashSet<TTableId> missingTables;
    for (const auto& tableId : tableIdsToFetch) {
        EmplaceOrCrash(missingTables, tableId);
    }

    for (const auto& [id, cell] : Bundle_->TabletCells) {
        // Not filled yet.
        YT_VERIFY(cell->Tablets.empty());
    }

    for (auto& [tableId, statistics] : tableIdToStatistics) {
        auto& table = GetOrCrash(Bundle_->Tables, tableId);
        SetTableStatistics(table, statistics);

        for (const auto& tabletResponse : statistics) {
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
                    THROW_ERROR_EXCEPTION(
                        "Tablet %v of table %v belongs to an unknown cell %v",
                        tabletResponse.TabletId,
                        table->Id,
                        tabletResponse.CellId);
                }
                EmplaceOrCrash(cellIt->second->Tablets, tablet->Id, tablet);
                tablet->Cell = cellIt->second.Get();
            } else {
                YT_VERIFY(tabletResponse.State == ETabletState::Unmounted);
                tablet->Cell = nullptr;
            }

            tablet->Index = tabletResponse.Index;
            tablet->Statistics = std::move(tabletResponse.Statistics);
            tablet->PerformanceCountersProto = std::move(tabletResponse.PerformanceCounters);
            tablet->State = tabletResponse.State;
            tablet->MountTime = tabletResponse.MountTime;

            YT_VERIFY(tablet->Index == std::ssize(table->Tablets));

            table->Tablets.push_back(tablet);
        }
        EraseOrCrash(missingTables, tableId);
    }

    for (auto tableId : missingTables) {
        EraseOrCrash(Bundle_->Tables, tableId);
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

    Bundle_->NodeMemoryStatistics.clear();

    if (IsParameterizedBalancingEnabled()) {
        THashSet<TNodeAddress> addresses;
        for (const auto& [id, cell] : Bundle_->TabletCells) {
            if (cell->NodeAddress) {
                addresses.insert(*cell->NodeAddress);
            }
        }

        YT_LOG_DEBUG("Started fetching node statistics (NodeCount: %v)", addresses.size());
        Counters_->NodeStatisticsRequestCount.Increment(addresses.size());
        Bundle_->NodeMemoryStatistics = FetchNodeStatistics(addresses);
        YT_LOG_DEBUG("Finished fetching node statistics (NodeCount: %v)", addresses.size());
    }
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
            auto req = TTableYPathProxy::Get(FromObjectId(cellId) + "/@");
            ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
            it->second.Request->AddRequest(req, ToString(cellId));
        }
    }

    ExecuteRequestsToCellTags(&batchRequests);

    THashMap<TTabletCellId, TTabletCellInfo> tabletCells;
    for (auto cellTag : cellTags) {
        for (auto cellId : CellIds_) {
            const auto& batchReq = batchRequests[cellTag].Response.Get().Value();
            auto rspOrError = batchReq->GetResponse<TTableYPathProxy::TRspGet>(ToString(cellId));
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

THashMap<TNodeAddress, TTabletCellBundle::TNodeMemoryStatistics> TBundleState::FetchNodeStatistics(
    const THashSet<TNodeAddress>& addresses) const
{
    TListNodeOptions options;
    static const TString TabletStaticPath = "/statistics/memory/tablet_static";
    options.Attributes = TAttributeFilter({}, {TabletStaticPath});

    auto nodes = WaitFor(Client_
        ->ListNode("//sys/tablet_nodes", options))
        .ValueOrThrow();
    auto nodeList = ConvertTo<IListNodePtr>(nodes);

    THashMap<TNodeAddress, TTabletCellBundle::TNodeMemoryStatistics> nodeStatistics;
    for (const auto& node : nodeList->GetChildren()) {
        const auto& address = node->AsString()->GetValue();
        if (!addresses.contains(address)) {
            continue;
        }

        try {
            auto statistics = ConvertTo<TTabletCellBundle::TNodeMemoryStatistics>(
                SyncYPathGet(node->Attributes().ToMap(), TabletStaticPath));

            EmplaceOrCrash(nodeStatistics, address, std::move(statistics));
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Failed to get tablet_static attribute for node %v",
                address);
        }
    }

    THROW_ERROR_EXCEPTION_IF(std::ssize(nodeStatistics) != std::ssize(addresses),
        "Not all nodes fetched for bundle %v",
        Bundle_->Name);

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
    const THashSet<TTableId>& tableIds) const
{
    auto cellTagToBatch = FetchTableAttributes(
        Client_,
        tableIds,
        Bundle_->Tables,
        [] (const NTabletClient::TMasterTabletServiceProxy::TReqGetTableBalancingAttributesPtr& request) {
            request->set_fetch_statistics(true);
            ToProto(request->mutable_requested_performance_counters(), DefaultPerformanceCountersKeys_);
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

            std::vector<TTabletStatisticsResponse> tablets;
            for (const auto& tablet : response.tablets()) {
                tablets.push_back(TTabletStatisticsResponse{
                    .Index = tablet.index(),
                    .TabletId = FromProto<TTabletId>(tablet.tablet_id()),
                    .State = FromProto<ETabletState>(tablet.state()),
                    .Statistics = BuildTabletStatistics(
                        tablet.statistics(),
                        statisticsFieldNames,
                        /*saveOriginalNode*/ table->IsParameterizedBalancingEnabled()),
                    .PerformanceCounters = tablet.performance_counters(),
                });

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

void TBundleState::InitializeProfilingCounters(const TTablePtr& table)
{
    TTableProfilingCounters profilingCounters;
    auto profiler = Profiler_
        .WithSparse()
        .WithTag("table_path", table->Path);

    profilingCounters.InMemoryMoves = profiler.Counter("/tablet_balancer/in_memory_moves");
    profilingCounters.OrdinaryMoves = profiler.Counter("/tablet_balancer/ordinary_moves");
    profilingCounters.TabletMerges = profiler.Counter("/tablet_balancer/tablet_merges");
    profilingCounters.TabletSplits = profiler.Counter("/tablet_balancer/tablet_splits");
    profilingCounters.NonTrivialReshards = profiler.Counter("/tablet_balancer/non_trivial_reshards");
    profilingCounters.ParameterizedMoves = profiler.Counter("/tablet_balancer/parameterized_moves");

    EmplaceOrCrash(ProfilingCounters_, table->Id, std::move(profilingCounters));
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
