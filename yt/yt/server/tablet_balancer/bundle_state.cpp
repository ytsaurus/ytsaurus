#include "bundle_state.h"

#include "bootstrap.h"
#include "cluster_state_provider.h"
#include "config.h"
#include "helpers.h"
#include "private.h"
#include "table_registry.h"
#include "tablet_action.h"

#include <yt/yt/server/lib/tablet_balancer/config.h>
#include <yt/yt/server/lib/tablet_balancer/table.h>
#include <yt/yt/server/lib/tablet_balancer/tablet.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/ytlib/tablet_client/master_tablet_service_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NTabletBalancer {

using namespace NApi;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NHydra;
using namespace NLogging;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletClient::NProto;
using namespace NTabletNode;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const std::vector<std::string> DefaultPerformanceCountersKeys{
    #define XX(name, Name) #name,
    ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
    #undef XX
};

const std::vector<std::string> AdditionalPerformanceCountersKeys{
    #define XX(name, Name) #name,
    ITERATE_NODE_TABLET_PERFORMANCE_COUNTERS(XX)
    #undef XX
};

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTableSettings
{
    TTableTabletBalancerConfigPtr Config;
    EInMemoryMode InMemoryMode;
    bool Dynamic;
    TTableReplicaId UpstreamReplicaId;
};

bool IsTableBalancingEnabled(const TTableSettings& table)
{
    if (!table.Dynamic) {
        return false;
    }

    return table.Config->EnableAutoTabletMove || table.Config->EnableAutoReshard;
}

bool IsTableReplicaBalancingEnabled(const TBundleTabletBalancerConfigPtr& config, const TTableSettings& table)
{
    auto groupName = GetBalancingGroup(table.InMemoryMode, table.Config, config);
    if (!groupName) {
        return false;
    }

    const auto& groupConfig = GetOrCrash(config->Groups, *groupName);
    return !groupConfig->Parameterized->ReplicaClusters.empty();
}

struct TTableStatisticsResponse
{
    struct TTabletStatisticsResponse
    {
        i64 Index;
        TTabletId TabletId;

        ETabletState State;
        TTabletStatistics Statistics;
        std::variant<
            TTablet::TPerformanceCountersProtoList,
            TUnversionedOwningRow> PerformanceCounters;
        TTabletCellId CellId;
        TInstant MountTime = TInstant::Zero();
    };

    std::vector<TTabletStatisticsResponse> Tablets;
    std::vector<TLegacyOwningKey> PivotKeys;
};

void SetTableStatistics(const TTablePtr& table, const TTableStatisticsResponse& statistics)
{
    table->CompressedDataSize = 0;
    table->UncompressedDataSize = 0;

    for (const auto& tablet : statistics.Tablets) {
        table->CompressedDataSize += tablet.Statistics.CompressedDataSize;
        table->UncompressedDataSize += tablet.Statistics.UncompressedDataSize;
    }
}

DEFINE_ENUM(EFetchKind,
    (State)
    (Statistics)
    (PerformanceCounters)
);

struct TBundleAttributes
{
    ETabletCellHealth Health = ETabletCellHealth::Failed;
    std::vector<TTabletActionId> UnfinishedActions;
    TBundleTabletBalancerConfigPtr Config;
};

TBundleAttributes ParseBundleAttributes(const IAttributeDictionary* attributes, bool throwOnError)
{
    auto health = attributes->Get<ETabletCellHealth>("health");

    auto actions = attributes->Get<std::vector<IMapNodePtr>>("tablet_actions", {});
    std::vector<TTabletActionId> actionIds;
    for (auto actionMapNode : actions) {
        auto state = ConvertTo<ETabletActionState>(actionMapNode->GetChildOrThrow("state"));
        if (IsTabletActionFinished(state)) {
            continue;
        }

        auto actionId = ConvertTo<TTabletActionId>(actionMapNode->GetChildOrThrow("tablet_action_id"));
        actionIds.push_back(actionId);
    }

    TBundleTabletBalancerConfigPtr config;
    try {
        config = attributes->Get<TBundleTabletBalancerConfigPtr>("tablet_balancer_config");
    } catch (const TErrorException& ex) {
        config.Reset();
        if (throwOnError) {
            THROW_ERROR_EXCEPTION(
                NTabletBalancer::EErrorCode::IncorrectConfig,
                "Bundle has unparsable tablet balancer config")
                << ex;
        }
    }

    return TBundleAttributes{
        .Health = health,
        .UnfinishedActions = actionIds,
        .Config = config,
    };
}

////////////////////////////////////////////////////////////////////////////////

THashMap<TTabletId, TTableId> ParseTabletToTableMapping(const IMapNodePtr& mapNode)
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
    const std::vector<std::string>& keys,
    bool saveOriginalNode)
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

void Deserialize(TTabletCellPeer& value, INodePtr node)
{
    auto mapNode = node->AsMap();

    if (auto address = mapNode->FindChildValue<TNodeAddress>("address")) {
        value.NodeAddress = *address;
    }

    if (auto stateNode = mapNode->FindChildValue<EPeerState>("state")) {
        value.State = *stateNode;
    }
}

THashMap<TTableId, TCellTag> BuildTableToCellTagMapping(const THashMap<TTableId, TTablePtr>& tables)
{
    THashMap<TTableId, TCellTag> tableIdToCellTag;
    for (const auto& [tableId, table] : tables) {
        EmplaceOrCrash(tableIdToCellTag, tableId, table->ExternalCellTag);
    }
    return tableIdToCellTag;
}

TTabletBalancerDynamicConfigPtr FetchTabletBalancerConfig(
    const NNative::IClientPtr& client)
{
    auto proxy = CreateObjectServiceReadProxy(
        client,
        EMasterChannelKind::Follower);
    auto req = TYPathProxy::Get(DefaultTabletBalancerDynamicConfigPath);
    auto rsp = WaitFor(proxy.Execute(req))
        .ValueOrThrow();

    return ConvertTo<TTabletBalancerDynamicConfigPtr>(TYsonString(rsp->value()));
}

struct TTableResolveResponse
{
    TTableId Id;
    std::optional<NObjectClient::TCellTag> CellTag;
    NTabletClient::TTableReplicaId UpstreamReplicaId;
};

std::vector<TTableResolveResponse> ResolveTablePaths(
    const NNative::IClientPtr& client,
    const std::vector<TYPath>& paths)
{
    auto proxy = CreateObjectServiceReadProxy(
        client,
        EMasterChannelKind::Follower);
    auto batchRequest = proxy.ExecuteBatch();

    auto Logger = TabletBalancerLogger();
    for (auto path : paths) {
        static const std::vector<std::string> attributeKeys{
            "id",
            "external",
            "external_cell_tag",
            "dynamic",
            "upstream_replica_id",
        };
        auto req = TYPathProxy::Get(path + "/@");
        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
        batchRequest->AddRequest(req);
    }

    auto batchResponse = WaitFor(batchRequest->Invoke())
        .ValueOrThrow();

    std::vector<TTableResolveResponse> tableResolveResponses;
    for (int index = 0; index < std::ssize(paths); ++index) {
        const auto& rspOrError = batchResponse->GetResponse<TYPathProxy::TRspGet>(index);
        if (!rspOrError.IsOK()) {
            tableResolveResponses.emplace_back();
            YT_LOG_WARNING(rspOrError,
                "Failed to resolve table id for table %v", paths[index]);
            continue;
        }

        auto attributes = ConvertToAttributes(TYsonString(rspOrError.ValueOrThrow()->value()));
        auto tableId = attributes->Get<TTableId>("id");

        auto dynamic = attributes->Get<bool>("dynamic");
        THROW_ERROR_EXCEPTION_IF(!dynamic,
            "Replica table %v with path %v must be dynamic",
            tableId,
            paths[index]);

        auto cellTag = CellTagFromId(tableId);
        auto external = attributes->Get<bool>("external");
        if (external) {
            cellTag = attributes->Get<TCellTag>("external_cell_tag");
        }

        auto upstreamReplicaId = attributes->Find<TTableReplicaId>("upstream_replica_id");
        THROW_ERROR_EXCEPTION_IF(!upstreamReplicaId,
            "Replica table %v has no upstream replica id",
            tableId);

        tableResolveResponses.push_back(TTableResolveResponse{
            .Id = tableId,
            .CellTag = cellTag,
            .UpstreamReplicaId = *upstreamReplicaId,
        });
    }
    return tableResolveResponses;
}

THashMap<TClusterName, std::vector<TYPath>> GetReplicaBalancingMinorTables(
    const THashSet<TTableId>& majorTableIds,
    const THashMap<TTableId, TTablePtr>& tables,
    const std::string& selfClusterName,
    const TLogger& Logger,
    bool enableVerboseLogging)
{
    THashMap<TClusterName, THashSet<TYPath>> clusterToMinorTables;
    for (auto tableId : majorTableIds) {
        const auto& majorTable = GetOrCrash(tables, tableId);
        const auto minorTables = majorTable->GetReplicaBalancingMinorTables(selfClusterName);
        YT_LOG_DEBUG_IF(enableVerboseLogging,
            "List of minor tables associated with major table (MajorTableId: %v, MinorTables: %v)",
            tableId,
            minorTables);

        for (const auto& [cluster, tables] : minorTables) {
            clusterToMinorTables[cluster].insert(tables.begin(), tables.end());
        }
    }

    THashMap<TClusterName, std::vector<TYPath>> clusterToPaths;
    for (const auto& [cluster, paths] : clusterToMinorTables) {
        EmplaceOrCrash(clusterToPaths, cluster, std::vector(paths.begin(), paths.end()));
    }
    return clusterToPaths;
}

THashMap<TClusterName, THashMap<TYPath, std::vector<TTableId>>> GetReplicaBalancingMinorToMajorTables(
    const THashSet<TTableId>& majorTableIds,
    const THashMap<TTableId, TTablePtr>& tables,
    const std::string& selfClusterName)
{
    THashMap<TClusterName, THashMap<TYPath, std::vector<TTableId>>> minorToMajorTables;
    for (auto tableId : majorTableIds) {
        const auto& table = GetOrCrash(tables, tableId);
        for (const auto& [cluster, minorTables] : table->GetReplicaBalancingMinorTables(selfClusterName)) {
            for (const auto& minorTable : minorTables) {
                minorToMajorTables[cluster][minorTable].push_back(tableId);
            }
        }
    }
    return minorToMajorTables;
}

void RemoveTablesFromBundle(const TBundleSnapshotPtr& bundleSnapshot, const THashSet<TTableId>& tableIdsToRemove)
{
    auto removeTablets = [&tableIdsToRemove] (THashMap<TTabletId, TTabletPtr>* tablets) {
        std::vector<TTabletId> tabletsToRemove;
        for (const auto& [id, tablet] : *tablets) {
            if (tableIdsToRemove.contains(tablet->Table->Id)) {
                tabletsToRemove.push_back(id);
            }
        }
        for (auto id : tabletsToRemove) {
            EraseOrCrash(*tablets, id);
        }
    };

    removeTablets(&bundleSnapshot->Bundle->Tablets);

    for (const auto& [id, cell] : bundleSnapshot->Bundle->TabletCells) {
        // We ignore cell statistics and don't decrease Cell.MemorySize here.
        removeTablets(&cell->Tablets);
    }

    for (auto id : tableIdsToRemove) {
        EraseOrCrash(bundleSnapshot->Bundle->Tables, id);
    }

    // We ignore old tables in TableRegistry.
    // We plan to remove TableRegistry itself.
}

void CheckBundleSnapshotInvariants(const TBundleSnapshotPtr& bundleSnapshot)
{
    THashSet<TTabletId> mountedTabletIdsFromTables;
    THashSet<TTabletId> tabletIdsFromTabletCells;

    auto Logger = TabletBalancerLogger()
        .WithTag("BundleName: %v", bundleSnapshot->Bundle->Name)
        .WithTag("StateFetchTime: %v", bundleSnapshot->StateFetchTime)
        .WithTag("StatisticsFetchTime: %v", bundleSnapshot->StatisticsFetchTime)
        .WithTag("PerformanceCountersFetchTime: %v", bundleSnapshot->PerformanceCountersFetchTime);

    YT_LOG_FATAL_UNLESS(
        bundleSnapshot->Bundle->Config,
        "Bundle snapshot invariant check failed: does not have config");

    for (const auto& [id, tablet] : bundleSnapshot->Bundle->Tablets) {
        YT_VERIFY(tablet->Table);
        YT_LOG_FATAL_IF(
            std::find_if(tablet->Table->Tablets.begin(), tablet->Table->Tablets.end(), [&id] (const auto& tablet) {
                return tablet->Id == id;
            }) == tablet->Table->Tablets.end(),
            "Bundle snapshot invariant check failed: tablet's table does not have this tablet in list of tablets "
            "(TabletId: %v, TableId: %v)",
            id,
            tablet->Table->Id);

        YT_LOG_FATAL_UNLESS(
            bundleSnapshot->Bundle->Tables.contains(tablet->Table->Id),
            "Bundle snapshot invariant check failed: tablet linked to unknown table "
            "(TabletId: %v, TableId: %v)",
            id,
            tablet->Table->Id);

        auto cell = tablet->Cell.Lock();
        if (tablet->State == ETabletState::Unmounted) {
            YT_LOG_FATAL_IF(
                cell,
                "Bundle snapshot invariant check failed: unmounted tablet linked to cell (TabletId: %v, CellId: %v)",
                id,
                cell ? ToString(cell->Id) : "<nonexistent>");
            continue;
        }

        if (!cell) {
            YT_LOG_DEBUG("Bundle snapshot invariant check will fail, printing tablets found on each tablet cell");
            for (const auto& [cellId, tabletCell] : bundleSnapshot->Bundle->TabletCells) {
                YT_LOG_EVENT(
                    TabletBalancerLogger(),
                    NLogging::ELogLevel::Debug,
                    "List of tablets on tablet cell (CellId: %v, TabletIds: %v)",
                    cellId,
                    GetKeys(tabletCell->Tablets));
            }
        }

        YT_LOG_FATAL_UNLESS(
            cell,
            "Bundle snapshot invariant check failed: mounted tablet is not linked to any alive cell (TabletId: %v)",
            id);

        YT_LOG_FATAL_UNLESS(
            cell->Tablets.contains(id),
            "Bundle snapshot invariant check failed: tablet's cell does not have this tablet in list of tablets "
            "(TabletId: %v, CellId: %v)",
            id,
            cell->Id);
    }

    for (const auto& [id, cell] : bundleSnapshot->Bundle->TabletCells) {
        if (cell->NodeAddress) {
            YT_LOG_FATAL_UNLESS(
                bundleSnapshot->Bundle->NodeStatistics.contains(*cell->NodeAddress),
                "Bundle snapshot invariant check failed: cell node address is missing in list of nodes "
                "(CellId: %v, NodeAddress: %v)",
                id,
                cell->NodeAddress);
        }

        for (const auto& [tabletId, tablet] : cell->Tablets) {
            YT_LOG_FATAL_UNLESS(
                cell->Tablets.contains(tabletId),
                "Bundle snapshot invariant check failed: tablet to cell link mistmatch (TabletId: %v, CellId: %v)",
                tabletId,
                id);
            YT_LOG_FATAL_IF(
                tablet->State == ETabletState::Unmounted,
                "Bundle snapshot invariant check failed: tablet from cell's list of tablets is unmounted "
                "(TabletId: %v, CellId: %v)",
                tabletId,
                id);
            YT_LOG_FATAL_UNLESS(
                bundleSnapshot->Bundle->Tablets.contains(tabletId),
                "Bundle snapshot invariant check failed: tablet cell has unknown tablet (TabletId: %v, CellId: %v)",
                tabletId,
                id);
        }
    }

    for (const auto& [id, table] : bundleSnapshot->Bundle->Tables) {
        YT_LOG_FATAL_IF(
            !table->PivotKeys.empty() && std::ssize(table->Tablets) != std::ssize(table->PivotKeys),
            "Bundle snapshot invariant check failed: tablet count and pivot keys mistmatch "
            "(TableId: %v, TabletCount: %v, PivotKeyCount: %v)",
            id,
            std::ssize(table->Tablets),
            std::ssize(table->PivotKeys));

        for (const auto& tablet : table->Tablets) {
            YT_LOG_FATAL_UNLESS(
                bundleSnapshot->Bundle->Tablets.contains(tablet->Id),
                "Bundle snapshot invariant check failed: table has unknown tablet (TabletId: %v, TableId: %v)",
                tablet->Id,
                id);

            YT_LOG_FATAL_IF(
                tablet->Table != table.Get(),
                "Bundle snapshot invariant check failed: tablet linked to a different table (TabletId: %v, TableId: %v)",
                tablet->Id,
                id);
        }

        YT_LOG_FATAL_UNLESS(
            table->TableConfig,
            "Bundle snapshot invariant check failed: table do not have config (TableId: %v)",
            id);

        YT_LOG_FATAL_IF(
            bundleSnapshot->Bundle.Get() != table->Bundle,
            "Bundle snapshot invariant check failed: table linked to unknown tablet cell bundle "
            "(Bundle: %v, TableId: %v)",
            bundleSnapshot->Bundle->Name,
            id);
    }
}

void DropAlienTables(const TBundleSnapshotPtr& bundleSnapshot)
{
    bundleSnapshot->AlienTablePaths.clear();
    bundleSnapshot->AlienTables.clear();
    bundleSnapshot->BannedReplicaClusters.clear();

    for (auto& [id, table] : bundleSnapshot->Bundle->Tables) {
        table->ReplicaMode.reset();
        table->AlienTables.clear();
    }
}

auto GetPerformanceCountersColumnCount(const TTableSchemaPtr& schema)
{
    return std::count_if(schema->Columns().begin(), schema->Columns().end(), [] (const auto& column) {
        return column.LogicalType()->GetMetatype() == ELogicalMetatype::Struct;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

TBundleProfilingCounters::TBundleProfilingCounters(const NProfiling::TProfiler& profiler)
    : TabletCellTabletsRequestCount(profiler.WithSparse().Counter("/master_requests/tablet_cell_tablets_count"))
    , BasicTableAttributesRequestCount(profiler.WithSparse().Counter("/master_requests/basic_table_attributes_count"))
    , ActualTableSettingsRequestCount(profiler.WithSparse().Counter("/master_requests/actual_table_settings_count"))
    , TableStatisticsRequestCount(profiler.WithSparse().Counter("/master_requests/table_statistics_count"))
    , DirectStateRequest(profiler.WithSparse().Counter("/bundle_state_provider/direct_state_request"))
    , DirectStatisticsRequest(profiler.WithSparse().Counter("/bundle_state_provider/direct_statistics_request"))
    , DirectPerformanceCountersRequest(profiler.WithSparse().Counter("/bundle_state_provider/direct_performance_counters_request"))
    , StateRequestThrottled(profiler.WithSparse().Counter("/master_requests/state_request_throttled"))
    , StatisticsRequestThrottled(profiler.WithSparse().Counter("/master_requests/statistics_request_throttled"))
{ }

////////////////////////////////////////////////////////////////////////////////

class TBundleState
    : public IBundleState
{
public:
    using TPerClusterPerformanceCountersTableSchemas = THashMap<TClusterName, NQueryClient::TTableSchemaPtr>;

public:
    TBundleState(
        TString name,
        IBootstrap* bootstrap,
        IInvokerPtr fetcherInvoker,
        IInvokerPtr controlInvoker,
        TBundleStateProviderConfigPtr config,
        IClusterStateProviderPtr clusterStateProvider,
        IMulticellThrottlerPtr throttler,
        const IAttributeDictionary* initialAttributes);

    TFuture<TBundleSnapshotPtr> GetBundleSnapshot() override;
    TFuture<TBundleSnapshotPtr> GetBundleSnapshotWithReplicaBalancingStatistics(
        std::tuple<TInstant, TInstant, TInstant> minFreshnessRequirement,
        const THashSet<TGroupName>& groupsForMoveBalancing,
        const THashSet<TGroupName>& groupsForReshardBalancing,
        const THashSet<std::string>& allowedReplicaClusters,
        const THashSet<std::string>& replicaClustersToIgnore) override;

    void Start() override;
    void Stop() override;

    void Reconfigure(TBundleStateProviderConfigPtr config) override;

    TFuture<TBundleTabletBalancerConfigPtr> GetConfig(bool allowStale) override;
    ETabletCellHealth GetHealth() const override;
    std::vector<TTabletActionId> GetUnfinishedActions() const override;

private:
    struct TTabletCellInfo
    {
        TTabletCellPtr TabletCell;
        THashMap<TTabletId, TTableId> TabletToTableId;
    };

    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler_;
    const TString Name_;

    const NApi::NNative::IClientPtr Client_;
    const NHiveClient::TClientDirectoryPtr ClientDirectory_;
    const NHiveClient::TClusterDirectoryPtr ClusterDirectory_;
    const IInvokerPtr FetcherInvoker_;
    const IInvokerPtr ControlInvoker_;

    const std::string SelfClusterName_;
    const IClusterStateProviderPtr ClusterStateProvider_;
    const TPeriodicExecutorPtr PollExecutor_;
    const IMulticellThrottlerPtr MasterRequestThrottler_;

    const TTableRegistryPtr TableRegistry_;

    TAtomicIntrusivePtr<TBundleStateProviderConfig> Config_;

    TInstant BundleConfigFetchTime_;
    TBundleTabletBalancerConfigPtr BundleConfig_;
    NTabletClient::ETabletCellHealth Health_;
    std::vector<TTabletActionId> UnfinishedActions_;

    TBundleProfilingCountersPtr Counters_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);

    std::vector<TBundleSnapshotPtr> BundleSnapshots_;
    TFuture<TBundleTabletBalancerConfigPtr> BundleConfigFuture_;
    TFuture<TBundleSnapshotPtr> UpdateStateFuture_;
    TFuture<TBundleSnapshotPtr> UpdateStatisticsFuture_;
    TFuture<TBundleSnapshotPtr> UpdatePerformanceCountersFuture_;

    void FetchState();

    TBundleTabletBalancerConfigPtr UpdateConfig();
    TBundleSnapshotPtr UpdateState();
    TBundleSnapshotPtr UpdateStatistics();
    TBundleSnapshotPtr UpdatePerformanceCounters();

    TBundleSnapshotPtr DoUpdateState(const TBundleStateProviderConfigPtr& config);

    TBundleSnapshotPtr DoUpdateStatistics(
        const TBundleStateProviderConfigPtr& config,
        TBundleSnapshotPtr bundleSnapshot = nullptr) const;

    TBundleSnapshotPtr DoUpdatePerformanceCounters(
        const TBundleStateProviderConfigPtr& config,
        TBundleSnapshotPtr bundleSnapshot = nullptr) const;

    TBundleSnapshotPtr DeepCopyLatestBundleSnapshot(EFetchKind kind) const;

    void BuildNewState(
        bool fetchTabletCellsFromSecondaryMasters,
        const TBundleSnapshotPtr& bundleSnapshot,
        const std::vector<TTabletCellId>& cellIds,
        bool isFirstIteration);

    void InitializeAttributes(
        const IAttributeDictionary* attributes,
        const TBundleSnapshotPtr& bundleSnapshot,
        bool throwOnError);

    THashMap<TTabletCellId, TTabletCellInfo> FetchTabletCells(
        const std::vector<TTabletCellId>& cellIds,
        const NObjectClient::TCellTagList& cellTags) const;

    THashMap<TTableId, TTablePtr> FetchBasicTableAttributes(
        const THashSet<TTableId>& tableIds,
        TTabletCellBundle* bundle) const;
    THashMap<TNodeAddress, TTabletCellBundle::TNodeStatistics> GetNodeStatistics(
        const NYTree::IListNodePtr& nodeStatistics,
        const THashSet<std::string>& addresses) const;

    TTabletCellInfo TabletCellInfoFromAttributes(
        TTabletCellId cellId,
        const NYTree::IAttributeDictionaryPtr& attributes) const;

    TBundleSnapshotPtr DoGetBundleSnapshot();

    TBundleSnapshotPtr DoGetBundleSnapshotWithReplicaBalancingStatistics(
        std::tuple<TInstant, TInstant, TInstant> minFreshnessRequirement,
        const THashSet<TGroupName>& groupsForMoveBalancing,
        const THashSet<TGroupName>& groupsForReshardBalancing,
        const THashSet<std::string>& allowedReplicaClusters,
        const THashSet<std::string>& replicaClustersToIgnore);

    void FetchStatistics(
        TBundleSnapshotPtr bundleSnapshot,
        const NYTree::IListNodePtr& nodeStatistics,
        const TBundleStateProviderConfigPtr& config) const;

    void FetchReplicaStatistics(
        const TBundleSnapshotPtr& bundleSnapshot,
        const THashSet<std::string>& allowedReplicaClusters,
        const THashSet<std::string>& replicaClustersToIgnore,
        bool fetchReshard,
        bool fetchMove);

    static THashMap<TTableId, TTableSettings> FetchActualTableSettings(
        const NApi::NNative::IClientPtr& client,
        const THashSet<TTableId>& tableIdsToFetch,
        const THashMap<TTableId, TCellTag>& tableIdToCellTag,
        const IMulticellThrottlerPtr& throttler);

    static THashMap<TTableId, TTableStatisticsResponse> FetchTableStatistics(
        const NApi::NNative::IClientPtr& client,
        const THashSet<TTableId>& tableIds,
        const THashSet<TTableId>& tableIdsToFetchPivotKeys,
        const THashMap<TTableId, TCellTag>& tableIdToCellTag,
        const IMulticellThrottlerPtr& throttler,
        bool fetchPerformanceCounters,
        bool parameterizedBalancingEnabledDefault = false,
        THashSet<TTableId> tableIdsWithParameterizedBalancing = {});

    void FetchReplicaModes(
        const TBundleSnapshotPtr& bundleSnapshot,
        const THashSet<TTableId>& majorTableIds,
        const THashSet<std::string>& allowedReplicaClusters,
        const THashSet<std::string>& replicaClustersToIgnore);

    void FetchPerformanceCountersFromAlienTable(
        const TBundleSnapshotPtr& bundleSnapshot,
        const NApi::NNative::IClientPtr& client,
        THashMap<TTableId, TTableStatisticsResponse>* tableIdToStatistics,
        const std::string& cluster);

    void FillPerformanceCounters(
        THashMap<TTableId, TTableStatisticsResponse>* tableIdToStatistics,
        const TTablePerformanceCountersMap& tableToPerformanceCounters) const;
    void FillTabletWithStatistics(
        const TTabletPtr& tablet,
        TTableStatisticsResponse::TTabletStatisticsResponse& tabletResponse) const;

    bool IsReplicaBalancingEnabled(
        const TTabletCellBundlePtr& bundle,
        const THashSet<TGroupName>& groupNames,
        std::function<bool(const TTableTabletBalancerConfigPtr&)> isBalancingEnabled) const;
    bool HasReplicaBalancingGroups(const TTabletCellBundlePtr& bundle) const;

    TTableProfilingCounters InitializeProfilingCounters(
        const TTable* table,
        const TString& groupName) const;

    THashSet<TTableId> GetReplicaBalancingMajorTables(const TTabletCellBundlePtr& bundle) const;

    void DropOldBundleSnapshots();
    TFuture<TBundleSnapshotPtr> CreateUpdateFutureIfNeeded(EFetchKind kind, bool isDirectRequest);

    TBundleSnapshotPtr GetLatestBundleSnapshot(EFetchKind kind) const;
    TBundleSnapshotPtr GetLatestSatisfyingSnapshot(
        std::tuple<TInstant, TInstant, TInstant> minFreshnessRequirement) const;

    INodePtr GetBundleWithAttributes() const;
};

TBundleState::TBundleState(
    TString name,
    IBootstrap* bootstrap,
    IInvokerPtr invoker,
    IInvokerPtr controlInvoker,
    TBundleStateProviderConfigPtr config,
    IClusterStateProviderPtr clusterStateProvider,
    IMulticellThrottlerPtr throttler,
    const IAttributeDictionary* initialAttributes)
    : Logger(TabletBalancerLogger().WithTag("BundleName: %v", name))
    , Profiler_(TabletBalancerProfiler().WithTag("tablet_cell_bundle", name))
    , Name_(std::move(name))
    , Client_(bootstrap->GetClient())
    , ClientDirectory_(bootstrap->GetClientDirectory())
    , ClusterDirectory_(bootstrap->GetClusterDirectory())
    , FetcherInvoker_(invoker)
    , ControlInvoker_(controlInvoker)
    , SelfClusterName_(bootstrap->GetClusterName())
    , ClusterStateProvider_(clusterStateProvider)
    , PollExecutor_(New<TPeriodicExecutor>(
        ControlInvoker_,
        BIND(&TBundleState::FetchState, MakeWeak(this)),
        config->FetchPlannerPeriod))
    , MasterRequestThrottler_(throttler)
    , TableRegistry_(New<TTableRegistry>())
    , Config_(std::move(config))
    , Counters_(New<TBundleProfilingCounters>(Profiler_))
    , BundleSnapshots_({New<TBundleSnapshot>()})
{
    BundleSnapshots_.back()->Bundle = New<TTabletCellBundle>(Name_);
    InitializeAttributes(initialAttributes, BundleSnapshots_.back(), /*throwOnError*/ false);
    Start();
}

void TBundleState::InitializeAttributes(
    const IAttributeDictionary* attributes,
    const TBundleSnapshotPtr& bundleSnapshot,
    bool throwOnError)
{
    YT_VERIFY(bundleSnapshot);
    auto now = Now();
    auto guard = WriterGuard(Lock_);

    try {
        auto parsedAttributes = ParseBundleAttributes(attributes, throwOnError);

        Health_ = parsedAttributes.Health;
        UnfinishedActions_ = std::move(parsedAttributes.UnfinishedActions);
        bundleSnapshot->Bundle->Config = parsedAttributes.Config;

        if (BundleConfigFetchTime_ < now) {
            BundleConfigFetchTime_ = now;
            BundleConfig_ = bundleSnapshot->Bundle->Config;
        }
    } catch (const TErrorException& ex) {
        YT_LOG_ERROR(ex, "Failed to parse bundle attributes");
        bundleSnapshot->Bundle->Config.Reset();
        if (throwOnError) {
            throw;
        }
    }
}

void TBundleState::DropOldBundleSnapshots()
{
    YT_ASSERT_WRITER_SPINLOCK_AFFINITY(Lock_);

    if (std::ssize(BundleSnapshots_) <= 3) {
        // There might be old bundle snapshots, but it does not matter if there is no too much of them.
        return;
    }

    auto addUnique = [] (std::vector<TBundleSnapshotPtr>* snapshots, TBundleSnapshotPtr newSnapshot) {
        for (const auto& snapshot : *snapshots) {
            if (snapshot.Get() == newSnapshot.Get()) {
                return;
            }
        }
        snapshots->push_back(std::move(newSnapshot));
    };

    std::vector<TBundleSnapshotPtr> bundleSnapshots;
    addUnique(&bundleSnapshots, GetLatestBundleSnapshot(EFetchKind::State));
    addUnique(&bundleSnapshots, GetLatestBundleSnapshot(EFetchKind::Statistics));
    addUnique(&bundleSnapshots, GetLatestBundleSnapshot(EFetchKind::PerformanceCounters));

    std::swap(BundleSnapshots_, bundleSnapshots);
}

TFuture<TBundleSnapshotPtr> TBundleState::CreateUpdateFutureIfNeeded(EFetchKind kind, bool isDirectRequest)
{
    YT_ASSERT_WRITER_SPINLOCK_AFFINITY(Lock_);

    switch (kind) {
        case EFetchKind::State: {
            if (!UpdateStateFuture_) {
                Counters_->DirectStateRequest.Increment(isDirectRequest);

                YT_LOG_DEBUG("Planning to fetch bundle state%v",
                    isDirectRequest ? " due to a direct request" : "");
                UpdateStateFuture_ = BIND(&TBundleState::UpdateState, MakeStrong(this))
                    .AsyncVia(FetcherInvoker_)
                    .Run();
            }
            return UpdateStateFuture_;
        }

        case EFetchKind::Statistics: {
            if (UpdateStateFuture_) {
                return UpdateStateFuture_;
            }

            if (!UpdateStatisticsFuture_) {
                Counters_->DirectStatisticsRequest.Increment(isDirectRequest);

                YT_LOG_DEBUG("Planning to fetch bundle statistics%v",
                    isDirectRequest ? " due to a direct request" : "");
                UpdateStatisticsFuture_ = BIND(&TBundleState::UpdateStatistics, MakeStrong(this))
                    .AsyncVia(FetcherInvoker_)
                    .Run();
            }
            return UpdateStatisticsFuture_;
        }

        case EFetchKind::PerformanceCounters: {
            if (UpdateStateFuture_) {
                return UpdateStateFuture_;
            }

            if (UpdateStatisticsFuture_) {
                return UpdateStatisticsFuture_;
            }

            if (!UpdatePerformanceCountersFuture_) {
                Counters_->DirectPerformanceCountersRequest.Increment(isDirectRequest);

                YT_LOG_DEBUG("Planning to fetch performance counters%v",
                    isDirectRequest ? " due to a direct request" : "");
                UpdatePerformanceCountersFuture_ = BIND(&TBundleState::UpdatePerformanceCounters, MakeStrong(this))
                    .AsyncVia(FetcherInvoker_)
                    .Run();
            }
            return UpdatePerformanceCountersFuture_;
        }
    }
}

TFuture<TBundleSnapshotPtr> TBundleState::GetBundleSnapshot()
{
    auto now = Now();
    auto readerGuard = WriterGuard(Lock_);
    auto config = Config_.Acquire();

    auto getSatisfyingBundleSnapshot = [&] () -> TBundleSnapshotPtr {
        for (const auto& snapshot : BundleSnapshots_) {
            if (now <= snapshot->StateFetchTime + config->StateFreshnessTime &&
                now <= snapshot->StatisticsFetchTime + config->StatisticsFreshnessTime &&
                now <= snapshot->PerformanceCountersFetchTime + config->PerformanceCountersFreshnessTime)
            {
                return snapshot;
            }
        }
        return nullptr;
    };

    auto getFetchKind = [&] () {
        auto fetchKind = EFetchKind::State;
        for (const auto& snapshot : BundleSnapshots_) {
            if (now <= snapshot->StateFetchTime + config->StateFreshnessTime &&
                now <= snapshot->StatisticsFetchTime + config->StatisticsFreshnessTime)
            {
                return EFetchKind::PerformanceCounters;
            } else if (now <= snapshot->StateFetchTime + config->StateFreshnessTime) {
                fetchKind = EFetchKind::Statistics;
            }
        }
        return fetchKind;
    };

    if (auto snapshot = getSatisfyingBundleSnapshot()) {
        return MakeFuture(snapshot);
    }

    DropOldBundleSnapshots();

    auto kind = getFetchKind();
    return CreateUpdateFutureIfNeeded(kind, /*isDirectRequest*/ true);
}

TFuture<TBundleSnapshotPtr> TBundleState::GetBundleSnapshotWithReplicaBalancingStatistics(
    std::tuple<TInstant, TInstant, TInstant> minFreshnessRequirement,
    const THashSet<TGroupName>& groupsForMoveBalancing,
    const THashSet<TGroupName>& groupsForReshardBalancing,
    const THashSet<std::string>& allowedReplicaClusters,
    const THashSet<std::string>& replicaClustersToIgnore)
{
    return BIND(
        &TBundleState::DoGetBundleSnapshotWithReplicaBalancingStatistics,
        MakeStrong(this),
        minFreshnessRequirement,
        groupsForMoveBalancing,
        groupsForReshardBalancing,
        allowedReplicaClusters,
        replicaClustersToIgnore)
        .AsyncVia(FetcherInvoker_)
        .Run();
}

void TBundleState::Start()
{
    YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

    YT_LOG_DEBUG("Starting bundle state instance");

    {
        auto guard = WriterGuard(Lock_);

        UpdateStateFuture_.Reset();
        UpdateStatisticsFuture_.Reset();
        UpdatePerformanceCountersFuture_.Reset();
        BundleConfigFuture_.Reset();
    }

    PollExecutor_->Start();
}

void TBundleState::Stop()
{
    YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

    YT_LOG_DEBUG("Stopping bundle state instance");
    YT_UNUSED_FUTURE(PollExecutor_->Stop());
}

void TBundleState::Reconfigure(TBundleStateProviderConfigPtr config)
{
    YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

    auto oldConfig = Config_.Acquire();
    auto oldFetchPeriod = oldConfig->FetchPlannerPeriod;
    auto newFetchPeriod = config->FetchPlannerPeriod;

    Config_.Store(std::move(config));
    if (oldFetchPeriod != newFetchPeriod) {
        PollExecutor_->SetPeriod(newFetchPeriod);
    }
}

TBundleSnapshotPtr TBundleState::DoGetBundleSnapshotWithReplicaBalancingStatistics(
    std::tuple<TInstant, TInstant, TInstant> minFreshnessRequirement,
    const THashSet<TGroupName>& groupsForMoveBalancing,
    const THashSet<TGroupName>& groupsForReshardBalancing,
    const THashSet<std::string>& allowedReplicaClusters,
    const THashSet<std::string>& replicaClustersToIgnore)
{
    // Logically all replica balancing fields cannot be modified from two threads at the same time
    // because replica balancing attributes can only be fetched in sync mode and will be used right after that.
    // We are not planning to run two balancing operations of the same bundle at the same time.
    // Therefore, we are not planning to fetch replica balancing attributes of the same bundle at the same time.

    TBundleSnapshotPtr bundleSnapshot;
    {
        auto readerGuard = ReaderGuard(Lock_);
        bundleSnapshot = GetLatestSatisfyingSnapshot(minFreshnessRequirement);
    }
    YT_VERIFY(bundleSnapshot);

    bool isReshardReplicaBalancingRequired = IsReplicaBalancingEnabled(
        bundleSnapshot->Bundle,
        groupsForReshardBalancing,
        [] (const TTableTabletBalancerConfigPtr& config) {
            return config->EnableAutoReshard;
        });

    bool isMoveReplicaBalancingRequired = IsReplicaBalancingEnabled(
        bundleSnapshot->Bundle,
        groupsForMoveBalancing,
        [] (const TTableTabletBalancerConfigPtr& config) {
            return config->EnableAutoTabletMove;
        });

    YT_LOG_DEBUG("Calculated if replica statistics fetch required (IsReshardRequired: %v, IsMoveRequired: %v)",
        isReshardReplicaBalancingRequired,
        isMoveReplicaBalancingRequired);

    DropAlienTables(bundleSnapshot);
    bundleSnapshot->BannedReplicaClusters = replicaClustersToIgnore;

    auto config = Config_.Acquire();
    if (isReshardReplicaBalancingRequired || isMoveReplicaBalancingRequired) {
        if (!config->UseStatisticsReporter) {
            YT_LOG_ERROR("Cannot balance replicas when statistics reporter is not enabled");
            bundleSnapshot->ReplicaBalancingFetchFailed = true;

            bundleSnapshot->NonFatalError = TError(
                NTabletBalancer::EErrorCode::StatisticsFetchFailed,
                "Replica statistics fetch failed. Please enable statistics reporter");
        } else {
            try {
                FetchReplicaStatistics(
                    bundleSnapshot,
                    allowedReplicaClusters,
                    replicaClustersToIgnore,
                    isReshardReplicaBalancingRequired,
                    isMoveReplicaBalancingRequired);
            } catch (const TErrorException& ex) {
                bundleSnapshot->ReplicaBalancingFetchFailed = true;

                bundleSnapshot->NonFatalError = TError(
                    NTabletBalancer::EErrorCode::StatisticsFetchFailed,
                    "Replica statistics fetch failed")
                    << ex;
            }
        }
    }

    return bundleSnapshot;
}

TBundleSnapshotPtr TBundleState::GetLatestBundleSnapshot(EFetchKind kind) const
{
    YT_ASSERT_SPINLOCK_AFFINITY(Lock_);
    YT_VERIFY(!BundleSnapshots_.empty());

    TInstant latestFetchTime;
    auto result = BundleSnapshots_.back();
    for (const auto& snapshot : BundleSnapshots_) {
        YT_VERIFY(snapshot->Bundle);

        TInstant fetchTime;
        switch (kind) {
            case EFetchKind::State:
                fetchTime = snapshot->StateFetchTime;
                break;
            case EFetchKind::Statistics:
                fetchTime = snapshot->StatisticsFetchTime;
                break;
            case EFetchKind::PerformanceCounters:
                fetchTime = snapshot->PerformanceCountersFetchTime;
                break;
        }

        if (fetchTime > latestFetchTime) {
            latestFetchTime = fetchTime;
            result = snapshot;
        }
    }
    return result;
}

TBundleSnapshotPtr TBundleState::GetLatestSatisfyingSnapshot(
    std::tuple<TInstant, TInstant, TInstant> minFreshnessRequirement) const
{
    YT_ASSERT_SPINLOCK_AFFINITY(Lock_);
    YT_VERIFY(!BundleSnapshots_.empty());

    TBundleSnapshotPtr result;
    auto [stateFetchTime, statisticsFetchTime, performanceCountersFetchTime] = minFreshnessRequirement;
    for (const auto& snapshot : BundleSnapshots_) {
        YT_VERIFY(snapshot->Bundle);

        if (snapshot->StateFetchTime >= stateFetchTime &&
            snapshot->StatisticsFetchTime >= statisticsFetchTime &&
            snapshot->PerformanceCountersFetchTime >= performanceCountersFetchTime)
        {
            result = snapshot;
            stateFetchTime = snapshot->StateFetchTime;
            statisticsFetchTime = snapshot->StatisticsFetchTime;
            performanceCountersFetchTime = snapshot->PerformanceCountersFetchTime;
        }
    }

    return result;
}

TFuture<TBundleTabletBalancerConfigPtr> TBundleState::GetConfig(bool allowStale)
{
    auto dynamicConfig = Config_.Acquire();

    if (allowStale) {
        auto guard = ReaderGuard(Lock_);
        YT_LOG_ALERT_UNLESS(
            BundleConfig_,
            "Bundle state config is not found (BundleConfigFetchTime: %v, HasBundleConfigFuture: %v)",
            BundleConfigFetchTime_,
            static_cast<bool>(BundleConfigFuture_));
        return MakeFuture(BundleConfig_);
    }

    auto guard = WriterGuard(Lock_);

    auto now = Now();
    if (now <= BundleConfigFetchTime_ + dynamicConfig->ConfigFreshnessTime) {
        return MakeFuture(BundleConfig_);
    }

    if (!BundleConfigFuture_) {
        YT_LOG_DEBUG("Planning to fetch bundle config due to a direct request");
        BundleConfigFuture_ = BIND(&TBundleState::UpdateConfig, MakeStrong(this))
            .AsyncVia(FetcherInvoker_)
            .Run();
    }
    return BundleConfigFuture_;
}

ETabletCellHealth TBundleState::GetHealth() const
{
    auto guard = ReaderGuard(Lock_);
    return Health_;
}

std::vector<TTabletActionId> TBundleState::GetUnfinishedActions() const
{
    auto guard = ReaderGuard(Lock_);
    return UnfinishedActions_;
}

void TBundleState::FetchState()
{
    YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

    auto config = Config_.Acquire();

    auto writerGuard = WriterGuard(Lock_);
    auto now = Now();

    YT_LOG_DEBUG("Started to plan bundle state provider fetches (Config: %v, "
        "HasStateFuture: %v, HasStatisticsFuture: %v, HasPerformanceCountersFuture: %v)",
        ConvertToYsonString(config, NYson::EYsonFormat::Text),
        static_cast<bool>(UpdateStateFuture_),
        static_cast<bool>(UpdateStatisticsFuture_),
        static_cast<bool>(UpdatePerformanceCountersFuture_));

    if ((config->StateFetchPeriod &&
         config->StatisticsFetchPeriod &&
         config->StateFetchPeriod < config->StatisticsFetchPeriod) ||
        (config->StatisticsFetchPeriod &&
         config->PerformanceCountersFetchPeriod &&
         config->StatisticsFetchPeriod < config->PerformanceCountersFetchPeriod))
    {
        YT_LOG_ALERT(
            "Incorrect bundle state config. The following condition must be met: "
            "\"performance_counters_fetch_period\" <= \"statistics_fetch_period\" <= \"state_fetch_period\" "
            "(StateFetchPeriod: %v, StatisticsFetchPeriod: %v, PerformanceCountersFetchPeriod: %v)",
            config->StateFetchPeriod,
            config->StatisticsFetchPeriod,
            config->PerformanceCountersFetchPeriod);

        return;
    }

    auto needToUpdateAnything = [&] () {
        for (const auto& snapshot : BundleSnapshots_) {
            bool isFirstIteration = !static_cast<bool>(BundleSnapshots_.back()->StateFetchTime);
            YT_LOG_DEBUG("Examined existing bundle snapshot fetch times "
                "(StateFetchTime: %v, StatisticsFetchTime: %v, PerformanceCountersFetchTime: %v, IsFirstIteration: %v)",
                snapshot->StateFetchTime,
                snapshot->StatisticsFetchTime,
                snapshot->PerformanceCountersFetchTime,
                isFirstIteration);

            if (isFirstIteration && !config->StateFetchPeriod) {
                return false;
            }

            if ((!config->StateFetchPeriod || now <= snapshot->StateFetchTime + *config->StateFetchPeriod) &&
                (!config->StatisticsFetchPeriod || now <= snapshot->StatisticsFetchTime + *config->StatisticsFetchPeriod) &&
                (!config->PerformanceCountersFetchPeriod ||
                 now <= snapshot->PerformanceCountersFetchTime + *config->PerformanceCountersFetchPeriod))
            {
                return false;
            }
        }
        return true;
    };

    auto getFetchKind = [&] () {
        EFetchKind fetchKind = EFetchKind::State;
        for (const auto& snapshot : BundleSnapshots_) {
            if ((!config->StateFetchPeriod || now <= snapshot->StateFetchTime + *config->StateFetchPeriod) &&
                (!config->StatisticsFetchPeriod || now <= snapshot->StatisticsFetchTime + *config->StatisticsFetchPeriod))
            {
                return EFetchKind::PerformanceCounters;
            } else if (!config->StateFetchPeriod || now <= snapshot->StateFetchTime + *config->StateFetchPeriod) {
                fetchKind = EFetchKind::Statistics;
            }
        }
        return fetchKind;
    };

    if (!needToUpdateAnything()) {
        return;
    }

    DropOldBundleSnapshots();

    auto kind = getFetchKind();
    YT_UNUSED_FUTURE(CreateUpdateFutureIfNeeded(kind, /*isDirectRequest*/ false));
}

TBundleTabletBalancerConfigPtr TBundleState::UpdateConfig()
{
    auto dynamicConfig = Config_.Acquire();
    try {
        auto bundle = GetBundleWithAttributes();
        YT_VERIFY(bundle);

        auto bundleAttributes = ParseBundleAttributes(&bundle->Attributes(), /*throwOnError*/ true);

        auto now = Now();
        auto guard = WriterGuard(Lock_);

        if (BundleConfigFetchTime_ < now) {
            BundleConfigFetchTime_ = now;
            BundleConfig_ = bundleAttributes.Config;
            Health_ = bundleAttributes.Health;
            UnfinishedActions_ = bundleAttributes.UnfinishedActions;
        }

        BundleConfigFuture_.Reset();

        return BundleConfig_;
    } catch (const TErrorException& ex) {
        YT_LOG_ERROR(ex, "Failed to update bundle config");

        auto guard = WriterGuard(Lock_);
        BundleConfigFuture_.Reset();

        throw;
    }
}

TBundleSnapshotPtr TBundleState::UpdateState()
{
    auto config = Config_.Acquire();
    try {
        YT_LOG_DEBUG("Started updating bundle state");

        auto bundleSnapshot = DoUpdateState(config);
        YT_VERIFY(bundleSnapshot);

        auto now = Now();
        auto guard = WriterGuard(Lock_);

        bundleSnapshot->StateFetchTime = now;
        bundleSnapshot->StatisticsFetchTime = now;
        bundleSnapshot->PerformanceCountersFetchTime = now;

        if (config->CheckInvariants) {
            CheckBundleSnapshotInvariants(bundleSnapshot);
        }

        BundleSnapshots_.push_back(std::move(bundleSnapshot));

        UpdateStateFuture_.Reset();

        YT_LOG_DEBUG("Finished updating bundle state");
        return GetLatestBundleSnapshot(EFetchKind::State);
    } catch (const TErrorException& ex) {
        YT_LOG_ERROR(ex, "Failed to update bundle state");
        if (ex.Error().FindMatching(NRpc::EErrorCode::RequestQueueSizeLimitExceeded)) {
            Counters_->StateRequestThrottled.Increment(1);
        }

        auto guard = WriterGuard(Lock_);
        UpdateStateFuture_.Reset();

        throw;
    }
}

INodePtr TBundleState::GetBundleWithAttributes() const
{
    auto bundlesOrError = WaitFor(ClusterStateProvider_->GetBundles());

    if (!bundlesOrError.IsOK()) {
        YT_LOG_ERROR(bundlesOrError, "Failed to fetch bundle list");
        bundlesOrError.ThrowOnError();
    }

    auto bundle = [] (const auto& bundles, const auto& bundleName) -> INodePtr {
        for (const auto& bundle : bundles) {
            if (bundle->AsString()->GetValue() == bundleName) {
                return bundle;
            }
        }
        return nullptr;
    } (bundlesOrError.Value()->GetChildren(), Name_);

    if (!bundle) {
        YT_LOG_DEBUG("Bundle is not found in bundle list, it probably was already removed");
        THROW_ERROR_EXCEPTION("Bundle is not found in bundle list");
    }

    return bundle;
}

TBundleSnapshotPtr TBundleState::DoUpdateState(const TBundleStateProviderConfigPtr& config)
{
    auto bundle = GetBundleWithAttributes();
    auto bundleSnapshot = DeepCopyLatestBundleSnapshot(EFetchKind::State);
    InitializeAttributes(&bundle->Attributes(), bundleSnapshot, /*throwOnError*/ true);
    auto cellIds = bundle->Attributes().Get<std::vector<TTabletCellId>>("tablet_cell_ids");

    bool isFirstIteration = false;
    {
        auto guard = ReaderGuard(Lock_);
        isFirstIteration = !static_cast<bool>(BundleSnapshots_.back()->StateFetchTime);
    }

    BuildNewState(config->FetchTabletCellsFromSecondaryMasters, bundleSnapshot, cellIds, isFirstIteration);

    return DoUpdateStatistics(config, bundleSnapshot);
}

TBundleSnapshotPtr TBundleState::DeepCopyLatestBundleSnapshot(EFetchKind kind) const
{
    auto guard = ReaderGuard(Lock_);

    auto bundleSnapshot = New<TBundleSnapshot>();
    auto oldBundleSnapshot = GetLatestBundleSnapshot(kind);

    auto oldBundleGuard = Guard(oldBundleSnapshot->PublishedObjectLock);
    switch (kind) {
        case EFetchKind::State:
            bundleSnapshot->Bundle = oldBundleSnapshot->Bundle->DeepCopy(
                /*copyCells*/ false,
                /*copyTabletsAndStatistics*/ false);
            break;

        case EFetchKind::Statistics:
            bundleSnapshot->Bundle = oldBundleSnapshot->Bundle->DeepCopy(
                /*copyCells*/ true,
                /*copyTabletsAndStatistics=*/ false);
            break;

        case EFetchKind::PerformanceCounters:
            bundleSnapshot->Bundle = oldBundleSnapshot->Bundle->DeepCopy(
                /*copyCells*/ true,
                /*copyTabletsAndStatistics=*/ true);
            break;
    }

    bundleSnapshot->PerformanceCountersKeys = DefaultPerformanceCountersKeys;
    bundleSnapshot->TableRegistry = TableRegistry_;

    bundleSnapshot->StateFetchTime = oldBundleSnapshot->StateFetchTime;
    bundleSnapshot->StatisticsFetchTime = oldBundleSnapshot->StatisticsFetchTime;
    bundleSnapshot->PerformanceCountersFetchTime = oldBundleSnapshot->PerformanceCountersFetchTime;

    return bundleSnapshot;
}

TBundleSnapshotPtr TBundleState::UpdateStatistics()
{
    auto config = Config_.Acquire();
    try {
        YT_LOG_DEBUG("Started updating statistics and performance counters");

        auto bundleSnapshot = DoUpdateStatistics(config);
        YT_VERIFY(bundleSnapshot);

        auto now = Now();
        auto guard = WriterGuard(Lock_);

        bundleSnapshot->StatisticsFetchTime = now;
        bundleSnapshot->PerformanceCountersFetchTime = now;

        if (config->CheckInvariants) {
            CheckBundleSnapshotInvariants(bundleSnapshot);
        }

        BundleSnapshots_.push_back(std::move(bundleSnapshot));

        UpdateStatisticsFuture_.Reset();

        YT_LOG_DEBUG("Finished updating statistics and performance counters");
        return GetLatestBundleSnapshot(EFetchKind::Statistics);
    } catch (const TErrorException& ex) {
        YT_LOG_ERROR(ex, "Failed to update statistics and performance counters");
        if (ex.Error().FindMatching(NRpc::EErrorCode::RequestQueueSizeLimitExceeded)) {
            Counters_->StatisticsRequestThrottled.Increment(1);
        }

        auto guard = WriterGuard(Lock_);
        UpdateStatisticsFuture_.Reset();

        throw;
    }
}

TBundleSnapshotPtr TBundleState::DoUpdateStatistics(
    const TBundleStateProviderConfigPtr& config,
    TBundleSnapshotPtr bundleSnapshot) const
{
    auto nodesOrError = WaitFor(ClusterStateProvider_->GetNodes());

    if (!nodesOrError.IsOK()) {
        YT_LOG_ERROR(nodesOrError, "Failed to fetch node statistics");
        nodesOrError.ThrowOnError();
    }

    auto nodeStatistics = nodesOrError.ValueOrThrow();
    THROW_ERROR_EXCEPTION_IF(
        !nodeStatistics,
        "Failed to get node statistics because node fetch "
        "failed earlier during cluster state provider fetch");

    if (!bundleSnapshot) {
        bundleSnapshot = DeepCopyLatestBundleSnapshot(EFetchKind::Statistics);
    }

    FetchStatistics(bundleSnapshot, nodeStatistics, config);

    if (config->UseStatisticsReporter) {
        bundleSnapshot = DoUpdatePerformanceCounters(config, bundleSnapshot);
    }

    return bundleSnapshot;
}

TBundleSnapshotPtr TBundleState::UpdatePerformanceCounters()
{
    auto config = Config_.Acquire();
    if (!config->UseStatisticsReporter) {
        auto guard = ReaderGuard(Lock_);
        return GetLatestBundleSnapshot(EFetchKind::Statistics);
    }

    try {
        YT_LOG_DEBUG("Started updating performance counters");

        auto bundleSnapshot = DoUpdatePerformanceCounters(config);
        YT_VERIFY(bundleSnapshot);

        auto now = Now();
        auto guard = WriterGuard(Lock_);

        bundleSnapshot->PerformanceCountersFetchTime = now;

        if (config->CheckInvariants) {
            CheckBundleSnapshotInvariants(bundleSnapshot);
        }

        BundleSnapshots_.push_back(std::move(bundleSnapshot));

        UpdatePerformanceCountersFuture_.Reset();

        YT_LOG_DEBUG("Finished updating performance counters");
        return GetLatestBundleSnapshot(EFetchKind::PerformanceCounters);
    } catch (const TErrorException& ex) {
        YT_LOG_ERROR(ex, "Failed to update performance counters");

        auto guard = WriterGuard(Lock_);
        UpdatePerformanceCountersFuture_.Reset();

        throw;
    }
}

void TBundleState::BuildNewState(
    bool fetchTabletCellsFromSecondaryMasters,
    const TBundleSnapshotPtr& bundleSnapshot,
    const std::vector<TTabletCellId>& cellIds,
    bool isFirstIteration)
{
    const auto& bundle = bundleSnapshot->Bundle;
    THashMap<TTabletCellId, TTabletCellInfo> tabletCells;

    YT_LOG_DEBUG("Started fetching tablet cells (CellCount: %v)", cellIds.size());
    Counters_->TabletCellTabletsRequestCount.Increment(cellIds.size());

    auto secondaryCellTags = Client_->GetNativeConnection()->GetSecondaryMasterCellTags();
    if (fetchTabletCellsFromSecondaryMasters && !secondaryCellTags.empty()) {
        tabletCells = FetchTabletCells(cellIds, secondaryCellTags);
    } else {
        tabletCells = FetchTabletCells(cellIds, {Client_->GetNativeConnection()->GetPrimaryMasterCellTag()});
    }
    YT_LOG_DEBUG("Finished fetching tablet cells");

    THashSet<TTableId> newTableIds;
    for (const auto& [id, info] : tabletCells) {
        for (auto [tabletId, tableId] : info.TabletToTableId) {
            if (auto tableIt = bundle->Tables.find(tableId); tableIt == bundle->Tables.end()) {
                // A new table has been found.
                newTableIds.insert(tableId);
            }
        }
    }

    YT_VERIFY(bundle->Tablets.empty());
    YT_VERIFY(bundle->TabletCells.empty());

    YT_LOG_DEBUG("Started fetching basic table attributes (NewTableCount: %v)", newTableIds.size());
    Counters_->BasicTableAttributesRequestCount.Increment(newTableIds.size());
    auto tableInfos = FetchBasicTableAttributes(newTableIds, bundle.Get());
    YT_LOG_DEBUG("Finished fetching basic table attributes (NewTableCount: %v)", tableInfos.size());

    for (auto& [tableId, tableInfo] : tableInfos) {
        YT_LOG_DEBUG_UNLESS(!isFirstIteration,
            "New table has been found (TableId: %v, TablePath: %v)",
            tableId,
            tableInfo->Path);

        EmplaceOrCrash(bundle->Tables, tableId, std::move(tableInfo));
    }

    for (const auto& [cellId, tabletCellInfo] : tabletCells) {
        EmplaceOrCrash(bundle->TabletCells, cellId, tabletCellInfo.TabletCell);
    }
}

bool TBundleState::HasReplicaBalancingGroups(const TTabletCellBundlePtr& bundle) const
{
    for (const auto& [group, config] : bundle->Config->Groups) {
        if (config->Type == EBalancingType::Parameterized && !config->Parameterized->ReplicaClusters.empty()) {
            return true;
        }
    }
    return false;
}

bool TBundleState::IsReplicaBalancingEnabled(
    const TTabletCellBundlePtr& bundle,
    const THashSet<TGroupName>& groupNames,
    std::function<bool(const TTableTabletBalancerConfigPtr&)> isBalancingEnabled) const
{
    if (!HasReplicaBalancingGroups(bundle)) {
        return false;
    }

    for (const auto& [id, table] : bundle->Tables) {
        if (!isBalancingEnabled(table->TableConfig)) {
            continue;
        }

        auto groupName = table->GetBalancingGroup();
        if (!groupName) {
            continue;
        }

        if (!groupNames.contains(*groupName)) {
            continue;
        }

        const auto& groupConfig = GetOrCrash(bundle->Config->Groups, *groupName);
        if (groupConfig->Type == EBalancingType::Parameterized && !groupConfig->Parameterized->ReplicaClusters.empty()) {
            return true;
        }
    }
    return false;
}

void TBundleState::FetchStatistics(
    TBundleSnapshotPtr bundleSnapshot,
    const IListNodePtr& nodeStatistics,
    const TBundleStateProviderConfigPtr& config) const
{
    const auto& bundle = bundleSnapshot->Bundle;
    YT_LOG_DEBUG("Started fetching actual table settings (TableCount: %v)", bundle->Tables.size());
    Counters_->ActualTableSettingsRequestCount.Increment(bundle->Tables.size());

    THashSet<TTableId> tableIdsToFetchSettings;
    for (const auto& [id, table] : bundle->Tables) {
        InsertOrCrash(tableIdsToFetchSettings, id);
    }

    auto tableIdToSettings = FetchActualTableSettings(
        Client_,
        tableIdsToFetchSettings,
        BuildTableToCellTagMapping(bundle->Tables),
        MasterRequestThrottler_);
    YT_LOG_DEBUG("Finished fetching actual table settings (TableCount: %v)", tableIdToSettings.size());

    THashSet<TTableId> tableIds;
    for (const auto& [id, info] : tableIdToSettings) {
        EmplaceOrCrash(tableIds, id);
    }
    DropMissingKeys(bundle->Tables, tableIds);

    THashSet<TTableId> tableIdsToFetch;
    THashSet<TTableId> tableIdsToFetchPivotKeys;
    THashSet<TTableId> tableIdsWithParameterizedBalancing;
    for (auto& [tableId, tableSettings] : tableIdToSettings) {
        const auto& table = GetOrCrash(bundle->Tables, tableId);
        table->Dynamic = tableSettings.Dynamic;
        table->TableConfig = tableSettings.Config;
        table->InMemoryMode = tableSettings.InMemoryMode;
        table->UpstreamReplicaId = tableSettings.UpstreamReplicaId;

        // Remove all tablets and write again (with statistics and other parameters).
        // This allows you to overwrite indexes correctly (Tablets[index].Index == index) and remove old tablets.
        // This must be done here because some tables may be removed before fetching @tablets attribute.
        table->Tablets.clear();

        if (IsTableBalancingEnabled(tableSettings)) {
            InsertOrCrash(tableIdsToFetch, tableId);

            if (IsTableReplicaBalancingEnabled(bundle->Config, tableSettings)) {
                InsertOrCrash(tableIdsToFetchPivotKeys, tableId);
            }

            if (table->IsParameterizedMoveBalancingEnabled() ||
                table->IsParameterizedReshardBalancingEnabled(/*enableParameterizedReshardByDefault*/ true))
            {
                InsertOrCrash(tableIdsWithParameterizedBalancing, tableId);
            }
        }
    }

    bundleSnapshot->PerformanceCountersKeys = DefaultPerformanceCountersKeys;

    YT_LOG_DEBUG("Started fetching table statistics (TableCount: %v)", tableIdsToFetch.size());
    Counters_->TableStatisticsRequestCount.Increment(tableIdsToFetch.size());

    auto tableIdToStatistics = FetchTableStatistics(
        Client_,
        tableIdsToFetch,
        tableIdsToFetchPivotKeys,
        BuildTableToCellTagMapping(bundle->Tables),
        MasterRequestThrottler_,
        /*fetchPerformanceCounters*/ !config->UseStatisticsReporter,
        /*parameterizedBalancingEnabledDefault*/ false,
        tableIdsWithParameterizedBalancing);

    YT_LOG_DEBUG("Finished fetching table statistics (TableCount: %v)", tableIdToStatistics.size());

    YT_VERIFY(bundle->Tablets.empty());
    for (const auto& [id, cell] : bundle->TabletCells) {
        // Not filled yet.
        YT_VERIFY(cell->Tablets.empty());
    }

    auto missingTables = tableIdsToFetch;

    THashSet<TTableId> tablesFromAnotherBundle;
    for (auto& [tableId, statistics] : tableIdToStatistics) {
        auto& table = GetOrCrash(bundle->Tables, tableId);
        YT_VERIFY(table->Tablets.empty());
        SetTableStatistics(table, statistics);

        for (auto& tabletResponse : statistics.Tablets) {
            auto tablet = New<TTablet>(tabletResponse.TabletId, table.Get());
            EmplaceOrCrash(bundle->Tablets, tabletResponse.TabletId, tablet);

            if (tabletResponse.CellId) {
                // Will fail if this is a new cell created since the last bundle/@tablet_cell_ids request.
                // Or if the table has been moved from one bundle to another.
                // In this case, it's okay to skip one iteration.

                auto cellIt = bundle->TabletCells.find(tabletResponse.CellId);
                if (cellIt == bundle->TabletCells.end()) {
                    if (auto [it, inserted] = tablesFromAnotherBundle.insert(tableId); inserted) {
                        YT_LOG_DEBUG("Table from another bundle was found (TableId: %v, TablePath: %v)",
                            tableId,
                            table->Path);
                    }
                    break;
                }

                EmplaceOrCrash(cellIt->second->Tablets, tablet->Id, tablet);
                tablet->Cell = cellIt->second;
            } else {
                YT_VERIFY(tabletResponse.State == ETabletState::Unmounted);
            }

            FillTabletWithStatistics(tablet, tabletResponse);

            YT_VERIFY(tablet->Index == std::ssize(table->Tablets));

            table->Tablets.push_back(tablet);
        }

        table->PivotKeys = std::move(statistics.PivotKeys);
        EraseOrCrash(missingTables, tableId);
    }

    RemoveTablesFromBundle(bundleSnapshot, missingTables);

    RemoveTablesFromBundle(bundleSnapshot, tablesFromAnotherBundle);

    THashSet<TTabletId> tabletIds;
    for (const auto& [tableId, table] : bundle->Tables) {
        for (const auto& tablet : table->Tablets) {
            InsertOrCrash(tabletIds, tablet->Id);
        }
    }

    YT_VERIFY(DropAndReturnMissingKeys(bundle->Tablets, tabletIds).empty());

    THashSet<TNodeAddress> addresses;
    for (const auto& [id, cell] : bundle->TabletCells) {
        if (cell->NodeAddress) {
            addresses.insert(*cell->NodeAddress);
        }
    }

    YT_VERIFY(bundle->NodeStatistics.empty());
    bundle->NodeStatistics = GetNodeStatistics(nodeStatistics, addresses);
}

void TBundleState::FillTabletWithStatistics(
    const TTabletPtr& tablet,
    TTableStatisticsResponse::TTabletStatisticsResponse& tabletResponse) const
{
    tablet->Index = tabletResponse.Index;
    tablet->Statistics = std::move(tabletResponse.Statistics);
    tablet->State = tabletResponse.State;
    tablet->MountTime = tabletResponse.MountTime;

    Visit(std::move(tabletResponse.PerformanceCounters),
        [&] (auto&& performanceCounters) {
            tablet->PerformanceCounters = std::move(performanceCounters);
        });
}

void TBundleState::FetchReplicaStatistics(
    const TBundleSnapshotPtr& bundleSnapshot,
    const THashSet<std::string>& allowedReplicaClusters,
    const THashSet<std::string>& replicaClustersToIgnore,
    bool fetchReshard,
    bool fetchMove)
{
    YT_LOG_DEBUG("Collecting replica balancing major and minor tables (FetchReshard: %v, FetchMove: %v)",
        fetchReshard,
        fetchMove);

    auto majorTableIds = GetReplicaBalancingMajorTables(bundleSnapshot->Bundle);
    auto minorTablePaths = GetReplicaBalancingMinorTables(
        majorTableIds,
        bundleSnapshot->Bundle->Tables,
        SelfClusterName_,
        Logger,
        bundleSnapshot->Bundle->Config->EnableVerboseLogging);

    YT_LOG_DEBUG("Collected replica balancing major and minor tables (MajorTableCount: %v, MinorTableCount: %v)",
        std::ssize(majorTableIds),
        std::ssize(minorTablePaths));

    auto perClusterMinorToMajorTables = GetReplicaBalancingMinorToMajorTables(
        majorTableIds,
        bundleSnapshot->Bundle->Tables,
        SelfClusterName_);

    for (const auto& [cluster, tablePaths] : minorTablePaths) {
        if (!allowedReplicaClusters.contains(cluster)) {
            THROW_ERROR_EXCEPTION("Table replicas from cluster %Qv are not allowed",
                cluster)
                << TErrorAttribute("allowed_replica_clusters", allowedReplicaClusters);
        }

        if (replicaClustersToIgnore.contains(cluster)) {
            YT_LOG_DEBUG("Skipping replica cluster that is banned on metacluster (Cluster: %v)", cluster);
            continue;
        }

        auto client = ClientDirectory_->GetClientOrThrow(cluster);
        YT_LOG_DEBUG("Started resolving alien table paths (ClusterName: %v, TableCount: %v)",
            cluster,
            std::ssize(tablePaths));
        auto tableResponses = ResolveTablePaths(client, tablePaths);
        YT_LOG_DEBUG("Finished resolving alien table paths (ClusterName: %v)", cluster);

        THashMap<TTableId, TAlienTablePtr> alienTables;
        THashMap<TTableId, TCellTag> tableIdToCellTag;
        YT_VERIFY(std::ssize(tableResponses) == std::ssize(tablePaths));
        for (int index = 0; index < std::ssize(tableResponses); ++index) {
            const auto& table = tableResponses[index];
            if (table.Id) {
                YT_VERIFY(table.CellTag);
                EmplaceOrCrash(
                    bundleSnapshot->AlienTablePaths,
                    TBundleSnapshot::TAlienTableTag{cluster, tablePaths[index]}, table.Id);
                EmplaceOrCrash(alienTables, table.Id, New<TAlienTable>(
                    tablePaths[index],
                    table.Id,
                    table.CellTag.value(),
                    table.UpstreamReplicaId));
                EmplaceOrCrash(tableIdToCellTag, table.Id, table.CellTag.value());
            }
        }

        YT_LOG_DEBUG("Resolved alien table paths (ClusterName: %v, TableCount: %v)",
            cluster,
            std::ssize(alienTables));

        auto tableIdsToFetchStatistics = GetKeySet(alienTables);

        YT_LOG_DEBUG("Started fetching alien table statistics (ClusterName: %v, TableCount: %v)",
            cluster,
            tableIdsToFetchStatistics.size());
        Counters_->TableStatisticsRequestCount.Increment(tableIdsToFetchStatistics.size());

        auto tableIdToStatistics = FetchTableStatistics(
            client,
            tableIdsToFetchStatistics,
            /*tableIdsToFetchPivotKeys*/ tableIdsToFetchStatistics,
            tableIdToCellTag,
            MasterRequestThrottler_,
            /*fetchPerformanceCounters*/ false,
            /*parameterizedBalancingEnabledDefault*/ true);

        YT_LOG_DEBUG("Finished fetching alien table statistics (ClusterName: %v, TableCount: %v)",
            cluster,
            tableIdToStatistics.size());

        if (fetchMove) {
            FetchPerformanceCountersFromAlienTable(bundleSnapshot, client, &tableIdToStatistics, cluster);
        }

        const auto& minorToMajorTables = GetOrCrash(perClusterMinorToMajorTables, cluster);
        for (auto& [tableId, statistics] : tableIdToStatistics) {
            auto table = GetOrCrash(alienTables, tableId);
            for (auto& tabletResponse : statistics.Tablets) {
                auto tablet = New<TTablet>(tabletResponse.TabletId, /*table*/ nullptr);
                FillTabletWithStatistics(tablet, tabletResponse);

                YT_VERIFY(tablet->Index == std::ssize(table->Tablets));

                table->Tablets.push_back(std::move(tablet));
            }
            table->PivotKeys = std::move(statistics.PivotKeys);

            THROW_ERROR_EXCEPTION_IF(std::ssize(table->PivotKeys) != std::ssize(table->Tablets),
                "Not all pivot keys of table %v are known. Fetched %v tablets and %v pivot keys",
                tableId,
                std::ssize(table->Tablets),
                std::ssize(table->PivotKeys));

            const auto& majorTables = GetOrCrash(minorToMajorTables, table->Path);
            for (const auto& majorTableId : majorTables) {
                auto majorTable = GetOrCrash(bundleSnapshot->Bundle->Tables, majorTableId);
                majorTable->AlienTables[cluster].push_back(table);
            }

            EmplaceOrCrash(bundleSnapshot->AlienTables, table->Id, std::move(table));
        }
    }

    if (fetchReshard) {
        YT_LOG_DEBUG("Started fetching replica table modes (MajorTableCount: %v)", std::ssize(majorTableIds));
        FetchReplicaModes(bundleSnapshot, majorTableIds, allowedReplicaClusters, replicaClustersToIgnore);
        YT_LOG_DEBUG("Finished fetching replica table modes");
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
    const std::vector<TTabletCellId>& cellIds,
    const NObjectClient::TCellTagList& cellTags) const
{
    THashMap<TCellTag, TCellTagBatch> batchRequests;

    static const std::vector<std::string> attributeKeys{"tablets", "status", "total_statistics", "peers", "tablet_cell_life_stage"};
    for (auto cellTag : cellTags) {
        auto proxy = CreateObjectServiceReadProxy(
            Client_,
            EMasterChannelKind::Follower,
            cellTag);
        auto it = EmplaceOrCrash(batchRequests, cellTag, TCellTagBatch{proxy.ExecuteBatch(), {}});

        for (auto cellId : cellIds) {
            auto req = TYPathProxy::Get(FromObjectId(cellId) + "/@");
            ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
            it->second.Request->AddRequest(req, ToString(cellId));
        }
    }

    ExecuteRequestsToCellTags(&batchRequests, MasterRequestThrottler_);

    THashMap<TTabletCellId, TTabletCellInfo> tabletCells;
    for (auto cellTag : cellTags) {
        for (auto cellId : cellIds) {
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
        } catch (const TErrorException& ex) {
            YT_LOG_ERROR(ex, "Failed to get \"statistics\" or \"tablet_slots\" attribute for node %Qv",
                address);
        }
    }

    if (std::ssize(nodeStatistics) != std::ssize(addresses)) {
        THROW_ERROR_EXCEPTION(
            "Failed to fetch statistics for some nodes of bundle %Qv",
            Name_)
            << TErrorAttribute("fetched_count", std::ssize(nodeStatistics))
            << TErrorAttribute("expected_count", std::ssize(addresses));
    }

    return nodeStatistics;
}

THashMap<TTableId, TTablePtr> TBundleState::FetchBasicTableAttributes(
    const THashSet<TTableId>& tableIds,
    TTabletCellBundle* bundle) const
{
    static const std::vector<std::string> attributeKeys{"path", "external", "sorted", "external_cell_tag"};
    auto tableToAttributes = FetchAttributes(Client_, tableIds, attributeKeys, MasterRequestThrottler_);

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
            bundle));
    }

    return tableInfos;
}

THashMap<TTableId, TTableSettings> TBundleState::FetchActualTableSettings(
    const NApi::NNative::IClientPtr& client,
    const THashSet<TTableId>& tableIdsToFetch,
    const THashMap<TTableId, TCellTag>& tableIdToCellTag,
    const IMulticellThrottlerPtr& throttler)
{
    auto cellTagToBatch = FetchTableAttributes(
        client,
        tableIdsToFetch,
        /*tableIdsToFetchPivotKeys*/ {},
        tableIdToCellTag,
        throttler,
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
                .UpstreamReplicaId = FromProto<TTableReplicaId>(attributes.upstream_replica_id()),
            });
        }
    }

    return tableConfigs;
}

THashMap<TTableId, TTableStatisticsResponse> TBundleState::FetchTableStatistics(
    const NApi::NNative::IClientPtr& client,
    const THashSet<TTableId>& tableIds,
    const THashSet<TTableId>& tableIdsToFetchPivotKeys,
    const THashMap<TTableId, TCellTag>& tableIdToCellTag,
    const IMulticellThrottlerPtr& throttler,
    bool fetchPerformanceCounters,
    bool parameterizedBalancingEnabledDefault,
    THashSet<TTableId> tableIdsWithParameterizedBalancing)
{
    // There is no point in tableIdsWithParameterizedBalancing if parameterized balancing is enabled by default.
    YT_VERIFY(!parameterizedBalancingEnabledDefault || tableIdsWithParameterizedBalancing.empty());

    auto cellTagToBatch = FetchTableAttributes(
        client,
        tableIds,
        tableIdsToFetchPivotKeys,
        tableIdToCellTag,
        throttler,
        [fetchPerformanceCounters] (const NTabletClient::TMasterTabletServiceProxy::TReqGetTableBalancingAttributesPtr& request) {
            request->set_fetch_statistics(true);
            if (fetchPerformanceCounters) {
                ToProto(request->mutable_requested_performance_counters(), DefaultPerformanceCountersKeys);
            }
    });

    THashMap<TTableId, TTableStatisticsResponse> tableToStatistics;
    for (const auto& [cellTag, batch] : cellTagToBatch) {
        THROW_ERROR_EXCEPTION_IF_FAILED(
            batch.Response.Get(),
            "Failed to fetch tablets from cell %v",
            cellTag);

        auto responseBatch = batch.Response.Get().ValueOrThrow();
        auto statisticsFieldNames = FromProto<std::vector<std::string>>(responseBatch->statistics_field_names());

        for (int index = 0; index < batch.Request->table_ids_size(); ++index) {
            auto tableId = FromProto<TTableId>(batch.Request->table_ids()[index]);

            const auto& response = responseBatch->tables()[index];
            if (response.tablets_size() == 0) {
                // The table has already been deleted.
                continue;
            }

            bool parameterizedBalancingEnabled = parameterizedBalancingEnabledDefault ||
                tableIdsWithParameterizedBalancing.contains(tableId);

            TTableStatisticsResponse tableStatistics;
            for (const auto& tablet : response.tablets()) {
                tableStatistics.Tablets.push_back(TTableStatisticsResponse::TTabletStatisticsResponse{
                    .Index = tablet.index(),
                    .TabletId = FromProto<TTabletId>(tablet.tablet_id()),
                    .State = FromProto<ETabletState>(tablet.state()),
                    .Statistics = BuildTabletStatistics(
                        tablet.statistics(),
                        statisticsFieldNames,
                        /*saveOriginalNode*/ parameterizedBalancingEnabled),
                });

                if (fetchPerformanceCounters) {
                    tableStatistics.Tablets.back().PerformanceCounters = tablet.performance_counters();
                }

                if (tablet.has_cell_id()) {
                    tableStatistics.Tablets.back().CellId = FromProto<TTabletCellId>(tablet.cell_id());
                }

                if (tablet.has_mount_time()) {
                    tableStatistics.Tablets.back().MountTime = FromProto<TInstant>(tablet.mount_time());
                }
            }

            if (tableIdsToFetchPivotKeys.contains(tableId) && response.pivot_keys_size() > 0) {
                tableStatistics.PivotKeys = FromProto<std::vector<TLegacyOwningKey>>(response.pivot_keys());
            }

            EmplaceOrCrash(tableToStatistics, tableId, std::move(tableStatistics));
        }
    }

    return tableToStatistics;
}

void TBundleState::FetchReplicaModes(
    const TBundleSnapshotPtr& bundleSnapshot,
    const THashSet<TTableId>& majorTableIds,
    const THashSet<std::string>& allowedReplicaClusters,
    const THashSet<std::string>& replicaClustersToIgnore)
{
    using TCellTagWithReplicaType = std::pair<TCellTag, EObjectType>;
    THashMap<TCellTagWithReplicaType, THashSet<TTableReplicaId>> cellTagToReplicaIds;
    THashMap<TTableReplicaId, TTableBase*> replicaIdToTable;

    auto getCellTag = [&] (const auto& table, const auto& cluster) -> TCellTagWithReplicaType {
        auto type = TypeFromId(table->UpstreamReplicaId);
        switch (type) {
            case EObjectType::ChaosTableReplica:
                return {CellTagFromId(table->Id), type};

            case EObjectType::TableReplica:
                return {CellTagFromId(table->UpstreamReplicaId), type};

            default:
                YT_LOG_WARNING(
                    "Upstream replica mode cannot be fetched since it has unexpected type "
                    "(TableId: %v, UpstreamReplicaId: %v, Type: %v, Cluster: %v)",
                    table->Id,
                    table->UpstreamReplicaId,
                    type,
                    cluster);
                return {InvalidCellTag, type};
        }
    };

    for (const auto& tableId : majorTableIds) {
        const auto& table = GetOrCrash(bundleSnapshot->Bundle->Tables, tableId);
        auto [cellTag, replicaType] = getCellTag(table, SelfClusterName_);
        if (cellTag == InvalidCellTag) {
            continue;
        }

        EmplaceOrCrash(replicaIdToTable, table->UpstreamReplicaId, table.Get());
        auto it = cellTagToReplicaIds.emplace(std::pair(cellTag, replicaType), THashSet<TTableReplicaId>{}).first;
        InsertOrCrash(it->second, table->UpstreamReplicaId);

        for (const auto& [cluster, tables] : table->GetReplicaBalancingMinorTables(SelfClusterName_)) {
            for (const auto& minorTablePath : tables) {
                auto minorTableIdIt = bundleSnapshot->AlienTablePaths.find(TBundleSnapshot::TAlienTableTag(cluster, minorTablePath));
                if (minorTableIdIt == bundleSnapshot->AlienTablePaths.end()) {
                    continue;
                }

                auto minorTableIt = bundleSnapshot->AlienTables.find(minorTableIdIt->second);
                if (minorTableIt == bundleSnapshot->AlienTables.end()) {
                    // Alien table attributes or statistics was not fetched successfully.
                    continue;
                }

                const auto& minorTable = minorTableIt->second;
                auto [cellTag, replicaType] = getCellTag(minorTable, cluster);
                if (cellTag == InvalidCellTag) {
                    continue;
                }

                EmplaceOrCrash(replicaIdToTable, minorTable->UpstreamReplicaId, minorTable.Get());
                auto replicaIdsIt = cellTagToReplicaIds.emplace(
                    std::pair(cellTag, replicaType),
                    THashSet<TTableReplicaId>{}).first;
                InsertOrCrash(replicaIdsIt->second, minorTable->UpstreamReplicaId);
            }
        }
    }

    auto fetchReplicaModes = [] (
        const NNative::IClientPtr& localClient,
        const NNative::IClientPtr& client,
        const THashSet<TTableReplicaId>& replicaIds,
        const IMulticellThrottlerPtr& throttler,
        auto replicaType)
    {
        switch (replicaType) {
            case EObjectType::ChaosTableReplica:
                return FetchChaosTableReplicaModes(localClient, replicaIds);

            case EObjectType::TableReplica: {
                static const std::vector<std::string> attributeKeys{"mode"};
                auto tableToAttributes = FetchAttributes(client, replicaIds, attributeKeys, throttler);

                THashMap<TTableReplicaId, ETableReplicaMode> modes;
                for (const auto& [replicaId, attributes] : tableToAttributes) {
                    auto mode = attributes->Get<ETableReplicaMode>("mode");
                    EmplaceOrCrash(modes, replicaId, mode);
                }
                return modes;
            }

            default:
                YT_ABORT();
        }
    };

    for (const auto& [TCellTagWithReplicaType, replicaIds] : cellTagToReplicaIds) {
        auto [cellTag, replicaType] = TCellTagWithReplicaType;
        auto connection = ClusterDirectory_->GetConnectionOrThrow(cellTag);
        auto clusterName = connection->GetClusterName();
        THROW_ERROR_EXCEPTION_IF(!clusterName,
            "Cluster name for cell tag %v not found",
            cellTag);

        if (!allowedReplicaClusters.contains(*clusterName)) {
            THROW_ERROR_EXCEPTION("Table replicas from cluster %Qv are not allowed",
                clusterName)
                << TErrorAttribute("allowed_replica_clusters", allowedReplicaClusters);
        }

        if (replicaClustersToIgnore.contains(*clusterName)) {
            YT_LOG_DEBUG("Skipping replica cluster that is banned on metacluster (Cluster: %v)", *clusterName);
            continue;
        }

        auto client = ClientDirectory_->GetClientOrThrow(*clusterName);

        YT_LOG_DEBUG("Started fetching replica table modes (Cluster: %v, TableCount: %v, CellTag: %v)",
            clusterName,
            replicaIds.size(),
            cellTag);

        auto replicaIdToMode = fetchReplicaModes(Client_, client, replicaIds, MasterRequestThrottler_, replicaType);

        YT_LOG_DEBUG("Finished fetching replica table modes (Cluster: %v, TableCount: %v)",
            clusterName,
            replicaIdToMode.size());

        for (const auto& [replicaId, mode] : replicaIdToMode) {
            auto table = GetOrCrash(replicaIdToTable, replicaId);
            table->ReplicaMode = mode;
        }
    }
}

TBundleSnapshotPtr TBundleState::DoUpdatePerformanceCounters(
    const TBundleStateProviderConfigPtr& config,
    TBundleSnapshotPtr bundleSnapshot) const
{
    THROW_ERROR_EXCEPTION_IF(
        config->StatisticsTablePath.empty(),
        "Cannot update performance counters because \"use_statistics_reporter\" "
        "is set when statistics_table_path is empty");

    if (!bundleSnapshot) {
        bundleSnapshot = DeepCopyLatestBundleSnapshot(EFetchKind::PerformanceCounters);
    }

    THashSet<TTableId> tableIdsToFetch;
    for (const auto& [tableId, table] : bundleSnapshot->Bundle->Tables) {
        if (table->IsParameterizedMoveBalancingEnabled() ||
            table->IsParameterizedReshardBalancingEnabled(
                /*enableParameterizedReshardByDefault*/ true))
        {
            tableIdsToFetch.insert(tableId);
        }
    }

    if (tableIdsToFetch.empty()) {
        return bundleSnapshot;
    }

    YT_LOG_DEBUG("Started fetching table performance counters from archive (TableCount: %v)",
        tableIdsToFetch.size());

    auto [tableToPerformanceCounters, schema] = FetchPerformanceCountersAndSchemaFromTable(
        Client_,
        tableIdsToFetch,
        config->StatisticsTablePath);
    YT_VERIFY(schema);

    bundleSnapshot->PerformanceCountersKeys.insert(
        bundleSnapshot->PerformanceCountersKeys.end(),
        AdditionalPerformanceCountersKeys.begin(),
        AdditionalPerformanceCountersKeys.end());

    auto performanceCountersColumnCount = GetPerformanceCountersColumnCount(schema);
    YT_LOG_DEBUG_IF(performanceCountersColumnCount != std::ssize(bundleSnapshot->PerformanceCountersKeys),
        "Statistics reporter schema and current tablet balancer version has different performance counter keys "
        "(StatisticsReporterPerformanceCountersColumnCount: %v, PerformanceCountersKeyCount: %v)",
        performanceCountersColumnCount,
        std::ssize(bundleSnapshot->PerformanceCountersKeys));

    bundleSnapshot->Bundle->PerformanceCountersTableSchema = std::move(schema);

    THashSet<TTableId> unfetchedTableIds;
    for (auto tableId : tableIdsToFetch) {
        auto performanceCountersIt = tableToPerformanceCounters.find(tableId);
        if (performanceCountersIt == tableToPerformanceCounters.end()) {
            unfetchedTableIds.insert(tableId);
            continue;
        }

        auto& tabletToPerformanceCounters = performanceCountersIt->second;
        const auto& it = bundleSnapshot->Bundle->Tables.find(tableId);
        if (it == bundleSnapshot->Bundle->Tables.end()) {
            continue;
        }

        for (auto& tablet : it->second->Tablets) {
            // There might be no such tablet in select result if the tablet has been unmounted for a long time
            // and performance counters have already been removed by ttl from the table.
            if (auto performanceCountersIt = tabletToPerformanceCounters.find(tablet->Id);
                performanceCountersIt != tabletToPerformanceCounters.end())
            {
                tablet->PerformanceCounters = std::move(performanceCountersIt->second);
            } else if (tablet->State != ETabletState::Unmounted) {
                THROW_ERROR_EXCEPTION(
                    "Performance counters for tablet %v of table %v were not found in statistics table",
                    tablet->Id,
                    tableId);
            }
        }
    }

    RemoveTablesFromBundle(bundleSnapshot, unfetchedTableIds);

    YT_LOG_DEBUG("Finished fetching table performance counters from archive (TableCount: %v)",
        tableToPerformanceCounters.size());

    return bundleSnapshot;
}

void TBundleState::FetchPerformanceCountersFromAlienTable(
    const TBundleSnapshotPtr& bundleSnapshot,
    const NApi::NNative::IClientPtr& client,
    THashMap<TTableId, TTableStatisticsResponse>* tableIdToStatistics,
    const std::string& cluster)
{
    YT_LOG_DEBUG("Started fetching tablet balancer config (ClusterName: %v)", cluster);
    auto config = FetchTabletBalancerConfig(client);
    YT_LOG_DEBUG("Finished fetching tablet balancer config (ClusterName: %v)", cluster);

    THROW_ERROR_EXCEPTION_IF(!config->UseStatisticsReporter || config->StatisticsTablePath.empty(),
        "Cannot fetch table performance counters from another cluster without statistics table");

    auto tableIdsToFetchCounters = GetKeySet(*tableIdToStatistics);

    YT_LOG_DEBUG("Started fetching alien table performance counters from archive (Cluster: %v, TableCount: %v)",
        cluster,
        tableIdsToFetchCounters.size());

    auto [tableToPerformanceCounters, tableSchema] = FetchPerformanceCountersAndSchemaFromTable(
        client,
        tableIdsToFetchCounters,
        config->StatisticsTablePath);
    YT_VERIFY(tableSchema);

    YT_LOG_DEBUG("Finished fetching alien table performance counters from archive (Cluster: %v, TableCount: %v)",
        cluster,
        tableToPerformanceCounters.size());

    auto performanceCountersColumnCount = GetPerformanceCountersColumnCount(tableSchema);
    YT_LOG_DEBUG_IF(performanceCountersColumnCount != std::ssize(bundleSnapshot->PerformanceCountersKeys),
        "Statistics reporter schema and current tablet balancer version has different performance counter keys "
        "(StatisticsReporterPerformanceCountersColumnCount: %v, PerformanceCountersKeyCount: %v, Cluster: %v)",
        performanceCountersColumnCount,
        std::ssize(bundleSnapshot->PerformanceCountersKeys),
        cluster);

    bundleSnapshot->Bundle->PerClusterPerformanceCountersTableSchemas[cluster] = std::move(tableSchema);

    YT_LOG_DEBUG("Started filling alien table statistics and performance counters (Cluster: %v, TableCount: %v)",
        cluster,
        tableToPerformanceCounters.size());

    FillPerformanceCounters(tableIdToStatistics, tableToPerformanceCounters);

    auto aliveTables = GetKeySet(tableToPerformanceCounters);
    DropMissingKeys(*tableIdToStatistics, aliveTables);

    YT_LOG_DEBUG("Finished filling alien table statistics and performance counters (Cluster: %v, TableCount: %v)",
        cluster,
        tableToPerformanceCounters.size());
}

void TBundleState::FillPerformanceCounters(
    THashMap<TTableId, TTableStatisticsResponse>* tableIdToStatistics,
    const TTablePerformanceCountersMap& tableToPerformanceCounters) const
{
    for (auto& [tableId, statistics] : *tableIdToStatistics) {
        auto it = tableToPerformanceCounters.find(tableId);
        if (it == tableToPerformanceCounters.end()) {
            // The table may not be fetched or
            // the table has already been deleted or
            // the table is too new, then we just pretend that we haven’t seen it yet.
            continue;
        }

        auto& tabletToPerformanceCounters = it->second;
        for (auto& tablet : statistics.Tablets) {
            // There might be no such tablet in select result if the tablet has been unmounted for a long time
            // and performance counters have already been removed by ttl from the table.
            if (auto performanceCountersIt = tabletToPerformanceCounters.find(tablet.TabletId);
                performanceCountersIt != tabletToPerformanceCounters.end())
            {
                tablet.PerformanceCounters = std::move(performanceCountersIt->second);
            } else if (tablet.State != ETabletState::Unmounted) {
                THROW_ERROR_EXCEPTION(
                    "Performance counters for tablet %v of table %v were not found in statistics table",
                    tablet.TabletId,
                    tableId);
            }
        }
    }
}

THashSet<TTableId> TBundleState::GetReplicaBalancingMajorTables(const TTabletCellBundlePtr& bundle) const
{
    THashSet<TTableId> majorTableIds;
    for (const auto& [id, table] : bundle->Tables) {
        if (table->IsParameterizedMoveBalancingEnabled() ||
            table->IsParameterizedReshardBalancingEnabled(
                /*enableParameterizedReshardByDefault*/ true,
                /*desiredTabletCountRequired*/ false))
        {
            const auto& groupConfig = GetOrCrash(bundle->Config->Groups, *table->GetBalancingGroup());
            const auto& replicaClusters = groupConfig->Parameterized->ReplicaClusters;
            if (replicaClusters.empty()) {
                continue;
            }

            if (std::ranges::count(replicaClusters, SelfClusterName_) == 0) {
                THROW_ERROR_EXCEPTION(
                    "Wrong parameterized replica balancing configuration. "
                    "The list of replica clusters must contain current cluster name")
                    << TErrorAttribute("replica_clusters", replicaClusters)
                    << TErrorAttribute("current_cluster_name", SelfClusterName_);
            }
            majorTableIds.insert(id);
        }
    }

    return majorTableIds;
}

////////////////////////////////////////////////////////////////////////////////

IBundleStatePtr CreateBundleState(
    TString name,
    IBootstrap* bootstrap,
    IInvokerPtr fetcherInvoker,
    IInvokerPtr controlInvoker,
    TBundleStateProviderConfigPtr config,
    IClusterStateProviderPtr clusterStateProvider,
    IMulticellThrottlerPtr throttler,
    const IAttributeDictionary* initialAttributes)
{
    return New<TBundleState>(
        name,
        bootstrap,
        fetcherInvoker,
        controlInvoker,
        config,
        clusterStateProvider,
        throttler,
        initialAttributes);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
