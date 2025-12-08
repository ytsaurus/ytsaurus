#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/public.h>
#include <yt/yt/server/lib/tablet_balancer/table.h>
#include <yt/yt/server/lib/tablet_balancer/tablet.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell_bundle.h>

#include <yt/yt/server/lib/tablet_node/performance_counters.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TBundleProfilingCounters
    : public TRefCounted
{
    NProfiling::TCounter TabletCellTabletsRequestCount;
    NProfiling::TCounter BasicTableAttributesRequestCount;
    NProfiling::TCounter ActualTableSettingsRequestCount;
    NProfiling::TCounter TableStatisticsRequestCount;

    explicit TBundleProfilingCounters(const NProfiling::TProfiler& profiler);
};

DEFINE_REFCOUNTED_TYPE(TBundleProfilingCounters)

// It is not an actual copy so far. Table objects will be reused during next fetch iteration
// and list of tablets, pivot keys and so on will be changed.
struct TBundleSnapshot final
{
    TTabletCellBundlePtr Bundle;
    bool ReplicaBalancingFetchFailed = false;
    std::vector<std::string> PerformanceCountersKeys;
    TError NonFatalError;
    TTableRegistryPtr TableRegistry;

    using TAlienTableTag = std::tuple<TString, NYPath::TYPath>;
    THashMap<TAlienTableTag, TTableId> AlienTablePaths;
    THashMap<TTableId, TAlienTablePtr> AlienTables;
};

DEFINE_REFCOUNTED_TYPE(TBundleSnapshot)

////////////////////////////////////////////////////////////////////////////////

class TBundleState
    : public TRefCounted
{
public:
    using TPerClusterPerformanceCountersTableSchemas = THashMap<TClusterName, NQueryClient::TTableSchemaPtr>;

    DEFINE_BYVAL_RO_PROPERTY(NTabletClient::ETabletCellHealth, Health);
    DEFINE_BYVAL_RW_PROPERTY(bool, HasUntrackedUnfinishedActions, false);

public:
    TBundleState(
        TString name,
        NApi::NNative::IClientPtr client,
        NHiveClient::TClientDirectoryPtr clientDirectory,
        NHiveClient::TClusterDirectoryPtr clusterDirectory,
        IInvokerPtr invoker,
        std::string clusterName);

    void UpdateBundleAttributes(const NYTree::IAttributeDictionary* attributes);

    bool IsParameterizedBalancingEnabled() const;
    TFuture<TBundleSnapshotPtr> GetBundleSnapshot(
        const TTabletBalancerDynamicConfigPtr& dynamicConfig,
        const NYTree::IListNodePtr& nodeStatistics,
        const THashSet<TGroupName>& groupsForMoveBalancing,
        const THashSet<TGroupName>& groupsForReshardBalancing,
        const THashSet<std::string>& allowedReplicaClusters,
        int iterationIndex);

    TBundleTabletBalancerConfigPtr GetConfig() const;

    TTableProfilingCounters& GetProfilingCounters(
        const TTable* table,
        const TString& groupName);

private:
    struct TTabletCellInfo
    {
        TTabletCellPtr TabletCell;
        THashMap<TTabletId, TTableId> TabletToTableId;
    };

    struct TTableSettings
    {
        TTableTabletBalancerConfigPtr Config;
        EInMemoryMode InMemoryMode;
        bool Dynamic;
        NTabletClient::TTableReplicaId UpstreamReplicaId;
    };

    struct TTabletStatisticsResponse
    {
        i64 Index;
        TTabletId TabletId;

        ETabletState State;
        TTabletStatistics Statistics;
        std::variant<
            TTablet::TPerformanceCountersProtoList,
            NTableClient::TUnversionedOwningRow> PerformanceCounters;
        TTabletCellId CellId;
        TInstant MountTime = TInstant::Zero();
    };

    struct TTableStatisticsResponse
    {
        std::vector<TTabletStatisticsResponse> Tablets;
        std::vector<NTableClient::TLegacyOwningKey> PivotKeys;
    };

    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler_;

    const NApi::NNative::IClientPtr Client_;
    const NHiveClient::TClientDirectoryPtr ClientDirectory_;
    const NHiveClient::TClusterDirectoryPtr ClusterDirectory_;
    const IInvokerPtr Invoker_;
    const TTableRegistryPtr TableRegistry_;
    const std::string SelfClusterName_;

    TTabletCellBundlePtr Bundle_;

    TAtomicIntrusivePtr<TBundleStateProviderConfig> Config_;
    std::vector<TTabletCellId> CellIds_;
    TBundleProfilingCountersPtr Counters_;

    std::vector<std::string> PerformanceCountersKeys_;

    void UpdateState(bool fetchTabletCellsFromSecondaryMasters, int iterationIndex);

    THashMap<TTabletCellId, TTabletCellInfo> FetchTabletCells(
        const NObjectClient::TCellTagList& cellTags) const;

    THashMap<TTableId, TTablePtr> FetchBasicTableAttributes(
        const THashSet<TTableId>& tableIds) const;
    THashMap<TNodeAddress, TTabletCellBundle::TNodeStatistics> GetNodeStatistics(
        const NYTree::IListNodePtr& nodeStatistics,
        const THashSet<std::string>& addresses) const;

    TTabletCellInfo TabletCellInfoFromAttributes(
        TTabletCellId cellId,
        const NYTree::IAttributeDictionaryPtr& attributes) const;

    TBundleSnapshotPtr DoGetBundleSnapshot(
        const TTabletBalancerDynamicConfigPtr& dynamicConfig,
        const NYTree::IListNodePtr& nodeStatistics,
        const THashSet<TGroupName>& groupsForMoveBalancing,
        const THashSet<TGroupName>& groupsForReshardBalancing,
        const THashSet<std::string>& allowedReplicaClusters,
        int iterationIndex);

    void FetchStatistics(
        const NYTree::IListNodePtr& nodeStatistics,
        bool useStatisticsReporter,
        const NYPath::TYPath& statisticsTablePath);

    void FetchReplicaStatistics(
        const TBundleSnapshotPtr& bundleSnapshot,
        const THashSet<std::string>& allowedReplicaClusters,
        bool fetchReshard,
        bool fetchMove);

    THashMap<TTableId, TTableSettings> FetchActualTableSettings() const;

    THashMap<TTableId, TTableStatisticsResponse> FetchTableStatistics(
        const NApi::NNative::IClientPtr& client,
        const THashSet<TTableId>& tableIds,
        const THashSet<TTableId>& tableIdsToFetchPivotKeys,
        const THashMap<TTableId, NObjectClient::TCellTag>& tableIdToCellTag,
        bool fetchPerformanceCounters,
        bool parameterizedBalancingEnabledDefault = false) const;

    void FetchReplicaModes(
        const TBundleSnapshotPtr& bundleSnapshot,
        const THashSet<TTableId>& majorTableIds,
        const THashSet<std::string>& allowedReplicaClusters);

    void FetchPerformanceCountersFromTable(
        THashMap<TTableId, TTableStatisticsResponse>* tableIdToStatistics,
        const NYPath::TYPath& statisticsTablePath);

    void FetchPerformanceCountersFromAlienTable(
        const NApi::NNative::IClientPtr& client,
        THashMap<TTableId, TTableStatisticsResponse>* tableIdToStatistics,
        const std::string& cluster);

    void FillPerformanceCounters(
        THashMap<TTableId, TTableStatisticsResponse>* tableIdToStatistics,
        const THashMap<TTableId, THashMap<TTabletId, NTableClient::TUnversionedOwningRow>>& tableToPerformanceCounters) const;
    void FillTabletWithStatistics(const TTabletPtr& tablet, TTabletStatisticsResponse& tabletResponse) const;

    bool IsTableBalancingEnabled(const TTableSettings& table) const;
    bool IsReplicaBalancingEnabled(const TTableSettings& table) const;
    bool IsReplicaBalancingEnabled(
        const THashSet<TGroupName>& groupNames,
        std::function<bool(const TTableTabletBalancerConfigPtr&)> isBalancingEnabled) const;
    bool HasReplicaBalancingGroups() const;

    TTableProfilingCounters InitializeProfilingCounters(
        const TTable* table,
        const TString& groupName) const;

    static void SetTableStatistics(
        const TTablePtr& table,
        const TTableStatisticsResponse& tableStatistics);

    THashSet<TTableId> GetReplicaBalancingMajorTables() const;
};

DEFINE_REFCOUNTED_TYPE(TBundleState)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
