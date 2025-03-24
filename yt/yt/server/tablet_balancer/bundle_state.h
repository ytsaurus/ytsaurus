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

struct TTableProfilingCounters
{
    TString GroupName;
    NProfiling::TCounter InMemoryMoves;
    NProfiling::TCounter OrdinaryMoves;
    NProfiling::TCounter TabletMerges;
    NProfiling::TCounter TabletSplits;
    NProfiling::TCounter NonTrivialReshards;
    NProfiling::TCounter ParameterizedMoves;
    NProfiling::TCounter ParameterizedReshardMerges;
    NProfiling::TCounter ParameterizedReshardSplits;
};

struct TBundleProfilingCounters
    : public TRefCounted
{
    NProfiling::TCounter TabletCellTabletsRequestCount;
    NProfiling::TCounter BasicTableAttributesRequestCount;
    NProfiling::TCounter ActualTableSettingsRequestCount;
    NProfiling::TCounter TableStatisticsRequestCount;

    TBundleProfilingCounters(const NProfiling::TProfiler& profiler);
};

DEFINE_REFCOUNTED_TYPE(TBundleProfilingCounters)

////////////////////////////////////////////////////////////////////////////////

class TBundleState
    : public TRefCounted
{
public:
    using TTabletMap = THashMap<TTabletId, TTabletPtr>;
    using TTableProfilingCounterMap = THashMap<TTableId, TTableProfilingCounters>;

    DEFINE_BYREF_RO_PROPERTY(TTabletMap, Tablets);
    DEFINE_BYVAL_RO_PROPERTY(NTabletClient::ETabletCellHealth, Health);
    DEFINE_BYVAL_RO_PROPERTY(TTabletCellBundlePtr, Bundle);
    DEFINE_BYREF_RW_PROPERTY(TTableProfilingCounterMap, ProfilingCounters);
    DEFINE_BYVAL_RW_PROPERTY(bool, HasUntrackedUnfinishedActions, false);
    DEFINE_BYVAL_RO_PROPERTY(NQueryClient::TTableSchemaPtr, PerformanceCountersTableSchema);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TString>, PerformanceCountersKeys);

public:
    TBundleState(
        TString name,
        TTableRegistryPtr tableRegistry,
        NApi::NNative::IClientPtr client,
        IInvokerPtr invoker);

    void UpdateBundleAttributes(
        const NYTree::IAttributeDictionary* attributes);

    bool IsParameterizedBalancingEnabled() const;

    TFuture<void> UpdateState(bool fetchTabletCellsFromSecondaryMasters, int iterationIndex);
    TFuture<void> FetchStatistics(
        const NYTree::IListNodePtr& nodeStatistics,
        bool useStatisticsReporter,
        const NYPath::TYPath& statisticsTablePath);

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

    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler_;

    const NApi::NNative::IClientPtr Client_;
    const IInvokerPtr Invoker_;
    const TTableRegistryPtr TableRegistry_;

    std::vector<TTabletCellId> CellIds_;
    TBundleProfilingCountersPtr Counters_;

    void DoUpdateState(bool fetchTabletCellsFromSecondaryMasters, int iterationIndex);

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

    void DoFetchStatistics(
        const NYTree::IListNodePtr& nodeStatistics,
        bool useStatisticsReporter,
        const NYPath::TYPath& statisticsTablePath);

    THashMap<TTableId, TTableSettings> FetchActualTableSettings() const;
    THashMap<TTableId, std::vector<TTabletStatisticsResponse>> FetchTableStatistics(
        const THashSet<TTableId>& tableIds,
        bool fetchPerformanceCounters) const;
    void FetchPerformanceCountersFromTable(
        THashMap<TTableId, std::vector<TTabletStatisticsResponse>>* tableIdToStatistics,
        const NYPath::TYPath& statisticsTablePath);

    bool IsTableBalancingAllowed(const TTableSettings& table) const;

    TTableProfilingCounters InitializeProfilingCounters(
        const TTable* table,
        const TString& groupName) const;

    static void SetTableStatistics(
        const TTablePtr& table,
        const std::vector<TTabletStatisticsResponse>& tablets);
};

DEFINE_REFCOUNTED_TYPE(TBundleState)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
