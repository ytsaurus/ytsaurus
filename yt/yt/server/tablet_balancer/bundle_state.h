#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/public.h>
#include <yt/yt/server/lib/tablet_balancer/table.h>
#include <yt/yt/server/lib/tablet_balancer/tablet.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell_bundle.h>

#include <yt/yt/server/lib/tablet_server/performance_counters.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TTableProfilingCounters
{
    NProfiling::TCounter InMemoryMoves;
    NProfiling::TCounter OrdinaryMoves;
    NProfiling::TCounter TabletMerges;
    NProfiling::TCounter TabletSplits;
    NProfiling::TCounter NonTrivialReshards;
    NProfiling::TCounter ParameterizedMoves;
};

struct TBundleProfilingCounters
    : public TRefCounted
{
    NProfiling::TCounter TabletCellTabletsRequestCount;
    NProfiling::TCounter BasicTableAttributesRequestCount;
    NProfiling::TCounter ActualTableSettingsRequestCount;
    NProfiling::TCounter TableStatisticsRequestCount;
    NProfiling::TCounter NodeStatisticsRequestCount;

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
    DEFINE_BYVAL_RO_PROPERTY(TTabletCellBundlePtr, Bundle, nullptr);
    DEFINE_BYREF_RW_PROPERTY(TTableProfilingCounterMap, ProfilingCounters);
    DEFINE_BYVAL_RW_PROPERTY(bool, HasUntrackedUnfinishedActions, false);

    static const std::vector<TString> DefaultPerformanceCountersKeys_;

public:
    TBundleState(
        TString name,
        NApi::NNative::IClientPtr client,
        IInvokerPtr invoker);

    void UpdateBundleAttributes(
        const NYTree::IAttributeDictionary* attributes,
        const TString& defaultParameterizedMetric);

    bool IsParameterizedBalancingEnabled() const;

    TFuture<void> UpdateState();
    TFuture<void> FetchStatistics();

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
        TTablet::TPerformanceCountersProtoList PerformanceCounters;
        TTabletCellId CellId;
        TInstant MountTime = TInstant::Zero();
    };

    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler_;

    const NApi::NNative::IClientPtr Client_;
    const IInvokerPtr Invoker_;

    std::vector<TTabletCellId> CellIds_;
    TBundleProfilingCountersPtr Counters_;

    void DoUpdateState();

    THashMap<TTabletCellId, TTabletCellInfo> FetchTabletCells() const;
    THashMap<TTableId, TTablePtr> FetchBasicTableAttributes(
        const THashSet<TTableId>& tableIds) const;
    THashMap<TString, TTabletCellBundle::TNodeMemoryStatistics> FetchNodeStatistics(
        const THashSet<TString>& addresses) const;

    void DoFetchStatistics();

    THashMap<TTableId, TTableSettings> FetchActualTableSettings() const;
    THashMap<TTableId, std::vector<TTabletStatisticsResponse>> FetchTableStatistics(
        const THashSet<TTableId>& tableIds) const;

    bool IsTableBalancingAllowed(const TTableSettings& table) const;

    void InitializeProfilingCounters(const TTablePtr& table);
    static void SetTableStatistics(
        const TTablePtr& table,
        const std::vector<TTabletStatisticsResponse>& tablets);
};

DEFINE_REFCOUNTED_TYPE(TBundleState)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
