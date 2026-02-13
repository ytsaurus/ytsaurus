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
    NProfiling::TCounter DirectStateRequest;
    NProfiling::TCounter DirectStatisticsRequest;
    NProfiling::TCounter DirectPerformanceCountersRequest;
    NProfiling::TCounter StateRequestThrottled;
    NProfiling::TCounter StatisticsRequestThrottled;

    explicit TBundleProfilingCounters(const NProfiling::TProfiler& profiler);
};

DEFINE_REFCOUNTED_TYPE(TBundleProfilingCounters)

struct TBundleSnapshot final
{
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, PublishedObjectLock);

    TTabletCellBundlePtr Bundle;
    bool ReplicaBalancingFetchFailed = false;
    std::vector<std::string> PerformanceCountersKeys;
    TError NonFatalError;
    TTableRegistryPtr TableRegistry;

    using TAlienTableTag = std::tuple<TString, NYPath::TYPath>;
    THashMap<TAlienTableTag, TTableId> AlienTablePaths;
    THashMap<TTableId, TAlienTablePtr> AlienTables;
    THashSet<std::string> BannedReplicaClusters;

    TInstant StateFetchTime;
    TInstant StatisticsFetchTime;
    TInstant PerformanceCountersFetchTime;
};

DEFINE_REFCOUNTED_TYPE(TBundleSnapshot)

////////////////////////////////////////////////////////////////////////////////

struct IBundleState
    : public TRefCounted
{
    virtual TFuture<TBundleSnapshotPtr> GetBundleSnapshot() = 0;
    virtual TFuture<TBundleSnapshotPtr> GetBundleSnapshotWithReplicaBalancingStatistics(
        std::tuple<TInstant, TInstant, TInstant> minFreshnessRequirement,
        const THashSet<TGroupName>& groupsForMoveBalancing,
        const THashSet<TGroupName>& groupsForReshardBalancing,
        const THashSet<std::string>& allowedReplicaClusters,
        const THashSet<std::string>& replicaClustersToIgnore) = 0;

    virtual void Start() = 0;
    virtual void Stop() = 0;

    virtual void Reconfigure(TBundleStateProviderConfigPtr config) = 0;

    virtual TFuture<TBundleTabletBalancerConfigPtr> GetConfig(bool allowStale) = 0;
    virtual NTabletClient::ETabletCellHealth GetHealth() const = 0;
    virtual std::vector<TTabletActionId> GetUnfinishedActions() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IBundleState)

////////////////////////////////////////////////////////////////////////////////

IBundleStatePtr CreateBundleState(
    TString name,
    IBootstrap* bootstrap,
    IInvokerPtr fetcherInvoker,
    IInvokerPtr controlInvoker,
    TBundleStateProviderConfigPtr config,
    IClusterStateProviderPtr clusterStateProvider,
    IMulticellThrottlerPtr throttler,
    const NYTree::IAttributeDictionary* initialAttributes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
