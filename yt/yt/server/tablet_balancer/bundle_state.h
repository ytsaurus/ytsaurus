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

struct IBundleState
    : public TRefCounted
{
    virtual void UpdateBundleAttributes(const NYTree::IAttributeDictionary* attributes) = 0;

    virtual TFuture<TBundleSnapshotPtr> GetBundleSnapshot(
        const TTabletBalancerDynamicConfigPtr& dynamicConfig,
        const NYTree::IListNodePtr& nodeStatistics,
        const THashSet<TGroupName>& groupsForMoveBalancing,
        const THashSet<TGroupName>& groupsForReshardBalancing,
        const THashSet<std::string>& allowedReplicaClusters,
        int iterationIndex) = 0;

    virtual TBundleTabletBalancerConfigPtr GetConfig() const = 0;
    virtual NTabletClient::ETabletCellHealth GetHealth() const = 0;
    virtual std::vector<TTabletActionId> GetUnfinishedActions() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IBundleState)

////////////////////////////////////////////////////////////////////////////////

IBundleStatePtr CreateBundleState(
    TString name,
    IBootstrap* bootstrap,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
