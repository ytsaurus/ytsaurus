#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/hive/config.h>

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableOptions
    : public NYTree::TYsonStruct
{
public:
    bool EnableReplicatedTableTracker;

    std::optional<int> MaxSyncReplicaCount;
    std::optional<int> MinSyncReplicaCount;

    TDuration SyncReplicaLagThreshold;

    // TODO(akozhikhov): We probably do not need these in this per-table config.
    TDuration TabletCellBundleNameTtl;
    TDuration RetryOnFailureInterval;

    bool EnablePreloadStateCheck;
    TDuration IncompletePreloadGracePeriod;

    std::optional<std::vector<TString>> PreferredSyncReplicaClusters;

    std::tuple<int, int> GetEffectiveMinMaxReplicaCount(int replicaCount) const;

    REGISTER_YSON_STRUCT(TReplicatedTableOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicatedTableOptions)

////////////////////////////////////////////////////////////////////////////////

class TDynamicReplicatedTableTrackerConfig
    : public NYTree::TYsonStruct
{
public:
    bool EnableReplicatedTableTracker;

    // COMPAT(akozhikhov): Drop this with old RTT.
    bool UseNewReplicatedTableTracker;

    TDuration CheckPeriod;
    TDuration UpdatePeriod;

    // COMPAT(akozhikhov): Drop this with old RTT.
    TDuration GeneralCheckTimeout;

    NTabletNode::TReplicatorHintConfigPtr ReplicatorHint;
    TAsyncExpiringCacheConfigPtr BundleHealthCache;
    TAsyncExpiringCacheConfigPtr ClusterStateCache;
    NHiveServer::TClusterDirectorySynchronizerConfigPtr ClusterDirectorySynchronizer;

    i64 MaxIterationsWithoutAcceptableBundleHealth;

    i64 MaxActionQueueSize;

    TDuration ClientExpirationTime;

    REGISTER_YSON_STRUCT(TDynamicReplicatedTableTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicReplicatedTableTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
