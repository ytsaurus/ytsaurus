#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/hive/config.h>

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct TDynamicReplicatedTableTrackerConfig
    : public NYTree::TYsonStruct
{
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
