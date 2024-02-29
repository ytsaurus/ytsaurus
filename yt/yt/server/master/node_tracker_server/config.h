#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/misc/arithmetic_formula.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

class TNodeDiscoveryManagerConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration UpdatePeriod;
    int PeerCount;
    int MaxPeersPerRack;
    TBooleanFormula NodeTagFilter;

    REGISTER_YSON_STRUCT(TNodeDiscoveryManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNodeDiscoveryManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TNodeTrackerConfig
    : public NYTree::TYsonStruct
{
public:
    // COMPAT(danilalexeev)
    TDuration DefaultNodeTransactionTimeout;
    TDuration DefaultDataNodeLeaseTransactionTimeout;

    REGISTER_YSON_STRUCT(TNodeTrackerConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TNodeTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

class TNodeGroupConfigBase
    : public NYTree::TYsonStruct
{
public:
    int MaxConcurrentNodeRegistrations;

    REGISTER_YSON_STRUCT(TNodeGroupConfigBase);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TNodeGroupConfig
    : public TNodeGroupConfigBase
{
public:
    TBooleanFormula NodeTagFilter;

    REGISTER_YSON_STRUCT(TNodeGroupConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNodeGroupConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicNodeTrackerTestingConfig
    : public NYTree::TYsonStruct
{
public:
    bool DisableDisposalFinishing;

    REGISTER_YSON_STRUCT(TDynamicNodeTrackerTestingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicNodeTrackerTestingConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicNodeTrackerConfig
    : public NYTree::TYsonStruct
{
public:
    static constexpr auto DefaultProfilingPeriod = TDuration::Seconds(10);

    THashMap<TString, TNodeGroupConfigPtr> NodeGroups;

    TDuration TotalNodeStatisticsUpdatePeriod;

    // COMPAT(aleksandra-zh).
    TDuration FullNodeStatesGossipPeriod;
    TDuration NodeStatisticsGossipPeriod;

    int MaxConcurrentNodeRegistrations;
    int MaxConcurrentNodeUnregistrations;

    int MaxConcurrentClusterNodeHeartbeats;
    int MaxConcurrentExecNodeHeartbeats;

    TDuration ForceNodeHeartbeatRequestTimeout;

    TNodeDiscoveryManagerConfigPtr MasterCacheManager;
    TNodeDiscoveryManagerConfigPtr TimestampProviderManager;

    TDynamicNodeTrackerTestingConfigPtr Testing;

    bool EnableStructuredLog;

    // COMPAT(ignat): Drop this after hosts migration.
    bool EnableNodeCpuStatistics;

    TDuration ProfilingPeriod;

    // COMPAT(gritukan): Drop it after 22.3.
    bool UseResourceStatisticsFromClusterNodeHeartbeat;

    // COMPAT(kvk1920)
    bool EnableRealChunkLocations;

    // COMPAT(kvk1920)
    bool ForbidMaintenanceAttributeWrites;

    // COMPAT(aleksandra-zh)
    bool EnablePerLocationNodeDisposal;

    TDuration NodeDisposalTickPeriod;

    TDuration PendingRestartLeaseTimeout;

    TDuration ResetNodePendingRestartMaintenancePeriod;

    int MaxNodesBeingDisposed;

    REGISTER_YSON_STRUCT(TDynamicNodeTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicNodeTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
