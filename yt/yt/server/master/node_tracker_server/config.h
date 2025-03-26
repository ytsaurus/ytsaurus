#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/misc/arithmetic_formula.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

struct TNodeDiscoveryManagerConfig
    : public NYTree::TYsonStruct
{
    TDuration UpdatePeriod;
    int PeerCount;
    int MaxPeersPerRack;
    TBooleanFormula NodeTagFilter;

    REGISTER_YSON_STRUCT(TNodeDiscoveryManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNodeDiscoveryManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TNodeTrackerConfig
    : public NYTree::TYsonStruct
{
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

struct TNodeGroupConfig
    : public TNodeGroupConfigBase
{
    TBooleanFormula NodeTagFilter;

    REGISTER_YSON_STRUCT(TNodeGroupConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNodeGroupConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicNodeTrackerTestingConfig
    : public NYTree::TYsonStruct
{
    bool DisableDisposalFinishing;

    REGISTER_YSON_STRUCT(TDynamicNodeTrackerTestingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicNodeTrackerTestingConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicNodeTrackerConfig
    : public NYTree::TYsonStruct
{
    static constexpr auto DefaultProfilingPeriod = TDuration::Seconds(10);

    THashMap<std::string, TNodeGroupConfigPtr> NodeGroups;

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

    // COMPAT(kvk1920)
    bool ForbidMaintenanceAttributeWrites;

    // COMPAT(aleksandra-zh)
    bool EnablePerLocationNodeDisposal;

    TDuration NodeDisposalTickPeriod;

    TDuration PendingRestartLeaseTimeout;

    TDuration ResetNodePendingRestartMaintenancePeriod;

    int MaxNodesBeingDisposed;

    bool ImmediatelyDisposeNondataNodes;

    TDuration ThrottledNodeRegistrationExpirationTime;

    REGISTER_YSON_STRUCT(TDynamicNodeTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicNodeTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
