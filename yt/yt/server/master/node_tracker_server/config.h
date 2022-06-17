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
    REGISTER_YSON_STRUCT(TNodeTrackerConfig);

    static void Register(TRegistrar)
    { }
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

class TDynamicNodeTrackerConfig
    : public NYTree::TYsonStruct
{
public:
    static constexpr auto DefaultProfilingPeriod = TDuration::Seconds(10);

    THashMap<TString, TNodeGroupConfigPtr> NodeGroups;

    TDuration TotalNodeStatisticsUpdatePeriod;

    TDuration IncrementalNodeStatesGossipPeriod;
    TDuration FullNodeStatesGossipPeriod;

    int MaxConcurrentNodeRegistrations;
    int MaxConcurrentNodeUnregistrations;

    int MaxConcurrentClusterNodeHeartbeats;
    int MaxConcurrentExecNodeHeartbeats;

    int MaxConcurrentFullHeartbeats;
    int MaxConcurrentIncrementalHeartbeats;

    TDuration ForceNodeHeartbeatRequestTimeout;

    TNodeDiscoveryManagerConfigPtr MasterCacheManager;
    TNodeDiscoveryManagerConfigPtr TimestampProviderManager;

    bool UseNewHeartbeats;

    bool EnableStructuredLog;

    // COMPAT(ignat): Drop this after hosts migration.
    bool EnableNodeCpuStatistics;

    // COMPAT(gritukan): Drop this after hosts migration.
    bool PreserveRackForNewHost;

    TDuration ProfilingPeriod;

    REGISTER_YSON_STRUCT(TDynamicNodeTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicNodeTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
