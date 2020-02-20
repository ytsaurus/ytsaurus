#pragma once

#include "public.h"

#include <yp/client/api/native/config.h>

#include <yt/client/api/rpc_proxy/config.h>

#include <yt/ytlib/program/config.h>

#include <yt/core/http/config.h>

namespace NYP::NServer::NHeavyScheduler {

////////////////////////////////////////////////////////////////////////////////

class TYTConnectorConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    NApi::NRpcProxy::TConnectionConfigPtr Connection;

    TString User;
    TString Token;

    NYPath::TYPath RootPath;

    TDuration ConnectPeriod;
    TDuration LeaderTransactionTimeout;

    TYTConnectorConfig();
};

DEFINE_REFCOUNTED_TYPE(TYTConnectorConfig);

////////////////////////////////////////////////////////////////////////////////

class TClusterReaderConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    int SelectBatchSize;

    TClusterReaderConfig();
};

DEFINE_REFCOUNTED_TYPE(TClusterReaderConfig);

////////////////////////////////////////////////////////////////////////////////

class TTaskManagerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    TDuration TaskTimeLimit;
    TEnumIndexedVector<ETaskSource, int> TaskSlotsPerSource;

    TTaskManagerConfig();
};

DEFINE_REFCOUNTED_TYPE(TTaskManagerConfig);

////////////////////////////////////////////////////////////////////////////////

class TDisruptionThrottlerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    bool LimitEvictionsByPodSet;
    bool ValidatePodDisruptionBudget;
    int SafeSuitableNodeCount;

    TDisruptionThrottlerConfig();
};

DEFINE_REFCOUNTED_TYPE(TDisruptionThrottlerConfig);

////////////////////////////////////////////////////////////////////////////////

class TSwapDefragmentatorConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    int StarvingPodsPerIterationLimit;
    int VictimCandidatePodCount;

    TSwapDefragmentatorConfig();
};

DEFINE_REFCOUNTED_TYPE(TSwapDefragmentatorConfig);

////////////////////////////////////////////////////////////////////////////////

class TAntiaffinityHealerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    int PodsPerIterationSoftLimit;

    TAntiaffinityHealerConfig();
};

DEFINE_REFCOUNTED_TYPE(TAntiaffinityHealerConfig);

////////////////////////////////////////////////////////////////////////////////

class TEvictionGarbageCollectorConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    TDuration TimeLimit;

    TEvictionGarbageCollectorConfig();
};

DEFINE_REFCOUNTED_TYPE(TEvictionGarbageCollectorConfig);

////////////////////////////////////////////////////////////////////////////////

class THeavySchedulerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    // General.
    TDuration IterationPeriod;
    bool Verbose;

    // Cluster reader.
    TClusterReaderConfigPtr ClusterReader;

    // Node segment.
    NCluster::TObjectId NodeSegment;

    // Cluster safety.
    int SafeClusterPodEvictionCount;

    TTaskManagerConfigPtr TaskManager;
    TDisruptionThrottlerConfigPtr DisruptionThrottler;
    TSwapDefragmentatorConfigPtr SwapDefragmentator;
    TAntiaffinityHealerConfigPtr AntiaffinityHealer;
    TEvictionGarbageCollectorConfigPtr EvictionGarbageCollector;

    THeavySchedulerConfig();
};

DEFINE_REFCOUNTED_TYPE(THeavySchedulerConfig);

class THeavySchedulerProgramConfig
    : public NYT::TSingletonsConfig
{
public:
    NClient::NApi::NNative::TClientConfigPtr Client;
    NHttp::TServerConfigPtr MonitoringServer;
    TYTConnectorConfigPtr YTConnector;
    THeavySchedulerConfigPtr HeavyScheduler;

    THeavySchedulerProgramConfig();
};

DEFINE_REFCOUNTED_TYPE(THeavySchedulerProgramConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
