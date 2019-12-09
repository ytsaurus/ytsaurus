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

    // Task manager.
    TDuration TaskTimeLimit;
    int ConcurrentTaskLimit;
    int StarvingPodsPerIterationLimit;
    bool LimitEvictionsByPodSet;

    // Cluster safety.
    int SafeClusterPodEvictionCount;

    // Victim pod search.
    int VictimCandidatePodCount;
    int SafeSuitableNodeCount;
    bool ValidatePodDisruptionBudget;

    THeavySchedulerConfig();
};

DEFINE_REFCOUNTED_TYPE(THeavySchedulerConfig);

////////////////////////////////////////////////////////////////////////////////

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
