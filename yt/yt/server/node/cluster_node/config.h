#pragma once

#include "public.h"

#include <yt/yt/server/lib/chaos_node/config.h>

#include <yt/yt/server/lib/dynamic_config/config.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/node/cellar_node/config.h>

#include <yt/yt/server/node/data_node/config.h>

#include <yt/yt/server/node/query_agent/config.h>

#include <yt/yt/ytlib/hive/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/node_tracker_client/config.h>
#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/transaction_client/config.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/library/profiling/solomon/exporter.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/concurrency/config.h>
#include <yt/yt/core/concurrency/fair_throttler.h>

#include <yt/yt/core/http/config.h>

#include <yt/yt/core/ytalloc/config.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

class TMemoryLimit
    : public NYTree::TYsonSerializable
{
public:
    // COMPAT(gritukan): Drop optional after configs migration.
    std::optional<NNodeTrackerClient::EMemoryLimitType> Type;

    std::optional<i64> Value;

    TMemoryLimit();

    void Validate();
};

DEFINE_REFCOUNTED_TYPE(TMemoryLimit)

////////////////////////////////////////////////////////////////////////////////

class TResourceLimitsConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Total amount of memory available for node.
    //! This value will be overridden when node runs in
    //! Porto environment.
    i64 TotalMemory;

    // COMPAT(gritukan)
    TMemoryLimitPtr UserJobs;
    TMemoryLimitPtr TabletStatic;
    TMemoryLimitPtr TabletDynamic;

    TEnumIndexedVector<NNodeTrackerClient::EMemoryCategory, TMemoryLimitPtr> MemoryLimits;

    // COMPAT(gritukan): Drop optional after configs migration.
    std::optional<i64> FreeMemoryWatermark;

    //! Total amount of CPU available for node.
    //! This value will be overridden when node runs in
    //! Porto environment.
    // COMPAT(gritukan): Drop optional after configs migration.
    std::optional<double> TotalCpu;

    // COMPAT(gritukan): Drop optional after configs migration.
    std::optional<double> NodeDedicatedCpu;

    // COMPAT(gritukan): Drop optional after configs migration.
    std::optional<double> CpuPerTabletSlot;

    double NodeCpuWeight;

    i64 MemoryAccountingTolerance;
    i64 MemoryAccountingGap;

    TResourceLimitsConfig();

    void Validate();
};

DEFINE_REFCOUNTED_TYPE(TResourceLimitsConfig)

////////////////////////////////////////////////////////////////////////////////

class TResourceLimitsDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    // COMPAT(gritukan)
    TMemoryLimitPtr UserJobs;
    TMemoryLimitPtr TabletStatic;
    TMemoryLimitPtr TabletDynamic;

    TEnumIndexedVector<NNodeTrackerClient::EMemoryCategory, TMemoryLimitPtr> MemoryLimits;

    std::optional<i64> FreeMemoryWatermark;

    std::optional<double> NodeDedicatedCpu;

    std::optional<double> CpuPerTabletSlot;

    double TotalCpu;

    bool UseInstanceLimitsTracker;

    TResourceLimitsDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TResourceLimitsDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectorDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<TDuration> IncrementalHeartbeatPeriod;
    std::optional<TDuration> IncrementalHeartbeatPeriodSplay;

    //! Period between consequent cluster node heartbeats.
    std::optional<TDuration> HeartbeatPeriod;

    //! Splay for cluster node heartbeats.
    std::optional<TDuration> HeartbeatPeriodSplay;

    // COMPAT(gritukan)
    bool UseHostObjects;

    TMasterConnectorDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TBatchingChunkServiceConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MaxBatchDelay;
    int MaxBatchCost;
    NConcurrency::TThroughputThrottlerConfigPtr CostThrottler;

    TBatchingChunkServiceConfig();
};

DEFINE_REFCOUNTED_TYPE(TBatchingChunkServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicConfigManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Whether dynamic config manager is enabled.
    bool Enabled;

    //! Period of config fetching from Cypress.
    TDuration UpdatePeriod;

    //! Whether alert for unrecognized dynamic config options
    //! should be enabled.
    bool EnableUnrecognizedOptionsAlert;

    TDynamicConfigManagerConfig();
};

DEFINE_REFCOUNTED_TYPE(TDynamicConfigManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TClusterNodeConnectionConfig
    : public NApi::NNative::TConnectionConfig
{
public:
    TClusterNodeConnectionConfig();
};

DEFINE_REFCOUNTED_TYPE(TClusterNodeConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

// COMPAT(gritukan): Drop optionals here after configs migration.
class TMasterConnectorConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Timeout for lease transactions.
    std::optional<TDuration> LeaseTransactionTimeout;

    //! Period between consequent lease transaction pings.
    std::optional<TDuration> LeaseTransactionPingPeriod;

    //! Splay for the first node registration.
    std::optional<TDuration> FirstRegisterSplay;

    //! Period between consequent registration attempts.
    std::optional<TDuration> RegisterRetryPeriod;

    //! Splay for consequent registration attempts.
    std::optional<TDuration> RegisterRetrySplay;

    //! Timeout for RegisterNode RPC requests.
    std::optional<TDuration> RegisterTimeout;

    //! Period between consequent cluster node heartbeats.
    TDuration HeartbeatPeriod;

    //! Splay for cluster node heartbeats.
    TDuration HeartbeatPeriodSplay;

    //! Timeout of the cluster node heartbeat RPC request.
    TDuration HeartbeatTimeout;

    //! Controls if cluster and cell directories are to be synchronized on connect.
    //! Useful for tests.
    std::optional<bool> SyncDirectoriesOnConnect;

    TMasterConnectorConfig();
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

class TClusterNodeConfig
    : public TDeprecatedServerConfig
{
public:
    //! Interval between Orchid cache rebuilds.
    TDuration OrchidCacheUpdatePeriod;

    //! Node-to-master connection.
    TClusterNodeConnectionConfigPtr ClusterConnection;

    //! Data node configuration part.
    NDataNode::TDataNodeConfigPtr DataNode;

    //! Exec node configuration part.
    NExecNode::TExecNodeConfigPtr ExecNode;

    //! Cellar node configuration part.
    NCellarNode::TCellarNodeConfigPtr CellarNode;

    //! Tablet node configuration part.
    NTabletNode::TTabletNodeConfigPtr TabletNode;

    //! Query node configuration part.
    NQueryAgent::TQueryAgentConfigPtr QueryAgent;

    //! Chaos node configuration part.
    NChaosNode::TChaosNodeConfigPtr ChaosNode;

    //! Metadata cache service configuration.
    NObjectClient::TCachingObjectServiceConfigPtr CachingObjectService;

    //! Chunk Service batcher and redirector.
    TBatchingChunkServiceConfigPtr BatchingChunkService;

    //! Timestamp provider config. Contains addresses used for timestamp generation.
    NTransactionClient::TRemoteTimestampProviderConfigPtr TimestampProvider;

    //! Known node addresses.
    NNodeTrackerClient::TNetworkAddressList Addresses;

    //! A set of tags to be assigned to this node.
    /*!
     * These tags are merged with others (e.g. provided by user and provided by master) to form
     * the full set of tags.
     */
    std::vector<TString> Tags;

    //! Name of the host node is running on.
    TString HostName;

    //! Limits for the node process and all jobs controlled by it.
    TResourceLimitsConfigPtr ResourceLimits;

    //! Timeout for RPC query in JobBandwidthThrottler.
    NJobProxy::TJobThrottlerConfigPtr JobThrottler;

    TDuration ResourceLimitsUpdatePeriod;
    std::optional<TDuration> InstanceLimitsUpdatePeriod;

    int SkynetHttpPort;

    NYTree::IMapNodePtr CypressAnnotations;

    bool EnableUnrecognizedOptionsAlert;

    bool AbortOnUnrecognizedOptions;

    //! Dynamic config manager config.
    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;

    //! If |true|, new master heartbeats are used (if master supports them).
    bool UseNewHeartbeats;

    //! List of the node flavors.
    std::vector<NNodeTrackerClient::ENodeFlavor> Flavors;

    //! Master connector config.
    TMasterConnectorConfigPtr MasterConnector;

    //! Is used to configure relative network throttler limits.
    i64 NetworkBandwidth;

    //! Bucket configuration for in network throttlers.
    THashMap<TString, NConcurrency::TFairThrottlerBucketConfigPtr> InThrottlers;

    //! Bucket configuration for out network throttlers.
    THashMap<TString, NConcurrency::TFairThrottlerBucketConfigPtr> OutThrottlers;

    TClusterNodeConfig();

    NHttp::TServerConfigPtr CreateSkynetHttpServerConfig();
};

DEFINE_REFCOUNTED_TYPE(TClusterNodeConfig)

////////////////////////////////////////////////////////////////////////////////

class TClusterNodeDynamicConfig
    : public TDeprecatedSingletonsDynamicConfig
{
public:
    //! Dynamic config annotation.
    TString ConfigAnnotation;

    //! Node resource limits.
    TResourceLimitsDynamicConfigPtr ResourceLimits;

    //! Data node configuration part.
    NDataNode::TDataNodeDynamicConfigPtr DataNode;

    //! Cellar node configuration part.
    NCellarNode::TCellarNodeDynamicConfigPtr CellarNode;

    //! Tablet node configuration part.
    NTabletNode::TTabletNodeDynamicConfigPtr TabletNode;

    //! Query agent configuration part.
    NQueryAgent::TQueryAgentDynamicConfigPtr QueryAgent;

    //! Exec agent configuration part.
    NExecNode::TExecNodeDynamicConfigPtr ExecNode;

    //! Metadata cache service configuration.
    NObjectClient::TCachingObjectServiceDynamicConfigPtr CachingObjectService;

    //! Master connector configuration.
    TMasterConnectorDynamicConfigPtr MasterConnector;

    //! Bucket configuration for in network throttlers.
    THashMap<TString, NConcurrency::TFairThrottlerBucketConfigPtr> InThrottlers;

    //! Bucket configuration for out network throttlers.
    THashMap<TString, NConcurrency::TFairThrottlerBucketConfigPtr> OutThrottlers;

    TClusterNodeDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TClusterNodeDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
