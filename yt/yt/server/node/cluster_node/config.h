#pragma once

#include "public.h"

#include <yt/yt/server/lib/chaos_node/public.h>

#include <yt/yt/server/lib/exec_node/public.h>

#include <yt/yt/server/lib/job_agent/public.h>

#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/tablet_node/public.h>

#include <yt/yt/server/node/cellar_node/public.h>

#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/server/node/query_agent/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>
#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/library/dynamic_config/public.h>

#include <yt/yt/library/profiling/solomon/exporter.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/concurrency/fair_throttler.h>

#include <yt/yt/core/http/public.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

class TMemoryLimit
    : public NYTree::TYsonStruct
{
public:
    // COMPAT(gritukan): Drop optional after configs migration.
    std::optional<NNodeTrackerClient::EMemoryLimitType> Type;

    std::optional<i64> Value;

    REGISTER_YSON_STRUCT(TMemoryLimit);

    static void Register(TRegistrar registrar);

    void Validate();
};

DEFINE_REFCOUNTED_TYPE(TMemoryLimit)

////////////////////////////////////////////////////////////////////////////////

class TResourceLimitsConfig
    : public NYTree::TYsonStruct
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

    TEnumIndexedArray<EMemoryCategory, TMemoryLimitPtr> MemoryLimits;

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

    REGISTER_YSON_STRUCT(TResourceLimitsConfig);

    static void Register(TRegistrar registrar);

    void Validate();
};

DEFINE_REFCOUNTED_TYPE(TResourceLimitsConfig)

////////////////////////////////////////////////////////////////////////////////

class TResourceLimitsOverrides
    : public NYTree::TYsonStruct
{
public:
    #define XX(name, Name) \
        std::optional<decltype(NNodeTrackerClient::NProto::TNodeResourceLimitsOverrides::default_instance().name())> Name;
    ITERATE_NODE_RESOURCE_LIMITS_DYNAMIC_CONFIG_OVERRIDES(XX)
    #undef XX

    REGISTER_YSON_STRUCT(TResourceLimitsOverrides);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TResourceLimitsOverrides)

////////////////////////////////////////////////////////////////////////////////

class TChunkReplicaCacheDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<TDuration> ExpirationTime;

    REGISTER_YSON_STRUCT(TChunkReplicaCacheDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkReplicaCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TResourceLimitsDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    // COMPAT(gritukan)
    TMemoryLimitPtr UserJobs;
    TMemoryLimitPtr TabletStatic;
    TMemoryLimitPtr TabletDynamic;

    TEnumIndexedArray<EMemoryCategory, TMemoryLimitPtr> MemoryLimits;

    std::optional<i64> FreeMemoryWatermark;

    std::optional<double> NodeDedicatedCpu;

    std::optional<double> CpuPerTabletSlot;

    double TotalCpu;

    bool UseInstanceLimitsTracker;

    TResourceLimitsOverridesPtr Overrides;

    REGISTER_YSON_STRUCT(TResourceLimitsDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TResourceLimitsDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectorDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<NConcurrency::TRetryingPeriodicExecutorOptions> HeartbeatExecutor;

    // COMPAT(gritukan)
    bool UseHostObjects;

    REGISTER_YSON_STRUCT(TMasterConnectorDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TProxyingChunkServiceConfig
    : public NYTree::TYsonStruct
{
public:
    NConcurrency::TThroughputThrottlerConfigPtr CostThrottler;

    REGISTER_YSON_STRUCT(TProxyingChunkServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyingChunkServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicConfigManagerConfig
    : public NYTree::TYsonStruct
{
public:
    //! Whether dynamic config manager is enabled.
    bool Enabled;

    //! Period of config fetching from Cypress.
    TDuration UpdatePeriod;

    //! Whether alert for unrecognized dynamic config options
    //! should be enabled.
    bool EnableUnrecognizedOptionsAlert;

    REGISTER_YSON_STRUCT(TDynamicConfigManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicConfigManagerConfig)

////////////////////////////////////////////////////////////////////////////////

// COMPAT(gritukan): Drop optionals here after configs migration.
class TMasterConnectorConfig
    : public NYTree::TYsonStruct
{
public:
    //! Timeout for lease transactions.
    std::optional<TDuration> LeaseTransactionTimeout;

    //! Period between consequent lease transaction pings.
    std::optional<TDuration> LeaseTransactionPingPeriod;

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

    NConcurrency::TRetryingPeriodicExecutorOptions HeartbeatExecutor;

    //! Controls if cluster and cell directories are to be synchronized on connect.
    //! Useful for tests.
    std::optional<bool> SyncDirectoriesOnConnect;

    REGISTER_YSON_STRUCT(TMasterConnectorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

class TClusterNodeConfig
    : public TNativeServerConfig
{
public:
    //! Interval between Orchid cache rebuilds.
    TDuration OrchidCacheUpdatePeriod;

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

    //! Job resource manager configuration part.
    NJobAgent::TJobResourceManagerConfigPtr JobResourceManager;

    //! Metadata cache service configuration.
    NObjectClient::TCachingObjectServiceConfigPtr CachingObjectService;

    //! Chunk service redirector.
    TProxyingChunkServiceConfigPtr ProxyingChunkService;

    //! Timestamp provider config. Contains addresses used for timestamp generation.
    NTransactionClient::TRemoteTimestampProviderConfigPtr TimestampProvider;

    //! Dry run config.
    NHydra::THydraDryRunConfigPtr DryRun;

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

    TDuration ResourceLimitsUpdatePeriod;
    std::optional<TDuration> InstanceLimitsUpdatePeriod;

    int SkynetHttpPort;

    NYTree::IMapNodePtr CypressAnnotations;

    bool EnableUnrecognizedOptionsAlert;

    bool AbortOnUnrecognizedOptions;

    //! Dynamic config manager config.
    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;

    //! If |true|, exec node do not report data node heartbeats.
    bool ExecNodeIsNotDataNode;

    //! List of the node flavors.
    std::vector<NNodeTrackerClient::ENodeFlavor> Flavors;

    //! Master connector config.
    TMasterConnectorConfigPtr MasterConnector;

    i64 NetworkBandwidth;

    //! Network throttler limit is this smaller than NetworkBandwidth.
    double ThrottlerFreeBandwidthRatio;

    bool EnableFairThrottler;

    NConcurrency::TFairThrottlerConfigPtr InThrottler;

    NConcurrency::TFairThrottlerConfigPtr OutThrottler;

    //! Bucket configuration for in network throttlers.
    THashMap<TString, NConcurrency::TFairThrottlerBucketConfigPtr> InThrottlers;

    //! Bucket configuration for out network throttlers.
    THashMap<TString, NConcurrency::TFairThrottlerBucketConfigPtr> OutThrottlers;

    NHttp::TServerConfigPtr CreateSkynetHttpServerConfig();

    REGISTER_YSON_STRUCT(TClusterNodeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClusterNodeConfig)

////////////////////////////////////////////////////////////////////////////////

class TClusterNodeDynamicConfig
    : public TNativeSingletonsDynamicConfig
{
public:
    static constexpr bool EnableHazard = true;

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

    //! Job resource manager configuration part.
    NJobAgent::TJobResourceManagerDynamicConfigPtr JobResourceManager;

    //! Metadata cache service configuration.
    NObjectClient::TCachingObjectServiceDynamicConfigPtr CachingObjectService;

    //! Master connector configuration.
    TMasterConnectorDynamicConfigPtr MasterConnector;

    //! Bucket configuration for in network throttlers.
    THashMap<TString, NConcurrency::TFairThrottlerBucketConfigPtr> InThrottlers;

    //! Bucket configuration for out network throttlers.
    THashMap<TString, NConcurrency::TFairThrottlerBucketConfigPtr> OutThrottlers;

    //! IO tracker config.
    NIO::TIOTrackerConfigPtr IOTracker;

    NRpc::TServerDynamicConfigPtr RpcServer;

    //! Network throttler limit is this smaller than NetworkBandwidth.
    std::optional<double> ThrottlerFreeBandwidthRatio;

    //! Chunk replica cache config overrides
    TChunkReplicaCacheDynamicConfigPtr ChunkReplicaCacheConfig;

    bool UsePortoNetworkLimitInThrottler;

    double MemoryUsageIsCloseToLimitThreshold;

    double TotalMemoryLimitExceededThreshold;

    double MemoryLimitExceededForCategoryThreshold;

    REGISTER_YSON_STRUCT(TClusterNodeDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClusterNodeDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
