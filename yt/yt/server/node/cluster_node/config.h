#pragma once

#include "public.h"

#include <yt/yt/server/lib/dynamic_config/config.h>

#include <yt/yt/server/lib/exec_agent/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/node/data_node/config.h>

#include <yt/yt/server/node/query_agent/config.h>

#include <yt/yt/ytlib/hive/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/node_tracker_client/config.h>
#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/transaction_client/config.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/concurrency/config.h>

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

    TMemoryLimit()
    {
        RegisterParameter("type", Type)
            .Default();

        RegisterParameter("value", Value)
            .Default();

        RegisterPostprocessor([&] {
            if (Type == NNodeTrackerClient::EMemoryLimitType::Static && !Value) {
                THROW_ERROR_EXCEPTION("Value should be set for static memory limits");
            }
            if (Type != NNodeTrackerClient::EMemoryLimitType::Static && Value) {
                THROW_ERROR_EXCEPTION("Value can be set only for static memory limits");
            }
        });
    }

    void Validate()
    {
        if (!Type) {
            THROW_ERROR_EXCEPTION("Memory limit type should be set");
        }
    }
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

    TResourceLimitsConfig()
    {
        // Very low default, override for production use.
        // COMPAT(gritukan)
        RegisterParameter("total_memory", TotalMemory)
            .Alias("memory")
            .GreaterThanOrEqual(0)
            .Default(5_GB);

        RegisterParameter("user_jobs", UserJobs)
            .Default();
        RegisterParameter("tablet_static", TabletStatic)
            .Default();
        RegisterParameter("tablet_dynamic", TabletDynamic)
            .Default();

        RegisterParameter("memory_limits", MemoryLimits)
            .Default();

        RegisterParameter("free_memory_watermark", FreeMemoryWatermark)
            .Default();

        RegisterParameter("total_cpu", TotalCpu)
            .Default();

        RegisterParameter("node_dedicated_cpu", NodeDedicatedCpu)
            .Default();

        RegisterParameter("cpu_per_tablet_slot", CpuPerTabletSlot)
            .Default();

        RegisterParameter("node_cpu_weight", NodeCpuWeight)
            .GreaterThanOrEqual(0.01)
            .LessThanOrEqual(100)
            .Default(10);

        RegisterParameter("memory_accounting_tolerance", MemoryAccountingTolerance)
            .GreaterThan(0)
            .LessThanOrEqual(1_GB)
            .Default(1_MB);

        RegisterPreprocessor([&] {
            // Default LookupRowsCache memory limit.
            auto lookupRowsCacheLimit = New<TMemoryLimit>();
            lookupRowsCacheLimit->Type = NNodeTrackerClient::EMemoryLimitType::Static;
            lookupRowsCacheLimit->Value = 0;
            MemoryLimits[NNodeTrackerClient::EMemoryCategory::LookupRowsCache] = lookupRowsCacheLimit;
        });

        RegisterPostprocessor([&] {
            if (UserJobs) {
                MemoryLimits[NNodeTrackerClient::EMemoryCategory::UserJobs] = UserJobs;
            }
            if (TabletStatic) {
                MemoryLimits[NNodeTrackerClient::EMemoryCategory::TabletStatic] = TabletStatic;
            }
            if (TabletDynamic) {
                MemoryLimits[NNodeTrackerClient::EMemoryCategory::TabletDynamic] = TabletDynamic;
            }
        });
    }

    void Validate()
    {
        if (!FreeMemoryWatermark) {
            THROW_ERROR_EXCEPTION("\'free_memory_watermark\' should be set");
        }
        if (!TotalCpu) {
            THROW_ERROR_EXCEPTION("\'total_cpu\' should be set");
        }
        if (!NodeDedicatedCpu) {
            THROW_ERROR_EXCEPTION("\'node_dedicated_cpu\' should be set");
        }
        if (!CpuPerTabletSlot) {
            THROW_ERROR_EXCEPTION("\'cpu_per_tablet_slot\' should be set");
        }
    }
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

    TResourceLimitsDynamicConfig()
    {
        RegisterParameter("user_jobs", UserJobs)
            .Default();
        RegisterParameter("tablet_static", TabletStatic)
            .Default();
        RegisterParameter("tablet_dynamic", TabletDynamic)
            .Default();

        RegisterParameter("memory_limits", MemoryLimits)
            .Default();
        RegisterParameter("free_memory_watermark", FreeMemoryWatermark)
            .Default();
        RegisterParameter("node_dedicated_cpu", NodeDedicatedCpu)
            .Default();
        RegisterParameter("cpu_per_tablet_slot", CpuPerTabletSlot)
            .Default();

        RegisterParameter("total_cpu", TotalCpu)
            .Default(0);

        RegisterPostprocessor([&] {
            if (UserJobs) {
                MemoryLimits[NNodeTrackerClient::EMemoryCategory::UserJobs] = UserJobs;
            }
            if (TabletStatic) {
                MemoryLimits[NNodeTrackerClient::EMemoryCategory::TabletStatic] = TabletStatic;
            }
            if (TabletDynamic) {
                MemoryLimits[NNodeTrackerClient::EMemoryCategory::TabletDynamic] = TabletDynamic;
            }
        });
    }
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

    TMasterConnectorDynamicConfig()
    {
        RegisterParameter("incremental_heartbeat_period", IncrementalHeartbeatPeriod)
            .Default();
        RegisterParameter("incremental_heartbeat_period_splay", IncrementalHeartbeatPeriodSplay)
            .Default();
        RegisterParameter("heartbeat_period", HeartbeatPeriod)
            .Default();
        RegisterParameter("heartbeat_period_splay", HeartbeatPeriodSplay)
            .Default();
    }
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

    TBatchingChunkServiceConfig()
    {
        RegisterParameter("max_batch_delay", MaxBatchDelay)
            .Default(TDuration::Zero());
        RegisterParameter("max_batch_cost", MaxBatchCost)
            .Default(1000);
        RegisterParameter("cost_throttler", CostThrottler)
            .DefaultNew();
    }
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

    TDynamicConfigManagerConfig()
    {
        RegisterParameter("enabled", Enabled)
            .Default(true);
        RegisterParameter("update_period", UpdatePeriod)
            .Default(TDuration::Seconds(30));
        RegisterParameter("enable_unrecognized_options_alert", EnableUnrecognizedOptionsAlert)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicConfigManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TClusterNodeConnectionConfig
    : public NApi::NNative::TConnectionConfig
{
public:
    TClusterNodeConnectionConfig()
    {
        RegisterPreprocessor([&] {
            // Provide a lower channel cache TTL to reduce the total number
            // of inter-cluster connections. This also gets propagated to job proxy config
            // and helps decreasing memory footprint.
            IdleChannelTtl = TDuration::Seconds(60);
        });
    }
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

    TMasterConnectorConfig()
    {
        RegisterParameter("lease_trascation_timeout", LeaseTransactionTimeout)
            .Default();
        RegisterParameter("lease_transaction_ping_period", LeaseTransactionPingPeriod)
            .Default();

        RegisterParameter("register_retry_period", RegisterRetryPeriod)
            .Default();
        RegisterParameter("register_retry_splay", RegisterRetrySplay)
            .Default();
        RegisterParameter("register_timeout", RegisterTimeout)
            .Default();

        RegisterParameter("heartbeat_period", HeartbeatPeriod)
            .Default(TDuration::Seconds(30));
        RegisterParameter("heartbeat_period_splay", HeartbeatPeriodSplay)
            .Default(TDuration::Seconds(1));
        RegisterParameter("heartbeat_timeout", HeartbeatTimeout)
            .Default(TDuration::Seconds(60));

        RegisterParameter("sync_directories_on_connect", SyncDirectoriesOnConnect)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

class TClusterNodeConfig
    : public TServerConfig
{
public:
    //! Interval between Orchid cache rebuilds.
    TDuration OrchidCacheUpdatePeriod;

    //! Node-to-master connection.
    TClusterNodeConnectionConfigPtr ClusterConnection;

    //! Data node configuration part.
    NDataNode::TDataNodeConfigPtr DataNode;

    //! Exec node configuration part.
    NExecAgent::TExecAgentConfigPtr ExecAgent;

    //! Tablet node configuration part.
    NTabletNode::TTabletNodeConfigPtr TabletNode;

    //! Query node configuration part.
    NQueryAgent::TQueryAgentConfigPtr QueryAgent;

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

    TClusterNodeConfig()
    {
        RegisterParameter("orchid_cache_update_period", OrchidCacheUpdatePeriod)
            .Default(TDuration::Seconds(5));
        RegisterParameter("cluster_connection", ClusterConnection);
        RegisterParameter("data_node", DataNode)
            .DefaultNew();
        RegisterParameter("exec_agent", ExecAgent)
            .DefaultNew();
        RegisterParameter("tablet_node", TabletNode)
            .DefaultNew();
        RegisterParameter("query_agent", QueryAgent)
            .DefaultNew();
        RegisterParameter("caching_object_service", CachingObjectService)
            .Alias("master_cache_service")
            .DefaultNew();
        RegisterParameter("batching_chunk_service", BatchingChunkService)
            .DefaultNew();
        RegisterParameter("timestamp_provider", TimestampProvider)
            .Default();
        RegisterParameter("addresses", Addresses)
            .Default();
        RegisterParameter("tags", Tags)
            .Default();
        RegisterParameter("resource_limits", ResourceLimits)
            .DefaultNew();
        RegisterParameter("job_throttler", JobThrottler)
            .DefaultNew();

        RegisterParameter("resource_limits_update_period", ResourceLimitsUpdatePeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("instance_limits_update_period", InstanceLimitsUpdatePeriod)
            .Default();

        RegisterParameter("skynet_http_port", SkynetHttpPort)
            .Default(10080);

        RegisterParameter("cypress_annotations", CypressAnnotations)
            .Default(NYTree::BuildYsonNodeFluently()
                .BeginMap()
                .EndMap()
            ->AsMap());

        RegisterParameter("enable_unrecognized_options_alert", EnableUnrecognizedOptionsAlert)
            .Default(false);

        RegisterParameter("abort_on_unrecognized_options", AbortOnUnrecognizedOptions)
            .Default(false);

        RegisterParameter("dynamic_config_manager", DynamicConfigManager)
            .DefaultNew();

        RegisterParameter("use_new_heartbeats", UseNewHeartbeats)
            .Default(false);

        RegisterParameter("flavors", Flavors)
            .Default(std::vector<NNodeTrackerClient::ENodeFlavor>({
                NNodeTrackerClient::ENodeFlavor::Data,
                NNodeTrackerClient::ENodeFlavor::Exec,
                NNodeTrackerClient::ENodeFlavor::Tablet
            }));

        RegisterParameter("master_connector", MasterConnector)
            .DefaultNew();

        RegisterParameter("network_bandwidth", NetworkBandwidth)
            .Default(1250000000);

        RegisterPostprocessor([&] {
            NNodeTrackerClient::ValidateNodeTags(Tags);

            // COMPAT(gritukan): Drop this code after configs migration.
            if (!ResourceLimits->MemoryLimits[NNodeTrackerClient::EMemoryCategory::UserJobs]) {
                auto& memoryLimit = ResourceLimits->MemoryLimits[NNodeTrackerClient::EMemoryCategory::UserJobs];
                memoryLimit = New<TMemoryLimit>();
                memoryLimit->Type = NNodeTrackerClient::EMemoryLimitType::Dynamic;
            }
            if (!ResourceLimits->MemoryLimits[NNodeTrackerClient::EMemoryCategory::TabletStatic]) {
                auto& memoryLimit = ResourceLimits->MemoryLimits[NNodeTrackerClient::EMemoryCategory::TabletStatic];
                memoryLimit = New<TMemoryLimit>();
                if (TabletNode->ResourceLimits->TabletStaticMemory == std::numeric_limits<i64>::max()) {
                    memoryLimit->Type = NNodeTrackerClient::EMemoryLimitType::None;
                } else {
                    memoryLimit->Type = NNodeTrackerClient::EMemoryLimitType::Static;
                    memoryLimit->Value = TabletNode->ResourceLimits->TabletStaticMemory;
                }
            }
            if (!ResourceLimits->MemoryLimits[NNodeTrackerClient::EMemoryCategory::TabletDynamic]) {
                auto& memoryLimit = ResourceLimits->MemoryLimits[NNodeTrackerClient::EMemoryCategory::TabletDynamic];
                memoryLimit = New<TMemoryLimit>();
                if (TabletNode->ResourceLimits->TabletDynamicMemory == std::numeric_limits<i64>::max()) {
                    memoryLimit->Type = NNodeTrackerClient::EMemoryLimitType::None;
                } else {
                    memoryLimit->Type = NNodeTrackerClient::EMemoryLimitType::Static;
                    memoryLimit->Value = TabletNode->ResourceLimits->TabletDynamicMemory;
                }
            }
            if (!ResourceLimits->FreeMemoryWatermark) {
                ResourceLimits->FreeMemoryWatermark = 0;
                auto freeMemoryWatermarkNode = ExecAgent->SlotManager->JobEnvironment->AsMap()->FindChild("free_memory_watermark");
                if (freeMemoryWatermarkNode) {
                    ResourceLimits->FreeMemoryWatermark = freeMemoryWatermarkNode->GetValue<i64>();
                }
            }
            if (!ResourceLimits->NodeDedicatedCpu) {
                ResourceLimits->NodeDedicatedCpu = 2; // Old default.
                auto nodeDedicatedCpuNode = ExecAgent->SlotManager->JobEnvironment->AsMap()->FindChild("node_dedicated_cpu");
                if (nodeDedicatedCpuNode) {
                    ResourceLimits->NodeDedicatedCpu = nodeDedicatedCpuNode->GetValue<double>();
                }
            }
            if (!ResourceLimits->CpuPerTabletSlot) {
                ResourceLimits->CpuPerTabletSlot = ExecAgent->JobController->CpuPerTabletSlot;
            }
            if (!InstanceLimitsUpdatePeriod) {
                auto resourceLimitsUpdatePeriodNode = ExecAgent->SlotManager->JobEnvironment->AsMap()->FindChild("resource_limits_update_period");
                if (resourceLimitsUpdatePeriodNode) {
                    InstanceLimitsUpdatePeriod = NYTree::ConvertTo<std::optional<TDuration>>(resourceLimitsUpdatePeriodNode);
                }
            }

            DynamicConfigManager->IgnoreConfigAbsence = true;

            // COMPAT(gritukan)
            if (!MasterConnector->LeaseTransactionTimeout) {
                MasterConnector->LeaseTransactionTimeout = DataNode->LeaseTransactionTimeout;
            }
            if (!MasterConnector->LeaseTransactionPingPeriod) {
                MasterConnector->LeaseTransactionPingPeriod = DataNode->LeaseTransactionPingPeriod;
            }
            if (!MasterConnector->FirstRegisterSplay) {
                // This is not a mistake!
                MasterConnector->FirstRegisterSplay = DataNode->IncrementalHeartbeatPeriod;
            }
            if (!MasterConnector->RegisterRetryPeriod) {
                MasterConnector->RegisterRetryPeriod = DataNode->RegisterRetryPeriod;
            }
            if (!MasterConnector->RegisterRetrySplay) {
                MasterConnector->RegisterRetrySplay = DataNode->RegisterRetrySplay;
            }
            if (!MasterConnector->RegisterTimeout) {
                MasterConnector->RegisterTimeout = DataNode->RegisterTimeout;
            }
            if (!MasterConnector->SyncDirectoriesOnConnect) {
                MasterConnector->SyncDirectoriesOnConnect = DataNode->SyncDirectoriesOnConnect;
            }
        });
    }

    NHttp::TServerConfigPtr CreateSkynetHttpServerConfig()
    {
        auto config = New<NHttp::TServerConfig>();
        config->Port = SkynetHttpPort;
        config->BindRetryCount = BusServer->BindRetryCount;
        config->BindRetryBackoff = BusServer->BindRetryBackoff;
        config->ServerName = "skynet";
        return config;
    }
};

DEFINE_REFCOUNTED_TYPE(TClusterNodeConfig)

////////////////////////////////////////////////////////////////////////////////

class TClusterNodeDynamicConfig
    : public TSingletonsDynamicConfig
{
public:
    //! Dynamic config annotation.
    TString ConfigAnnotation;

    //! Node resource limits.
    TResourceLimitsDynamicConfigPtr ResourceLimits;

    //! Data node configuration part.
    NDataNode::TDataNodeDynamicConfigPtr DataNode;

    //! Tablet node configuration part.
    NTabletNode::TTabletNodeDynamicConfigPtr TabletNode;

    //! Query agent configuration part.
    NQueryAgent::TQueryAgentDynamicConfigPtr QueryAgent;

    //! Exec agent configuration part.
    NExecAgent::TExecAgentDynamicConfigPtr ExecAgent;

    //! Metadata cache service configuration.
    NObjectClient::TCachingObjectServiceDynamicConfigPtr CachingObjectService;

    //! Master connector configuration.
    TMasterConnectorDynamicConfigPtr MasterConnector;

    TClusterNodeDynamicConfig()
    {
        RegisterParameter("config_annotation", ConfigAnnotation)
            .Optional();
        RegisterParameter("resource_limits", ResourceLimits)
            .DefaultNew();
        RegisterParameter("data_node", DataNode)
            .DefaultNew();
        RegisterParameter("tablet_node", TabletNode)
            .DefaultNew();
        RegisterParameter("query_agent", QueryAgent)
            .DefaultNew();
        RegisterParameter("exec_agent", ExecAgent)
            .DefaultNew();
        RegisterParameter("caching_object_service", CachingObjectService)
            .DefaultNew();
        RegisterParameter("master_connector", MasterConnector)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TClusterNodeDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
