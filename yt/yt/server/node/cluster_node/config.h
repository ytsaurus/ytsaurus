#pragma once

#include "public.h"

#include <yt/server/lib/exec_agent/config.h>

#include <yt/server/lib/misc/config.h>

#include <yt/server/lib/object_server/config.h>

#include <yt/server/node/query_agent/config.h>

#include <yt/server/lib/tablet_node/config.h>

#include <yt/server/node/data_node/config.h>

#include <yt/ytlib/hive/config.h>

#include <yt/ytlib/api/native/config.h>

#include <yt/ytlib/node_tracker_client/config.h>
#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/core/concurrency/config.h>

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

    TMemoryLimitPtr UserJobs;
    TMemoryLimitPtr TabletStatic;
    TMemoryLimitPtr TabletDynamic;

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

    TResourceLimitsConfig()
    {
        // Very low default, override for production use.
        // COMPAT(gritukan)
        RegisterParameter("total_memory", TotalMemory)
            .Alias("memory")
            .GreaterThanOrEqual(0)
            .Default(5_GB);

        RegisterParameter("user_jobs", UserJobs)
            .DefaultNew();
        RegisterParameter("tablet_static", TabletStatic)
            .DefaultNew();
        RegisterParameter("tablet_dynamic", TabletDynamic)
            .DefaultNew();

        RegisterParameter("free_memory_watermark", FreeMemoryWatermark)
            .Default();

        RegisterParameter("total_cpu", TotalCpu)
            .Default();

        RegisterParameter("node_dedicated_cpu", NodeDedicatedCpu)
            .Default();

        RegisterParameter("cpu_per_tablet_slot", CpuPerTabletSlot)
            .Default();

    }
};

DEFINE_REFCOUNTED_TYPE(TResourceLimitsConfig)

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
    NObjectServer::TMasterCacheServiceConfigPtr MasterCacheService;

    //! Chunk Service batcher and redirector.
    TBatchingChunkServiceConfigPtr BatchingChunkService;

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
    TDynamicConfigManagerConfigPtr DynamicConfigManager;

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
        RegisterParameter("master_cache_service", MasterCacheService)
            .DefaultNew();
        RegisterParameter("batching_chunk_service", BatchingChunkService)
            .DefaultNew();
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

        RegisterPostprocessor([&] {
            NNodeTrackerClient::ValidateNodeTags(Tags);

            // COMPAT(gritukan): Drop this code after configs migration.
            if (!ResourceLimits->UserJobs->Type) {
                ResourceLimits->UserJobs->Type = NNodeTrackerClient::EMemoryLimitType::Dynamic;
            }
            if (!ResourceLimits->TabletStatic->Type) {
                if (TabletNode->ResourceLimits->TabletStaticMemory == std::numeric_limits<i64>::max()) {
                    ResourceLimits->TabletStatic->Type = NNodeTrackerClient::EMemoryLimitType::None;
                } else {
                    ResourceLimits->TabletStatic->Type = NNodeTrackerClient::EMemoryLimitType::Static;
                    ResourceLimits->TabletStatic->Value = TabletNode->ResourceLimits->TabletStaticMemory;
                }
            }
            if (!ResourceLimits->TabletDynamic->Type) {
                if (TabletNode->ResourceLimits->TabletDynamicMemory == std::numeric_limits<i64>::max()) {
                    ResourceLimits->TabletDynamic->Type = NNodeTrackerClient::EMemoryLimitType::None;
                } else {
                    ResourceLimits->TabletDynamic->Type = NNodeTrackerClient::EMemoryLimitType::Static;
                    ResourceLimits->TabletDynamic->Value = TabletNode->ResourceLimits->TabletDynamicMemory;
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
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TClusterNodeConfig)

////////////////////////////////////////////////////////////////////////////////

class TClusterNodeDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Dynamic config annotation.
    TString ConfigAnnotation;

    TClusterNodeDynamicConfig()
    {
        RegisterParameter("config_annotation", ConfigAnnotation)
            .Optional();
    }
};

DEFINE_REFCOUNTED_TYPE(TClusterNodeDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
