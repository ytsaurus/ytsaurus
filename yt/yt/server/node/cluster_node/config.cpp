#include "config.h"

#include <yt/yt/server/node/data_node/config.h>

#include <yt/yt/server/node/cellar_node/config.h>

#include <yt/yt/server/node/query_agent/config.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/chaos_node/config.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/client/transaction_client/config.h>

#include <yt/yt/core/http/config.h>

#include <yt/yt/library/dynamic_config/config.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

void TMemoryLimit::Register(TRegistrar registrar)
{
    registrar.Parameter("type", &TThis::Type)
        .Default();

    registrar.Parameter("value", &TThis::Value)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        if (config->Type == NNodeTrackerClient::EMemoryLimitType::Static && !config->Value) {
            THROW_ERROR_EXCEPTION("Value should be set for static memory limits");
        }
        if (config->Type != NNodeTrackerClient::EMemoryLimitType::Static && config->Value) {
            THROW_ERROR_EXCEPTION("Value can be set only for static memory limits");
        }
    });
}

void TMemoryLimit::Validate()
{
    if (!Type) {
        THROW_ERROR_EXCEPTION("Memory limit type should be set");
    }
}

////////////////////////////////////////////////////////////////////////////////

void TResourceLimitsConfig::Register(TRegistrar registrar)
{
    // Very low default, override for production use.
    // COMPAT(gritukan)
    registrar.Parameter("total_memory", &TThis::TotalMemory)
        .Alias("memory")
        .GreaterThanOrEqual(0)
        .Default(5_GB);

    registrar.Parameter("user_jobs", &TThis::UserJobs)
        .Default();
    registrar.Parameter("tablet_static", &TThis::TabletStatic)
        .Default();
    registrar.Parameter("tablet_dynamic", &TThis::TabletDynamic)
        .Default();

    registrar.Parameter("memory_limits", &TThis::MemoryLimits)
        .Default();

    registrar.Parameter("free_memory_watermark", &TThis::FreeMemoryWatermark)
        .Default();

    registrar.Parameter("total_cpu", &TThis::TotalCpu)
        .Default();

    registrar.Parameter("node_dedicated_cpu", &TThis::NodeDedicatedCpu)
        .Default();

    registrar.Parameter("cpu_per_tablet_slot", &TThis::CpuPerTabletSlot)
        .Default(1.0);

    registrar.Parameter("node_cpu_weight", &TThis::NodeCpuWeight)
        .GreaterThanOrEqual(0.01)
        .LessThanOrEqual(100)
        .Default(10);

    registrar.Parameter("memory_accounting_tolerance", &TThis::MemoryAccountingTolerance)
        .GreaterThan(0)
        .LessThanOrEqual(1_GB)
        .Default(1_MB);

    registrar.Parameter("memory_accounting_gap", &TThis::MemoryAccountingGap)
        .GreaterThan(0)
        .Default(512_MB);

    registrar.Preprocessor([] (TThis* config) {
        // Default LookupRowsCache memory limit.
        auto lookupRowsCacheLimit = New<TMemoryLimit>();
        lookupRowsCacheLimit->Type = NNodeTrackerClient::EMemoryLimitType::Static;
        lookupRowsCacheLimit->Value = 0;
        config->MemoryLimits[EMemoryCategory::LookupRowsCache] = lookupRowsCacheLimit;
    });

    registrar.Postprocessor([] (TThis* config) {
        if (config->UserJobs) {
            config->MemoryLimits[EMemoryCategory::UserJobs] = config->UserJobs;
        }
        if (config->TabletStatic) {
            config->MemoryLimits[EMemoryCategory::TabletStatic] = config->TabletStatic;
        }
        if (config->TabletDynamic) {
            config->MemoryLimits[EMemoryCategory::TabletDynamic] = config->TabletDynamic;
        }
        // COMPAT(babenko)
        if (config->MemoryLimits[EMemoryCategory::BlobSession]) {
            config->MemoryLimits[EMemoryCategory::PendingDiskWrite] = config->MemoryLimits[EMemoryCategory::BlobSession];
        }
    });
}

void TResourceLimitsConfig::Validate()
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

////////////////////////////////////////////////////////////////////////////////

void TResourceLimitsDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("user_jobs", &TThis::UserJobs)
        .Default();
    registrar.Parameter("tablet_static", &TThis::TabletStatic)
        .Default();
    registrar.Parameter("tablet_dynamic", &TThis::TabletDynamic)
        .Default();

    registrar.Parameter("memory_limits", &TThis::MemoryLimits)
        .Default();
    registrar.Parameter("free_memory_watermark", &TThis::FreeMemoryWatermark)
        .Default();
    registrar.Parameter("node_dedicated_cpu", &TThis::NodeDedicatedCpu)
        .Default();
    registrar.Parameter("cpu_per_tablet_slot", &TThis::CpuPerTabletSlot)
        .Default();

    registrar.Parameter("total_cpu", &TThis::TotalCpu)
        .Default(0);

    registrar.Parameter("use_instance_limits_tracker", &TThis::UseInstanceLimitsTracker)
        .Default(true);

    registrar.Parameter("overrides", &TThis::Overrides)
        .DefaultNew();

    registrar.Postprocessor([] (TThis* config) {
        if (config->UserJobs) {
            config->MemoryLimits[EMemoryCategory::UserJobs] = config->UserJobs;
        }
        if (config->TabletStatic) {
            config->MemoryLimits[EMemoryCategory::TabletStatic] = config->TabletStatic;
        }
        if (config->TabletDynamic) {
            config->MemoryLimits[EMemoryCategory::TabletDynamic] = config->TabletDynamic;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TResourceLimitsOverrides::Register(TRegistrar registrar)
{
    #define XX(name, Name) \
        registrar.Parameter(#name, &TThis::Name) \
            .Default();
    ITERATE_NODE_RESOURCE_LIMITS_DYNAMIC_CONFIG_OVERRIDES(XX)
    #undef XX
}

////////////////////////////////////////////////////////////////////////////////

void TMasterConnectorDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("heartbeat_executor", &TThis::HeartbeatExecutor)
        .Default();

    registrar.Parameter("use_host_objects", &TThis::UseHostObjects)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TProxyingChunkServiceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cost_throttler", &TThis::CostThrottler)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicConfigManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(true);
    registrar.Parameter("update_period", &TThis::UpdatePeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("enable_unrecognized_options_alert", &TThis::EnableUnrecognizedOptionsAlert)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TMasterConnectorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("lease_transaction_timeout", &TThis::LeaseTransactionTimeout)
        .Default();
    registrar.Parameter("lease_transaction_ping_period", &TThis::LeaseTransactionPingPeriod)
        .Default();

    registrar.Parameter("register_retry_period", &TThis::RegisterRetryPeriod)
        .Default();
    registrar.Parameter("register_retry_splay", &TThis::RegisterRetrySplay)
        .Default();
    registrar.Parameter("register_timeout", &TThis::RegisterTimeout)
        .Default();

    registrar.Parameter("heartbeat_period", &TThis::HeartbeatPeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("heartbeat_period_splay", &TThis::HeartbeatPeriodSplay)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("heartbeat_timeout", &TThis::HeartbeatTimeout)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("sync_directories_on_connect", &TThis::SyncDirectoriesOnConnect)
        .Default();
    registrar.Parameter("heartbeat_executor", &TThis::HeartbeatExecutor)
        .Default({
            {
                .Period = TDuration::Seconds(30),
                .Splay = TDuration::Seconds(1),
                .Jitter = 0.0,
            },
            {
                .MinBackoff = TDuration::Seconds(5),
                .MaxBackoff = TDuration::Seconds(60),
                .BackoffMultiplier = 2.0,
            },
        });
    // COMPAT(cherepashka)
    registrar.Postprocessor([] (TThis* config) {
        config->HeartbeatExecutor.Period = config->HeartbeatPeriod;
        config->HeartbeatExecutor.Splay = config->HeartbeatPeriodSplay;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TClusterNodeConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("orchid_cache_update_period", &TThis::OrchidCacheUpdatePeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("data_node", &TThis::DataNode)
        .DefaultNew();
    registrar.Parameter("exec_node", &TThis::ExecNode)
        .Alias("exec_agent")
        .DefaultNew();
    registrar.Parameter("cellar_node", &TThis::CellarNode)
        .DefaultNew();
    registrar.Parameter("tablet_node", &TThis::TabletNode)
        .DefaultNew();
    registrar.Parameter("query_agent", &TThis::QueryAgent)
        .DefaultNew();
    registrar.Parameter("chaos_node", &TThis::ChaosNode)
        .DefaultNew();
    registrar.Parameter("job_resource_manager", &TThis::JobResourceManager)
        .DefaultNew();
    registrar.Parameter("caching_object_service", &TThis::CachingObjectService)
        .Alias("master_cache_service")
        .DefaultNew();
    registrar.Parameter("proxying_chunk_service", &TThis::ProxyingChunkService)
        .DefaultNew();
    registrar.Parameter("timestamp_provider", &TThis::TimestampProvider)
        .Default();
    registrar.Parameter("dry_run", &TThis::DryRun)
        .DefaultNew();
    registrar.Parameter("addresses", &TThis::Addresses)
        .Default();
    registrar.Parameter("tags", &TThis::Tags)
        .Default();
    registrar.Parameter("host_name", &TThis::HostName)
        .Default();
    registrar.Parameter("resource_limits", &TThis::ResourceLimits)
        .DefaultNew();

    registrar.Parameter("resource_limits_update_period", &TThis::ResourceLimitsUpdatePeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("instance_limits_update_period", &TThis::InstanceLimitsUpdatePeriod)
        .Default();

    registrar.Parameter("skynet_http_port", &TThis::SkynetHttpPort)
        .Default(10080);

    registrar.Parameter("cypress_annotations", &TThis::CypressAnnotations)
        .Default(NYTree::BuildYsonNodeFluently()
            .BeginMap()
            .EndMap()
        ->AsMap());

    registrar.Parameter("enable_unrecognized_options_alert", &TThis::EnableUnrecognizedOptionsAlert)
        .Default(false);

    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);

    registrar.Parameter("dynamic_config_manager", &TThis::DynamicConfigManager)
        .DefaultNew();

    registrar.Parameter("exec_node_is_not_data_node", &TThis::ExecNodeIsNotDataNode)
        .Default(false);

    registrar.Parameter("flavors", &TThis::Flavors)
        .Default({
            NNodeTrackerClient::ENodeFlavor::Data,
            NNodeTrackerClient::ENodeFlavor::Exec,
            NNodeTrackerClient::ENodeFlavor::Tablet
        });

    registrar.Parameter("master_connector", &TThis::MasterConnector)
        .DefaultNew();

    registrar.Parameter("network_bandwidth", &TThis::NetworkBandwidth)
        .Default(1250000000);
    registrar.Parameter("throttler_free_bandwidth_ratio", &TThis::ThrottlerFreeBandwidthRatio)
        .InRange(0.0, 1.0)
        .Default(0.1);
    registrar.Parameter("enable_fair_throttler", &TThis::EnableFairThrottler)
        .Default(false);
    registrar.Parameter("in_throttler", &TThis::InThrottler)
        .DefaultNew();
    registrar.Parameter("out_throttler", &TThis::OutThrottler)
        .DefaultNew();
    registrar.Parameter("in_throttlers", &TThis::InThrottlers)
        .Default();
    registrar.Parameter("out_throttlers", &TThis::OutThrottlers)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        NNodeTrackerClient::ValidateNodeTags(config->Tags);

        // COMPAT(gritukan): Drop this code after configs migration.
        if (!config->ResourceLimits->MemoryLimits[EMemoryCategory::UserJobs]) {
            auto& memoryLimit = config->ResourceLimits->MemoryLimits[EMemoryCategory::UserJobs];
            memoryLimit = New<TMemoryLimit>();
            memoryLimit->Type = NNodeTrackerClient::EMemoryLimitType::Dynamic;
        }
        if (!config->ResourceLimits->MemoryLimits[EMemoryCategory::TabletStatic]) {
            auto& memoryLimit = config->ResourceLimits->MemoryLimits[EMemoryCategory::TabletStatic];
            memoryLimit = New<TMemoryLimit>();
            if (config->TabletNode->ResourceLimits->TabletStaticMemory == std::numeric_limits<i64>::max()) {
                memoryLimit->Type = NNodeTrackerClient::EMemoryLimitType::None;
            } else {
                memoryLimit->Type = NNodeTrackerClient::EMemoryLimitType::Static;
                memoryLimit->Value = config->TabletNode->ResourceLimits->TabletStaticMemory;
            }
        }
        if (!config->ResourceLimits->MemoryLimits[EMemoryCategory::TabletDynamic]) {
            auto& memoryLimit = config->ResourceLimits->MemoryLimits[EMemoryCategory::TabletDynamic];
            memoryLimit = New<TMemoryLimit>();
            if (config->TabletNode->ResourceLimits->TabletDynamicMemory == std::numeric_limits<i64>::max()) {
                memoryLimit->Type = NNodeTrackerClient::EMemoryLimitType::None;
            } else {
                memoryLimit->Type = NNodeTrackerClient::EMemoryLimitType::Static;
                memoryLimit->Value = config->TabletNode->ResourceLimits->TabletDynamicMemory;
            }
        }
        if (!config->ResourceLimits->FreeMemoryWatermark) {
            config->ResourceLimits->FreeMemoryWatermark = 0;
            auto freeMemoryWatermarkNode = config->ExecNode->SlotManager->JobEnvironment->AsMap()->FindChild("free_memory_watermark");
            if (freeMemoryWatermarkNode) {
                config->ResourceLimits->FreeMemoryWatermark = freeMemoryWatermarkNode->GetValue<i64>();
            }
        }
        if (!config->ResourceLimits->NodeDedicatedCpu) {
            config->ResourceLimits->NodeDedicatedCpu = 2; // Old default.
            auto nodeDedicatedCpuNode = config->ExecNode->SlotManager->JobEnvironment->AsMap()->FindChild("node_dedicated_cpu");
            if (nodeDedicatedCpuNode) {
                config->ResourceLimits->NodeDedicatedCpu = nodeDedicatedCpuNode->GetValue<double>();
            }
        }
        if (!config->InstanceLimitsUpdatePeriod) {
            auto resourceLimitsUpdatePeriodNode = config->ExecNode->SlotManager->JobEnvironment->AsMap()->FindChild("resource_limits_update_period");
            if (resourceLimitsUpdatePeriodNode) {
                config->InstanceLimitsUpdatePeriod = NYTree::ConvertTo<std::optional<TDuration>>(resourceLimitsUpdatePeriodNode);
            }
        }

        config->DynamicConfigManager->IgnoreConfigAbsence = true;

        // COMPAT(gritukan)
        if (!config->MasterConnector->LeaseTransactionTimeout) {
            config->MasterConnector->LeaseTransactionTimeout = config->DataNode->LeaseTransactionTimeout;
        }
        if (!config->MasterConnector->LeaseTransactionPingPeriod) {
            config->MasterConnector->LeaseTransactionPingPeriod = config->DataNode->LeaseTransactionPingPeriod;
        }
        if (!config->MasterConnector->RegisterRetryPeriod) {
            config->MasterConnector->RegisterRetryPeriod = config->DataNode->RegisterRetryPeriod;
        }
        if (!config->MasterConnector->RegisterRetrySplay) {
            config->MasterConnector->RegisterRetrySplay = config->DataNode->RegisterRetrySplay;
        }
        if (!config->MasterConnector->RegisterTimeout) {
            config->MasterConnector->RegisterTimeout = config->DataNode->RegisterTimeout;
        }
        if (!config->MasterConnector->SyncDirectoriesOnConnect) {
            config->MasterConnector->SyncDirectoriesOnConnect = config->DataNode->SyncDirectoriesOnConnect;
        }

        if (!config->TcpDispatcher->NetworkBandwidth) {
            config->TcpDispatcher->NetworkBandwidth = config->NetworkBandwidth;
        }
    });
}

NHttp::TServerConfigPtr TClusterNodeConfig::CreateSkynetHttpServerConfig()
{
    auto config = New<NHttp::TServerConfig>();
    config->Port = SkynetHttpPort;
    config->BindRetryCount = BusServer->BindRetryCount;
    config->BindRetryBackoff = BusServer->BindRetryBackoff;
    config->ServerName = "HttpSky";
    return config;
}

////////////////////////////////////////////////////////////////////////////////

void TClusterNodeDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("config_annotation", &TThis::ConfigAnnotation)
        .Optional();
    registrar.Parameter("resource_limits", &TThis::ResourceLimits)
        .DefaultNew();
    registrar.Parameter("data_node", &TThis::DataNode)
        .DefaultNew();
    registrar.Parameter("cellar_node", &TThis::CellarNode)
        .DefaultNew();
    registrar.Parameter("tablet_node", &TThis::TabletNode)
        .DefaultNew();
    registrar.Parameter("query_agent", &TThis::QueryAgent)
        .DefaultNew();
    registrar.Parameter("exec_node", &TThis::ExecNode)
        .Alias("exec_agent")
        .DefaultNew();
    registrar.Parameter("job_resource_manager", &TThis::JobResourceManager)
        .DefaultNew();
    registrar.Parameter("caching_object_service", &TThis::CachingObjectService)
        .DefaultNew();
    registrar.Parameter("master_connector", &TThis::MasterConnector)
        .DefaultNew();
    registrar.Parameter("in_throttlers", &TThis::InThrottlers)
        .Default();
    registrar.Parameter("out_throttlers", &TThis::OutThrottlers)
        .Default();
    registrar.Parameter("io_tracker", &TThis::IOTracker)
        .DefaultNew();
    registrar.Parameter("rpc_server", &TThis::RpcServer)
        .DefaultNew();
    registrar.Parameter("chunk_replica_cache", &TThis::ChunkReplicaCacheConfig)
        .DefaultNew();
    registrar.Parameter("throttler_free_bandwidth_ratio", &TThis::ThrottlerFreeBandwidthRatio)
        .InRange(0.0, 1.0)
        .Optional();
    registrar.Parameter("use_porto_network_limit_in_throttler", &TThis::UsePortoNetworkLimitInThrottler)
        .Default(true);
    registrar.Parameter("total_memory_limit_exceeded_threshold", &TThis::TotalMemoryLimitExceededThreshold)
        .Default(1.0);
    registrar.Parameter("memory_usage_is_close_to_limit_threshold", &TThis::MemoryUsageIsCloseToLimitThreshold)
        .Default(1.0);
    registrar.Parameter("memory_limit_exceeded_for_category_threshold", &TThis::MemoryLimitExceededForCategoryThreshold)
        .Default(1.1);

}

////////////////////////////////////////////////////////////////////////////////

void TChunkReplicaCacheDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("expiration_time", &TThis::ExpirationTime)
        .GreaterThanOrEqual(TDuration::Zero())
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
