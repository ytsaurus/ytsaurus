#include "config.h"

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

TMemoryLimit::TMemoryLimit()
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

void TMemoryLimit::Validate()
{
    if (!Type) {
        THROW_ERROR_EXCEPTION("Memory limit type should be set");
    }
}

////////////////////////////////////////////////////////////////////////////////

TResourceLimitsConfig::TResourceLimitsConfig()
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

    RegisterParameter("memory_accounting_gap", MemoryAccountingGap)
        .GreaterThan(0)
        .Default(512_MB);

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

TResourceLimitsDynamicConfig::TResourceLimitsDynamicConfig()
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

    RegisterParameter("use_instance_limits_tracker", UseInstanceLimitsTracker)
        .Default(true);

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

////////////////////////////////////////////////////////////////////////////////

TMasterConnectorDynamicConfig::TMasterConnectorDynamicConfig()
{
    RegisterParameter("incremental_heartbeat_period", IncrementalHeartbeatPeriod)
        .Default();
    RegisterParameter("incremental_heartbeat_period_splay", IncrementalHeartbeatPeriodSplay)
        .Default();
    RegisterParameter("heartbeat_period", HeartbeatPeriod)
        .Default();
    RegisterParameter("heartbeat_period_splay", HeartbeatPeriodSplay)
        .Default();
    RegisterParameter("use_host_objects", UseHostObjects)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

TBatchingChunkServiceConfig::TBatchingChunkServiceConfig()
{
    RegisterParameter("max_batch_delay", MaxBatchDelay)
        .Default(TDuration::Zero());
    RegisterParameter("max_batch_cost", MaxBatchCost)
        .Default(1000);
    RegisterParameter("cost_throttler", CostThrottler)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

TDynamicConfigManagerConfig::TDynamicConfigManagerConfig()
{
    RegisterParameter("enabled", Enabled)
        .Default(true);
    RegisterParameter("update_period", UpdatePeriod)
        .Default(TDuration::Seconds(30));
    RegisterParameter("enable_unrecognized_options_alert", EnableUnrecognizedOptionsAlert)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

TClusterNodeConnectionConfig::TClusterNodeConnectionConfig()
{
    RegisterPreprocessor([&] {
        // Provide a lower channel cache TTL to reduce the total number
        // of inter-cluster connections. This also gets propagated to job proxy config
        // and helps decreasing memory footprint.
        IdleChannelTtl = TDuration::Seconds(60);
    });
}

////////////////////////////////////////////////////////////////////////////////

TMasterConnectorConfig::TMasterConnectorConfig()
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

////////////////////////////////////////////////////////////////////////////////

TClusterNodeConfig::TClusterNodeConfig()
{
    RegisterParameter("orchid_cache_update_period", OrchidCacheUpdatePeriod)
        .Default(TDuration::Seconds(5));
    RegisterParameter("cluster_connection", ClusterConnection);
    RegisterParameter("data_node", DataNode)
        .DefaultNew();
    RegisterParameter("exec_node", ExecNode)
        .Alias("exec_agent")
        .DefaultNew();
    RegisterParameter("cellar_node", CellarNode)
        .DefaultNew();
    RegisterParameter("tablet_node", TabletNode)
        .DefaultNew();
    RegisterParameter("query_agent", QueryAgent)
        .DefaultNew();
    RegisterParameter("chaos_node", ChaosNode)
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
    RegisterParameter("host_name", HostName)
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
        .Default({
            NNodeTrackerClient::ENodeFlavor::Data,
            NNodeTrackerClient::ENodeFlavor::Exec,
            NNodeTrackerClient::ENodeFlavor::Tablet
        });

    RegisterParameter("master_connector", MasterConnector)
        .DefaultNew();

    RegisterParameter("network_bandwidth", NetworkBandwidth)
        .Default(1250000000);
    RegisterParameter("in_throttlers", InThrottlers)
        .Default();
    RegisterParameter("out_throttlers", OutThrottlers)
        .Default();

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
            auto freeMemoryWatermarkNode = ExecNode->SlotManager->JobEnvironment->AsMap()->FindChild("free_memory_watermark");
            if (freeMemoryWatermarkNode) {
                ResourceLimits->FreeMemoryWatermark = freeMemoryWatermarkNode->GetValue<i64>();
            }
        }
        if (!ResourceLimits->NodeDedicatedCpu) {
            ResourceLimits->NodeDedicatedCpu = 2; // Old default.
            auto nodeDedicatedCpuNode = ExecNode->SlotManager->JobEnvironment->AsMap()->FindChild("node_dedicated_cpu");
            if (nodeDedicatedCpuNode) {
                ResourceLimits->NodeDedicatedCpu = nodeDedicatedCpuNode->GetValue<double>();
            }
        }
        if (!ResourceLimits->CpuPerTabletSlot) {
            ResourceLimits->CpuPerTabletSlot = ExecNode->JobController->CpuPerTabletSlot;
        }
        if (!InstanceLimitsUpdatePeriod) {
            auto resourceLimitsUpdatePeriodNode = ExecNode->SlotManager->JobEnvironment->AsMap()->FindChild("resource_limits_update_period");
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

TClusterNodeDynamicConfig::TClusterNodeDynamicConfig()
{
    RegisterParameter("config_annotation", ConfigAnnotation)
        .Optional();
    RegisterParameter("resource_limits", ResourceLimits)
        .DefaultNew();
    RegisterParameter("data_node", DataNode)
        .DefaultNew();
    RegisterParameter("cellar_node", CellarNode)
        .DefaultNew();
    RegisterParameter("tablet_node", TabletNode)
        .DefaultNew();
    RegisterParameter("query_agent", QueryAgent)
        .DefaultNew();
    RegisterParameter("exec_node", ExecNode)
        .Alias("exec_agent")
        .DefaultNew();
    RegisterParameter("caching_object_service", CachingObjectService)
        .DefaultNew();
    RegisterParameter("master_connector", MasterConnector)
        .DefaultNew();
    RegisterParameter("in_throttlers", InThrottlers)
        .Default();
    RegisterParameter("out_throttlers", OutThrottlers)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
