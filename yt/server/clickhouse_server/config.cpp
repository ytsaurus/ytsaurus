#include "config.h"

#include "clickhouse_config.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

THealthCheckerConfig::THealthCheckerConfig()
{
    RegisterParameter("period", Period)
        .Default(TDuration::Minutes(1));
    RegisterParameter("timeout", Timeout)
        .Default();
    RegisterParameter("queries", Queries)
        .Default();

    RegisterPostprocessor([&] {
        if (Timeout == TDuration::Zero()) {
            Timeout = Period / std::max<double>(1.0, Queries.size()) * 0.95;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TSubqueryConfig::TSubqueryConfig()
{
    RegisterParameter("chunk_slice_fetcher", ChunkSliceFetcher)
        .DefaultNew();
    RegisterParameter("max_job_count_for_pool", MaxJobCountForPool)
        .Default(1'000'000);
    RegisterParameter("min_data_weight_per_thread", MinDataWeightPerThread)
        .Default(64_MB);
    RegisterParameter("max_chunks_per_fetch", MaxChunksPerFetch)
        .Default(100'000);
    RegisterParameter("max_chunks_per_locate_request", MaxChunksPerLocateRequest)
        .Default(10'000);
    RegisterParameter("max_data_weight_per_subquery", MaxDataWeightPerSubquery)
        .Default(50_GB);
}

////////////////////////////////////////////////////////////////////////////////

TMemoryWatchdogConfig::TMemoryWatchdogConfig()
{
    // Default is effective infinity.
    RegisterParameter("memory_limit", MemoryLimit)
        .Default(1_TB);
    RegisterParameter("codicil_watermark", CodicilWatermark)
        .Default(0);
    RegisterParameter("period", Period)
        .Default(TDuration::Seconds(1));
}

////////////////////////////////////////////////////////////////////////////////

TSecurityManagerConfig::TSecurityManagerConfig()
{
    RegisterParameter("enable", Enable)
        .Default(true);
    RegisterParameter("operation_acl_update_period", OperationAclUpdatePeriod)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

TYtConfig::TYtConfig()
{
    RegisterParameter("clique_id", CliqueId)
        .Default();
    RegisterParameter("instance_id", InstanceId)
        .Default();

    RegisterParameter("client_cache", ClientCache)
        .DefaultNew();

    RegisterParameter("validate_operation_access", ValidateOperationAccess)
        .Default();
    RegisterParameter("operation_acl_update_period", OperationAclUpdatePeriod)
        .Default();

    RegisterParameter("security_manager", SecurityManager)
        .DefaultNew();

    RegisterParameter("user", User)
        .Default("yt-clickhouse");

    RegisterParameter("profiling_period", ProfilingPeriod)
        .Default(TDuration::Seconds(1));

    RegisterParameter("memory_watchdog", MemoryWatchdog)
        .Default(New<TMemoryWatchdogConfig>());

    RegisterParameter("table_writer_config", TableWriterConfig)
        .DefaultNew();

    RegisterParameter("discovery", Discovery)
        .DefaultNew("//sys/clickhouse/cliques");

    RegisterParameter("gossip_period", GossipPeriod)
        .Default(TDuration::Seconds(1));

    RegisterParameter("unknown_instance_age_threshold", UnknownInstanceAgeThreshold)
        .Default(TDuration::Seconds(1));

    RegisterParameter("unknown_instance_ping_limit", UnknownInstancePingLimit)
        .Default(10);

    RegisterParameter("permission_cache", PermissionCache)
        .DefaultNew();

    RegisterParameter("process_list_snapshot_update_period", ProcessListSnapshotUpdatePeriod)
        .Default(TDuration::Seconds(1));

    RegisterParameter("worker_thread_count", WorkerThreadCount)
        .Default(8);

    RegisterParameter("cpu_limit", CpuLimit)
        .Default();

    RegisterParameter("subquery", Subquery)
        .DefaultNew();

    RegisterParameter("create_table_default_attributes", CreateTableDefaultAttributes)
        .MergeBy(NYTree::EMergeStrategy::Combine)
        .Default(NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("optimize_for").Value("scan")
        .EndMap());

    RegisterParameter("total_reader_memory_limit", TotalReaderMemoryLimit)
        .Default(20_GB);
    RegisterParameter("reader_memory_requirement", ReaderMemoryRequirement)
        .Default(500_MB);

    RegisterParameter("health_checker", HealthChecker)
        .DefaultNew();

    RegisterPreprocessor([&] {
        PermissionCache->ExpireAfterAccessTime = TDuration::Minutes(2);
        PermissionCache->ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(20);
        PermissionCache->ExpireAfterFailedUpdateTime = TDuration::Zero();
        PermissionCache->RefreshTime = TDuration::Seconds(15);
        PermissionCache->BatchUpdate = true;
        PermissionCache->RefreshUser = CacheUserName;
        PermissionCache->AlwaysUseRefreshUser = false;
    });

    RegisterParameter("table_attribute_cache", TableAttributeCache)
        .DefaultNew();

    RegisterPreprocessor([&] {
        TableAttributeCache->ExpireAfterAccessTime = TDuration::Minutes(2);
        TableAttributeCache->ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(20);
        TableAttributeCache->ExpireAfterFailedUpdateTime = TDuration::Zero();
        TableAttributeCache->RefreshTime = TDuration::Seconds(15);
        TableAttributeCache->BatchUpdate = true;
    });

    RegisterPostprocessor([&] {
        if (ValidateOperationAccess) {
            SecurityManager->Enable = *ValidateOperationAccess;
        }
        if (OperationAclUpdatePeriod) {
            SecurityManager->OperationAclUpdatePeriod = *OperationAclUpdatePeriod;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TClickHouseServerBootstrapConfig::TClickHouseServerBootstrapConfig()
{
    RegisterParameter("cluster_connection", ClusterConnection);

    RegisterParameter("yt", Yt)
        .DefaultNew();

    RegisterParameter("interruption_graceful_timeout", InterruptionGracefulTimeout)
        .Default(TDuration::Seconds(2));

    RegisterParameter("clickhouse", ClickHouse)
        .Alias("engine")
        .DefaultNew();
}

TPorts TClickHouseServerBootstrapConfig::GetPorts() const
{
    return TPorts{
        .Monitoring = MonitoringPort,
        .Rpc = RpcPort,
        .Http = ClickHouse->HttpPort,
        .Tcp = ClickHouse->TcpPort
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
