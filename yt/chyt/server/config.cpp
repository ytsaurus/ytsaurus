#include "config.h"

#include "clickhouse_config.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

TCompositeSettings::TCompositeSettings()
{
    RegisterParameter("default_yson_format", DefaultYsonFormat)
        .Default(EExtendedYsonFormat::Binary);
}

////////////////////////////////////////////////////////////////////////////////

TDynamicTableSettings::TDynamicTableSettings()
{
    RegisterParameter("enable_dynamic_store_read", EnableDynamicStoreRead)
        .Default(true);

    RegisterParameter("write_retry_count", WriteRetryCount)
        .Default(5);

    RegisterParameter("write_retry_backoff", WriteRetryBackoff)
        .Default(TDuration::Seconds(1));

    RegisterParameter("max_rows_per_write", MaxRowsPerWrite)
        .Default(100'000);

    RegisterParameter("transaction_atomicity", TransactionAtomicity)
        .Default(NTransactionClient::EAtomicity::Full);

    RegisterParameter("fetch_from_tablets", FetchFromTablets)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

TTestingSettings::TTestingSettings()
{
    RegisterParameter("enable_key_condition_filtering", EnableKeyConditionFiltering)
        .Default(true);
    RegisterParameter("make_upper_bound_inclusive", MakeUpperBoundInclusive)
        .Default(true);

    RegisterParameter("throw_exception_in_distributor", ThrowExceptionInDistributor)
        .Default(false);
    RegisterParameter("throw_exception_in_subquery", ThrowExceptionInSubquery)
        .Default(false);
    RegisterParameter("subquery_allocation_size", SubqueryAllocationSize)
        .Default(0);

    RegisterParameter("hang_control_invoker", HangControlInvoker)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

TExecutionSettings::TExecutionSettings()
{
    RegisterParameter("query_depth_limit", QueryDepthLimit)
        .GreaterThanOrEqual(-1)
        .Default(0);

    RegisterParameter("min_data_weight_per_secondary_query", MinDataWeightPerSecondaryQuery)
        .GreaterThanOrEqual(-1)
        .Default(0);

    RegisterParameter("join_node_limit", JoinNodeLimit)
        .GreaterThanOrEqual(-1)
        .Default(0);
    RegisterParameter("select_node_limit", SelectNodeLimit)
        .GreaterThanOrEqual(-1)
        .Default(0);

    RegisterParameter("join_policy", JoinPolicy)
        .Default(EJoinPolicy::DistributeInitial);

    RegisterParameter("select_policy", SelectPolicy)
        .Default(ESelectPolicy::DistributeInitial);

    RegisterParameter("distribution_seed", DistributionSeed)
        .Default(42);

    RegisterParameter("input_streams_per_secondary_query", InputStreamsPerSecondaryQuery)
        .GreaterThanOrEqual(-1)
        .Default(0);

    RegisterParameter("optimize_query_processing_stage", OptimizeQueryProcessingStage)
        .Default(true);
    RegisterParameter("filter_joined_subquery_by_sort_key", FilterJoinedSubqueryBySortKey)
        .Default(true);

    RegisterParameter("allow_switch_to_sorted_pool", AllowSwitchToSortedPool)
        .Default(true);
    RegisterParameter("allow_key_truncating", AllowKeyTruncating)
        .Default(true);

    RegisterParameter("keep_nulls_in_right_or_full_join", KeepNullsInRightOrFullJoin)
        .Default(true);

    RegisterParameter("distributed_insert_stage", DistributedInsertStage)
        .Default(EDistributedInsertStage::WithMergeableState);
}

////////////////////////////////////////////////////////////////////////////////

TQuerySettings::TQuerySettings()
{
    RegisterParameter("enable_columnar_read", EnableColumnarRead)
        .Default(true);

    RegisterParameter("enable_computed_column_deduction", EnableComputedColumnDeduction)
        .Default(true);

    RegisterParameter("deduced_statement_mode", DeducedStatementMode)
        .Default(EDeducedStatementMode::In);

    RegisterParameter("use_block_sampling", UseBlockSampling)
        .Default(false);

    RegisterParameter("log_key_condition_details", LogKeyConditionDetails)
        .Default(false);

    RegisterParameter("convert_row_batches_in_worker_thread_pool", ConvertRowBatchesInWorkerThreadPool)
        .Default(true);

    RegisterParameter("infer_dynamic_table_ranges_from_pivot_keys", InferDynamicTableRangesFromPivotKeys)
        .Default(true);

    RegisterParameter("composite", Composite)
        .DefaultNew();

    RegisterParameter("dynamic_table", DynamicTable)
        .DefaultNew();

    RegisterParameter("testing", Testing)
        .DefaultNew();

    RegisterParameter("execution", Execution)
        .DefaultNew();

    RegisterParameter("table_reader", TableReader)
        .DefaultNew();

    RegisterParameter("enable_reader_tracing", EnableReaderTracing)
        .Default(false);
}

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

TShowTablesConfig::TShowTablesConfig()
{
    RegisterParameter("roots", Roots)
        .Default();

    RegisterPostprocessor([&] {
        const int MaxRootCount = 10;
        if (Roots.size() > MaxRootCount) {
            THROW_ERROR_EXCEPTION("Maximum number of roots for SHOW TABLES exceeded: %v > %v", Roots.size(), MaxRootCount);
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
    RegisterParameter("use_columnar_statistics", UseColumnarStatistics)
        .Default(true);
    RegisterParameter("min_slice_data_weight", MinSliceDataWeight)
        .Default(1_MB);
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
        .Default(TDuration::MilliSeconds(300));
    RegisterParameter("window_codicil_watermark", WindowCodicilWatermark)
        .Default(0);
    RegisterParameter("window_width", WindowWidth)
        .Default(TDuration::Minutes(15));
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

TQueryStatisticsReporterConfig::TQueryStatisticsReporterConfig()
{
    RegisterParameter("distributed_queries_handler", DistributedQueriesHandler)
        .DefaultNew();
    RegisterParameter("secondary_queries_handler", SecondaryQueriesHandler)
        .DefaultNew();
    RegisterParameter("ancestor_query_ids_handler", AncestorQueryIdsHandler)
        .DefaultNew();

    RegisterParameter("user", User)
        .Default("yt-clickhouse");

    RegisterPreprocessor([&] {
        Enabled = false;
    });
}

////////////////////////////////////////////////////////////////////////////////

TGossipConfig::TGossipConfig()
{
    RegisterParameter("period", Period)
        .Default(TDuration::Seconds(1));
    RegisterParameter("timeout", Timeout)
        .Default(TDuration::Seconds(1));

    RegisterParameter("unknown_instance_age_threshold", UnknownInstanceAgeThreshold)
        .Default(TDuration::Seconds(1));
    RegisterParameter("unknown_instance_ping_limit", UnknownInstancePingLimit)
        .Default(10);
    RegisterParameter("ping_banned", PingBanned)
        .Default(true);
    RegisterParameter("allow_unban", AllowUnban)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

TInvokerLivenessCheckerConfig::TInvokerLivenessCheckerConfig()
{
    RegisterParameter("enabled", Enabled)
        .Default(true);
    RegisterParameter("period", Period)
        .Default(TDuration::Seconds(30));
    RegisterParameter("timeout", Timeout)
        .Default(TDuration::Seconds(5));
}

////////////////////////////////////////////////////////////////////////////////

TYtConfig::TYtConfig()
{
    RegisterParameter("clique_id", CliqueId)
        .Default();
    RegisterParameter("instance_id", InstanceId)
        .Default();
    RegisterParameter("address", Address)
        .Default();

    RegisterParameter("client_cache", ClientCache)
        .DefaultNew();

    RegisterParameter("user_agent_black_list", UserAgentBlackList)
        .Default();

    RegisterParameter("validate_operation_access", ValidateOperationAccess)
        .Default();
    RegisterParameter("operation_acl_update_period", OperationAclUpdatePeriod)
        .Default();

    RegisterParameter("security_manager", SecurityManager)
        .DefaultNew();

    RegisterParameter("user", User)
        .Default("yt-clickhouse");

    RegisterParameter("show_tables", ShowTables)
        .DefaultNew();

    RegisterParameter("memory_watchdog", MemoryWatchdog)
        .Default(New<TMemoryWatchdogConfig>());

    RegisterParameter("table_writer", TableWriter)
        .DefaultNew();

    RegisterParameter("discovery", Discovery)
        .DefaultNew("//sys/clickhouse/cliques");

    RegisterParameter("gossip", Gossip)
        .DefaultNew();

    RegisterParameter("control_invoker_checker", ControlInvokerChecker)
        .DefaultNew();

    RegisterParameter("permission_cache", PermissionCache)
        .DefaultNew();

    RegisterParameter("process_list_snapshot_update_period", ProcessListSnapshotUpdatePeriod)
        .Default(TDuration::Seconds(1));

    RegisterParameter("worker_thread_count", WorkerThreadCount)
        .Default(8);

    RegisterParameter("fetcher_thread_count", FetcherThreadCount)
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

    RegisterParameter("enable_dynamic_tables", EnableDynamicTables)
        .Default(false);

    RegisterParameter("total_memory_tracker_update_period", TotalMemoryTrackerUpdatePeriod)
        .Default(TDuration::MilliSeconds(300));

    RegisterParameter("query_settings", QuerySettings)
        .Alias("settings")
        .DefaultNew();

    RegisterParameter("table_reader", TableReader)
        .DefaultNew();

    RegisterParameter("table_attribute_cache", TableAttributeCache)
        .DefaultNew();

    RegisterParameter("table_columnar_statistics_cache", TableColumnarStatisticsCache)
        .DefaultNew();

    RegisterParameter("query_statistics_reporter", QueryStatisticsReporter)
        .DefaultNew();

    RegisterPreprocessor([&] {
        TableAttributeCache->ExpireAfterAccessTime = TDuration::Minutes(2);
        TableAttributeCache->ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(20);
        TableAttributeCache->ExpireAfterFailedUpdateTime = TDuration::Zero();
        TableAttributeCache->RefreshTime = TDuration::Seconds(15);
        TableAttributeCache->BatchUpdate = true;

        PermissionCache->RefreshUser = CacheUserName;
        PermissionCache->AlwaysUseRefreshUser = false;

        TableReader->GroupSize = 20_MB;
        TableReader->WindowSize = 70_MB;
        TableReader->MaxBufferSize = 200_MB;
        TableReader->BlockRpcHedgingDelay = TDuration::MilliSeconds(50);
        TableReader->MetaRpcHedgingDelay = TDuration::MilliSeconds(10);
        TableReader->CancelPrimaryBlockRpcRequestOnHedging = true;

        // Disable background updates since we deal with consistency issues by checking cached table revision.
        TableColumnarStatisticsCache->RefreshTime = std::nullopt;
        TableColumnarStatisticsCache->ExpireAfterSuccessfulUpdateTime = TDuration::Hours(6);
        TableColumnarStatisticsCache->ExpireAfterAccessTime = TDuration::Hours(6);

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

TLauncherConfig::TLauncherConfig()
{
    RegisterParameter("version", Version)
        .Default(0);
}

////////////////////////////////////////////////////////////////////////////////

TMemoryConfig::TMemoryConfig()
{
    RegisterParameter("reader", Reader)
        .Default();
    RegisterParameter("uncompressed_block_cache", UncompressedBlockCache)
        .Default();
    RegisterParameter("compressed_block_cache", CompressedBlockCache)
        .Default();
    RegisterParameter("chunk_meta_cache", ChunkMetaCache)
        .Default();
    RegisterParameter("memory_limit", MemoryLimit)
        .Default();
    RegisterParameter("max_server_memory_usage", MaxServerMemoryUsage)
        .Default();
    RegisterParameter("watchdog_oom_watermark", WatchdogOomWatermark)
        .Default();
    RegisterParameter("watchdog_oom_window_watermark", WatchdogOomWindowWatermark)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TClickHouseServerBootstrapConfig::TClickHouseServerBootstrapConfig()
{
    RegisterParameter("cluster_connection", ClusterConnection);

    RegisterParameter("yt", Yt)
        .DefaultNew();

    RegisterParameter("graceful_interruption_delay", GracefulInterruptionDelay)
        .Default(TDuration::Seconds(2))
        // COMPAT(dakovalkov)
        .Alias("interruption_graceful_timeout");

    RegisterParameter("interruption_timeout", InterruptionTimeout)
        .Default(TDuration::Minutes(5));

    RegisterParameter("launcher", Launcher)
        .DefaultNew();

    RegisterParameter("clickhouse", ClickHouse)
        .Alias("engine")
        .DefaultNew();

    RegisterParameter("cpu_limit", CpuLimit)
        .Default();

    RegisterParameter("memory", Memory)
        .Default();

    RegisterPreprocessor([&] {
        Jaeger->ServiceName = "clickhouse_server";
        Jaeger->CollectorChannelConfig = New<NRpc::NGrpc::TChannelConfig>();
        Jaeger->CollectorChannelConfig->Address = "yt.c.jaeger.yandex-team.ru:14250";
    });

    RegisterPostprocessor([&] {
        if (CpuLimit) {
            Yt->CpuLimit = CpuLimit;
        }

        if (Memory) {
            if (Memory->Reader) {
                Yt->TotalReaderMemoryLimit = *Memory->Reader;
            }
            if (Memory->MemoryLimit) {
                Yt->MemoryWatchdog->MemoryLimit = *Memory->MemoryLimit;
            }
            if (Memory->WatchdogOomWatermark) {
                Yt->MemoryWatchdog->CodicilWatermark = *Memory->WatchdogOomWatermark;
            }
            if (Memory->WatchdogOomWindowWatermark) {
                Yt->MemoryWatchdog->WindowCodicilWatermark = *Memory->WatchdogOomWindowWatermark;
            }

            auto initDefault = [] (auto& config) {
                if (!config) {
                    config = New<typename std::remove_reference_t<decltype(config)>::TUnderlying>();
                }
            };

            if (Memory->UncompressedBlockCache) {
                initDefault(ClusterConnection->BlockCache);
                ClusterConnection->BlockCache->UncompressedData->Capacity = *Memory->UncompressedBlockCache;
            }
            if (Memory->CompressedBlockCache) {
                initDefault(ClusterConnection->BlockCache);
                ClusterConnection->BlockCache->CompressedData->Capacity = *Memory->CompressedBlockCache;
            }
            if (Memory->ChunkMetaCache) {
                initDefault(ClusterConnection->ChunkMetaCache);
                ClusterConnection->ChunkMetaCache->Capacity = *Memory->ChunkMetaCache;
            }

            if (Memory->MaxServerMemoryUsage) {
                ClickHouse->MaxServerMemoryUsage = *Memory->MaxServerMemoryUsage;
            }
        }
    });
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
