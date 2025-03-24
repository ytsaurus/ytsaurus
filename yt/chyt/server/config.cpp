#include "config.h"

#include "clickhouse_config.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/library/tracing/jaeger/tracer.h>

#include <yt/yt/library/re2/re2.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

TCompositeSettingsPtr TCompositeSettings::Create(bool convertUnsupportedTypesToString)
{
    auto settings = New<TCompositeSettings>();
    settings->ConvertUnsupportedTypesToString = convertUnsupportedTypesToString;
    return settings;
}

void TCompositeSettings::Register(TRegistrar registrar)
{
    registrar.Parameter("default_yson_format", &TThis::DefaultYsonFormat)
        .Default(EExtendedYsonFormat::Binary);

    registrar.Parameter("convert_unsupported_types_to_string", &TThis::ConvertUnsupportedTypesToString)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicTableSettings::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_dynamic_store_read", &TThis::EnableDynamicStoreRead)
        .Default(true);

    registrar.Parameter("write_retry_count", &TThis::WriteRetryCount)
        .Default(5);

    registrar.Parameter("write_retry_backoff", &TThis::WriteRetryBackoff)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("max_rows_per_write", &TThis::MaxRowsPerWrite)
        .Default(100'000);

    registrar.Parameter("transaction_atomicity", &TThis::TransactionAtomicity)
        .Default(NTransactionClient::EAtomicity::Full);

    registrar.Parameter("fetch_from_tablets", &TThis::FetchFromTablets)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TTestingSettings::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_key_condition_filtering", &TThis::EnableKeyConditionFiltering)
        .Default(true);
    registrar.Parameter("make_upper_bound_inclusive", &TThis::MakeUpperBoundInclusive)
        .Default(true);

    registrar.Parameter("throw_exception_in_distributor", &TThis::ThrowExceptionInDistributor)
        .Default(false);
    registrar.Parameter("throw_exception_in_subquery", &TThis::ThrowExceptionInSubquery)
        .Default(false);
    registrar.Parameter("subquery_allocation_size", &TThis::SubqueryAllocationSize)
        .Default(0);

    registrar.Parameter("hang_control_invoker", &TThis::HangControlInvoker)
        .Default(false);

    registrar.Parameter("local_clique_size", &TThis::LocalCliqueSize)
        .Default(0);

    registrar.Parameter("check_chyt_banned", &TThis::CheckChytBanned)
        .Default(true);

    registrar.Parameter("chunk_spec_fetcher_breakpoint", &TThis::ChunkSpecFetcherBreakpoint)
        .Default();
    registrar.Parameter("input_stream_factory_breakpoint", &TThis::InputStreamFactoryBreakpoint)
        .Default();
    registrar.Parameter("concat_table_range_breakpoint", &TThis::ConcatTableRangeBreakpoint)
        .Default();
    registrar.Parameter("list_dirs_breakpoint", &TThis::ListDirsBreakpoint)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TExecutionSettings::Register(TRegistrar registrar)
{
    registrar.Parameter("query_depth_limit", &TThis::QueryDepthLimit)
        .GreaterThanOrEqual(-1)
        .Default(0);

    registrar.Parameter("min_data_weight_per_secondary_query", &TThis::MinDataWeightPerSecondaryQuery)
        .GreaterThanOrEqual(-1)
        .Default(0);

    registrar.Parameter("join_node_limit", &TThis::JoinNodeLimit)
        .GreaterThanOrEqual(-1)
        .Default(0);
    registrar.Parameter("select_node_limit", &TThis::SelectNodeLimit)
        .GreaterThanOrEqual(-1)
        .Default(0);

    registrar.Parameter("join_policy", &TThis::JoinPolicy)
        .Default(EJoinPolicy::DistributeInitial);

    registrar.Parameter("select_policy", &TThis::SelectPolicy)
        .Default(ESelectPolicy::DistributeInitial);

    registrar.Parameter("distribute_only_global_and_sorted_join", &TThis::DistributeOnlyGlobalAndSortedJoin)
        .Default(true);

    registrar.Parameter("distribution_seed", &TThis::DistributionSeed)
        .Default(42);

    registrar.Parameter("input_streams_per_secondary_query", &TThis::InputStreamsPerSecondaryQuery)
        .GreaterThanOrEqual(-1)
        .Default(0);

    registrar.Parameter("optimize_query_processing_stage", &TThis::OptimizeQueryProcessingStage)
        .Default(true);
    registrar.Parameter("filter_joined_subquery_by_sort_key", &TThis::FilterJoinedSubqueryBySortKey)
        .Default(true);

    registrar.Parameter("allow_switch_to_sorted_pool", &TThis::AllowSwitchToSortedPool)
        .Default(true);
    registrar.Parameter("allow_key_truncating", &TThis::AllowKeyTruncating)
        .Default(true);

    registrar.Parameter("keep_nulls_in_right_or_full_join", &TThis::KeepNullsInRightOrFullJoin)
        .Default(true);

    registrar.Parameter("distributed_insert_stage", &TThis::DistributedInsertStage)
        .Default(EDistributedInsertStage::WithMergeableState);

    registrar.Parameter("table_read_lock_mode", &TThis::TableReadLockMode)
        .Default(ETableReadLockMode::BestEffort);

    registrar.Parameter("enable_min_max_filtering", &TThis::EnableMinMaxFiltering)
        .Default(true);

    registrar.Parameter("enable_optimize_read_in_order", &TThis::EnableOptimizeReadInOrder)
        .Default(false);
    registrar.Parameter("assume_no_null_keys", &TThis::AssumeNoNullKeys)
        .Default(false);
    registrar.Parameter("assume_no_nan_keys", &TThis::AssumeNoNanKeys)
        .Default(false);
}
////////////////////////////////////////////////////////////////////////////////

void TCachingSettings::Register(TRegistrar registrar)
{
    registrar.Parameter("table_attributes_invalidate_mode", &TThis::TableAttributesInvalidateMode)
        .Default(EInvalidateCacheMode::Sync);
    registrar.Parameter("invalidate_request_timeout", &TThis::InvalidateRequestTimeout)
        .Default(TDuration::Seconds(5));
}

////////////////////////////////////////////////////////////////////////////////

void TConcatTablesSettings::Register(TRegistrar registrar)
{
    registrar.Parameter("missing_column_mode", &TThis::MissingColumnMode)
        .Default(EMissingColumnMode::ReadAsNull);
    registrar.Parameter("type_mismatch_mode", &TThis::TypeMismatchMode)
        .Default(ETypeMismatchMode::Throw);
    registrar.Parameter("allow_empty_schema_intersection", &TThis::AllowEmptySchemaIntersection)
        .Default(false);
    registrar.Parameter("max_tables", &TThis::MaxTables)
        .LessThanOrEqual(2500)
        .Default(250);
}

////////////////////////////////////////////////////////////////////////////////

void TListDirSettings::Register(TRegistrar registrar)
{
    registrar.Parameter("max_size", &TThis::MaxSize)
        .Default(0);
}

////////////////////////////////////////////////////////////////////////////////

void TPrewhereSettings::Register(TRegistrar registrar)
{
    registrar.Parameter("prefilter_data_slices", &TThis::PrefilterDataSlices)
        .Default(false);
    registrar.Parameter("use_heuristic_column_sizes", &TThis::UseHeuristicColumnSizes)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TQuerySettings::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_columnar_read", &TThis::EnableColumnarRead)
        .Default(true);

    registrar.Parameter("enable_computed_column_deduction", &TThis::EnableComputedColumnDeduction)
        .Default(true);

    registrar.Parameter("deduced_statement_mode", &TThis::DeducedStatementMode)
        .Default(EDeducedStatementMode::In);

    registrar.Parameter("use_block_sampling", &TThis::UseBlockSampling)
        .Default(false);

    registrar.Parameter("log_key_condition_details", &TThis::LogKeyConditionDetails)
        .Default(false);

    registrar.Parameter("convert_row_batches_in_worker_thread_pool", &TThis::ConvertRowBatchesInWorkerThreadPool)
        .Default(true);

    registrar.Parameter("infer_dynamic_table_ranges_from_pivot_keys", &TThis::InferDynamicTableRangesFromPivotKeys)
        .Default(true);

    registrar.Parameter("composite", &TThis::Composite)
        .DefaultNew();

    registrar.Parameter("dynamic_table", &TThis::DynamicTable)
        .DefaultNew();

    registrar.Parameter("testing", &TThis::Testing)
        .DefaultNew();

    registrar.Parameter("execution", &TThis::Execution)
        .DefaultNew();

    registrar.Parameter("concat_tables", &TThis::ConcatTables)
        .DefaultNew();

    registrar.Parameter("list_dir", &TThis::ListDir)
        .DefaultNew();

    registrar.Parameter("table_reader", &TThis::TableReader)
        .DefaultNew();
    registrar.Parameter("table_writer", &TThis::TableWriter)
        .DefaultNew();

    registrar.Parameter("enable_reader_tracing", &TThis::EnableReaderTracing)
        .Default(false);

    registrar.Parameter("caching", &TThis::Caching)
        .DefaultNew();

    registrar.Parameter("cypress_read_options", &TThis::CypressReadOptions)
        .DefaultNew();
    registrar.Parameter("fetch_chunks_read_options", &TThis::FetchChunksReadOptions)
        .DefaultNew();

    registrar.Parameter("prewhere", &TThis::Prewhere)
        .DefaultNew();

    registrar.Preprocessor([] (TThis* config) {
        config->TableReader->GroupSize = 20_MB;
        config->TableReader->WindowSize = 70_MB;
        config->TableReader->MaxBufferSize = 200_MB;
        config->TableReader->BlockRpcHedgingDelay = TDuration::MilliSeconds(50);
        config->TableReader->MetaRpcHedgingDelay = TDuration::MilliSeconds(10);
        config->TableReader->CancelPrimaryBlockRpcRequestOnHedging = true;
    });
}

////////////////////////////////////////////////////////////////////////////////

void THealthCheckerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("period", &TThis::Period)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("timeout", &TThis::Timeout)
        .Default();
    registrar.Parameter("queries", &TThis::Queries)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        if (config->Timeout == TDuration::Zero()) {
            config->Timeout = config->Period / std::max<double>(1.0, config->Queries.size()) * 0.95;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TShowTablesConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("roots", &TThis::Roots)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        const int MaxRootCount = 10;
        if (config->Roots.size() > MaxRootCount) {
            THROW_ERROR_EXCEPTION("Maximum number of roots for SHOW TABLES exceeded: %v > %v", config->Roots.size(), MaxRootCount);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TSubqueryConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("columnar_statistics_fetcher", &TThis::ColumnarStatisticsFetcher)
        .DefaultNew();
    registrar.Parameter("chunk_slice_fetcher", &TThis::ChunkSliceFetcher)
        .DefaultNew();
    registrar.Parameter("max_job_count_for_pool", &TThis::MaxJobCountForPool)
        .Default(1'000'000);
    registrar.Parameter("min_data_weight_per_thread", &TThis::MinDataWeightPerThread)
        .Default(64_MB);
    registrar.Parameter("max_chunks_per_fetch", &TThis::MaxChunksPerFetch)
        .Default(100'000);
    registrar.Parameter("max_chunks_per_locate_request", &TThis::MaxChunksPerLocateRequest)
        .Default(10'000);
    registrar.Parameter("max_data_weight_per_subquery", &TThis::MaxDataWeightPerSubquery)
        .Default(50_GB);
    registrar.Parameter("use_columnar_statistics", &TThis::UseColumnarStatistics)
        .Default(true);
    registrar.Parameter("min_slice_data_weight", &TThis::MinSliceDataWeight)
        .Default(1_MB);
}

////////////////////////////////////////////////////////////////////////////////

void TMemoryWatchdogConfig::Register(TRegistrar registrar)
{
    // Default is effective infinity.
    registrar.Parameter("memory_limit", &TThis::MemoryLimit)
        .Default(1_TB);
    registrar.Parameter("codicil_watermark", &TThis::CodicilWatermark)
        .Default(0);
    registrar.Parameter("period", &TThis::Period)
        .Default(TDuration::MilliSeconds(300));
    registrar.Parameter("window_codicil_watermark", &TThis::WindowCodicilWatermark)
        .Default(0);
    registrar.Parameter("window_width", &TThis::WindowWidth)
        .Default(TDuration::Minutes(15));
}

////////////////////////////////////////////////////////////////////////////////

void TSecurityManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);
    registrar.Parameter("operation_acl_update_period", &TThis::OperationAclUpdatePeriod)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

void TGossipConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("period", &TThis::Period)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("timeout", &TThis::Timeout)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("unknown_instance_age_threshold", &TThis::UnknownInstanceAgeThreshold)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("unknown_instance_ping_limit", &TThis::UnknownInstancePingLimit)
        .Default(10);
    registrar.Parameter("ping_banned", &TThis::PingBanned)
        .Default(true);
    registrar.Parameter("allow_unban", &TThis::AllowUnban)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TInvokerLivenessCheckerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(true);
    registrar.Parameter("core_dump", &TThis::CoreDump)
        .Default(false);
    registrar.Parameter("period", &TThis::Period)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("timeout", &TThis::Timeout)
        .Default(TDuration::Seconds(5));
}

////////////////////////////////////////////////////////////////////////////////

void TQueryRegistryConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("process_list_snapshot_update_period", &TThis::ProcessListSnapshotUpdatePeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("save_running_queries", &TThis::SaveRunningQueries)
        .Default(true);
    registrar.Parameter("save_users", &TThis::SaveUsers)
        .Default(true);

    registrar.Parameter("clear_query_finish_infos_period", &TThis::ClearQueryFinishInfosPeriod)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

void TQuerySamplingConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("query_sampling_rate", &TThis::QuerySamplingRate)
        .InRange(0, 1)
        .Default(1);
    registrar.Parameter("user_agent_regexp", &TThis::UserAgentRegExp)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TClickHouseTableConfigPtr TClickHouseTableConfig::Create(TString database, TString name, TString engine)
{
    auto config = New<TClickHouseTableConfig>();
    config->Database = std::move(database);
    config->Name = std::move(name);
    config->Engine = std::move(engine);
    return config;
}

void TClickHouseTableConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("database", &TThis::Database)
        .Default();
    registrar.Parameter("name", &TThis::Name)
        .Default();
    registrar.Parameter("engine", &TThis::Engine)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TUserDefinedSqlObjectsStorageConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(false);
    registrar.Parameter("path", &TThis::Path)
        .Default();
    registrar.Parameter("update_period", &TThis::UpdatePeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("expire_after_successful_sync_time", &TThis::ExpireAfterSuccessfulSyncTime)
        .Default(TDuration::Seconds(60));

    registrar.Postprocessor([] (TThis* config) {
        if (config->Enabled && config->Path.empty()) {
            THROW_ERROR_EXCEPTION("No path is set for SQL UDF storage");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TSystemLogTableExporterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_in_progress_data_size", &TThis::MaxInProgressDataSize)
        .Default(10_MB);

    registrar.Parameter("max_bytes_to_keep", &TThis::MaxBytesToKeep)
        .Default(10_MB);
    registrar.Parameter("max_rows_to_keep", &TThis::MaxRowsToKeep)
        .Default(100'000);

    registrar.Parameter("create_table_attributes", &TThis::CreateTableAttributes)
        .DefaultCtor([] {
            return NYTree::BuildYsonNodeFluently()
                .BeginMap()
                    // NB: Set large value to prevent too many flushes of small chunks.
                    .Item("dynamic_store_auto_flush_period").Value(TDuration::Days(15))
                    .Item("enable_dynamic_store_read").Value(true)
                    .Item("optimize_for").Value(NTableClient::EOptimizeFor::Scan)
                .EndMap()->AsMap();
        });

    registrar.Preprocessor([] (TThis* config) {
        config->Enabled = false;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TSystemLogTableExportersConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cypress_root_directory", &TThis::CypressRootDirectory)
        .Default();

    registrar.Parameter("tables", &TThis::Tables)
        .Default();
    registrar.Parameter("default", &TThis::Default)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TYtConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("clique_id", &TThis::CliqueId)
        .Default();
    registrar.Parameter("instance_id", &TThis::InstanceId)
        .Default();
    registrar.Parameter("clique_alias", &TThis::CliqueAlias)
        .Default();
    registrar.Parameter("clique_incarnation", &TThis::CliqueIncarnation)
        .Default(-1);
    registrar.Parameter("address", &TThis::Address)
        .Default();
    registrar.Parameter("clique_instance_count", &TThis::CliqueInstanceCount)
        .Default(1);

    registrar.Parameter("client_cache", &TThis::ClientCache)
        .DefaultNew();

    registrar.Parameter("user_agent_blacklist", &TThis::UserAgentBlacklist)
        .Default();
    registrar.Parameter("enable_http_header_log", &TThis::EnableHttpHeaderLog)
        .Default(false);
    registrar.Parameter("http_header_blacklist", &TThis::HttpHeaderBlacklist)
        .Default();
    registrar.Parameter("user_name_blacklist", &TThis::UserNameBlacklist)
        .Default();

    registrar.Parameter("user_name_whitelist", &TThis::UserNameWhitelist)
        .Default();

    registrar.Parameter("validate_operation_access", &TThis::ValidateOperationAccess)
        .Default();
    registrar.Parameter("operation_acl_update_period", &TThis::OperationAclUpdatePeriod)
        .Default();

    registrar.Parameter("security_manager", &TThis::SecurityManager)
        .DefaultNew();

    registrar.Parameter("user", &TThis::User)
        .Default("yt-clickhouse");

    registrar.Parameter("show_tables", &TThis::ShowTables)
        .DefaultNew();

    registrar.Parameter("memory_watchdog", &TThis::MemoryWatchdog)
        .DefaultNew();

    registrar.Parameter("query_sticky_group_size", &TThis::QueryStickyGroupSize)
        .Default();

    registrar.Parameter("discovery", &TThis::Discovery)
        .DefaultNew();

    registrar.Parameter("gossip", &TThis::Gossip)
        .DefaultNew();

    registrar.Parameter("control_invoker_checker", &TThis::ControlInvokerChecker)
        .DefaultNew();

    registrar.Parameter("permission_cache", &TThis::PermissionCache)
        .DefaultNew();

    registrar.Parameter("worker_thread_count", &TThis::WorkerThreadCount)
        .Default(8);

    registrar.Parameter("fetcher_thread_count", &TThis::FetcherThreadCount)
        .Default(8);

    registrar.Parameter("cpu_limit", &TThis::CpuLimit)
        .Default();

    registrar.Parameter("subquery", &TThis::Subquery)
        .DefaultNew();

    registrar.Parameter("create_table_default_attributes", &TThis::CreateTableDefaultAttributes)
        .Default(NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("optimize_for").Value("scan")
        .EndMap());

    registrar.Parameter("total_reader_memory_limit", &TThis::TotalReaderMemoryLimit)
        .Default(20_GB);
    registrar.Parameter("reader_memory_requirement", &TThis::ReaderMemoryRequirement)
        .Default(500_MB);

    registrar.Parameter("health_checker", &TThis::HealthChecker)
        .DefaultNew();

    registrar.Parameter("database_directories", &TThis::DatabaseDirectories)
        .Default();

    registrar.Parameter("total_memory_tracker_update_period", &TThis::TotalMemoryTrackerUpdatePeriod)
        .Default(TDuration::MilliSeconds(300));

    registrar.Parameter("query_settings", &TThis::QuerySettings)
        .Alias("settings")
        .DefaultNew();

    registrar.Parameter("table_attribute_cache", &TThis::TableAttributeCache)
        .DefaultNew();

    registrar.Parameter("table_columnar_statistics_cache", &TThis::TableColumnarStatisticsCache)
        .DefaultNew();

    registrar.Parameter("query_registry", &TThis::QueryRegistry)
        .DefaultNew();

    registrar.Parameter("query_sampling", &TThis::QuerySampling)
        .DefaultNew();

    registrar.Parameter("user_defined_sql_objects_storage", &TThis::UserDefinedSqlObjectsStorage)
        .DefaultNew();

    registrar.Parameter("system_log_table_exporters", &TThis::SystemLogTableExporters)
        .DefaultNew();

    registrar.Preprocessor([] (TThis* config) {
        config->TableAttributeCache->ExpireAfterAccessTime = TDuration::Minutes(2);
        config->TableAttributeCache->ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(20);
        config->TableAttributeCache->ExpireAfterFailedUpdateTime = TDuration::Zero();
        config->TableAttributeCache->RefreshTime = TDuration::Seconds(15);
        config->TableAttributeCache->BatchUpdate = true;

        config->PermissionCache->RefreshUser = CacheUserName;
        config->PermissionCache->AlwaysUseRefreshUser = false;

        config->TableAttributeCache->UserName = CacheUserName;

        // Disable background updates since we deal with consistency issues by checking cached table revision.
        config->TableColumnarStatisticsCache->RefreshTime = std::nullopt;
        config->TableColumnarStatisticsCache->ExpireAfterSuccessfulUpdateTime = TDuration::Hours(6);
        config->TableColumnarStatisticsCache->ExpireAfterAccessTime = TDuration::Hours(6);

        config->Discovery->Directory = "//sys/clickhouse/cliques";
    });

    registrar.Postprocessor([] (TThis* config) {
        if (config->ValidateOperationAccess) {
            config->SecurityManager->Enable = *config->ValidateOperationAccess;
        }
        if (config->OperationAclUpdatePeriod) {
            config->SecurityManager->OperationAclUpdatePeriod = *config->OperationAclUpdatePeriod;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TLauncherConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("version", &TThis::Version)
        .Default(0);
}

////////////////////////////////////////////////////////////////////////////////

void TMemoryConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("reader", &TThis::Reader)
        .Default();
    registrar.Parameter("uncompressed_block_cache", &TThis::UncompressedBlockCache)
        .Default();
    registrar.Parameter("compressed_block_cache", &TThis::CompressedBlockCache)
        .Default();
    registrar.Parameter("chunk_meta_cache", &TThis::ChunkMetaCache)
        .Default();
    registrar.Parameter("memory_limit", &TThis::MemoryLimit)
        .Default();
    registrar.Parameter("max_server_memory_usage", &TThis::MaxServerMemoryUsage)
        .Default();
    registrar.Parameter("watchdog_oom_watermark", &TThis::WatchdogOomWatermark)
        .Default();
    registrar.Parameter("watchdog_oom_window_watermark", &TThis::WatchdogOomWindowWatermark)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TClickHouseServerBootstrapConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("core_dumper", &TThis::CoreDumper)
        .Default();

    registrar.Parameter("solomon_exporter", &TThis::SolomonExporter)
        .DefaultNew();

    registrar.Parameter("yt", &TThis::Yt)
        .DefaultNew();

    registrar.Parameter("graceful_interruption_delay", &TThis::GracefulInterruptionDelay)
        .Default(TDuration::Seconds(2))
        // COMPAT(dakovalkov)
        .Alias("interruption_graceful_timeout");

    registrar.Parameter("interruption_timeout", &TThis::InterruptionTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("launcher", &TThis::Launcher)
        .DefaultNew();

    registrar.Parameter("clickhouse", &TThis::ClickHouse)
        .Alias("engine")
        .DefaultNew();

    registrar.Parameter("cpu_limit", &TThis::CpuLimit)
        .Default();

    registrar.Parameter("memory", &TThis::Memory)
        .Default();

    registrar.Parameter("rpc_query_service_thread_count", &TThis::RpcQueryServiceThreadCount)
        .Default(100);

    registrar.Preprocessor([] (TThis* config) {
        auto jaegerConfig = config->GetSingletonConfig<NTracing::TJaegerTracerConfig>();
        jaegerConfig->ServiceName = "clickhouse_server";
        jaegerConfig->CollectorChannelConfig = New<NRpc::NGrpc::TChannelConfig>();
        jaegerConfig->CollectorChannelConfig->Address = "yt.c.jaeger.yandex-team.ru:14250";
    });

    registrar.Postprocessor([] (TThis* config) {
        if (config->CpuLimit) {
            config->Yt->CpuLimit = config->CpuLimit;
        }

        if (config->Memory) {
            if (config->Memory->Reader) {
                config->Yt->TotalReaderMemoryLimit = *config->Memory->Reader;
            }
            if (config->Memory->MemoryLimit) {
                config->Yt->MemoryWatchdog->MemoryLimit = *config->Memory->MemoryLimit;
            }
            if (config->Memory->WatchdogOomWatermark) {
                config->Yt->MemoryWatchdog->CodicilWatermark = *config->Memory->WatchdogOomWatermark;
            }
            if (config->Memory->WatchdogOomWindowWatermark) {
                config->Yt->MemoryWatchdog->WindowCodicilWatermark = *config->Memory->WatchdogOomWindowWatermark;
            }

            auto initDefault = [] (auto& config) {
                if (!config) {
                    config = New<typename std::remove_reference_t<decltype(config)>::TUnderlying>();
                }
            };

            if (config->Memory->UncompressedBlockCache) {
                initDefault(config->ClusterConnection->Dynamic->BlockCache);
                config->ClusterConnection->Dynamic->BlockCache->UncompressedData->Capacity = *config->Memory->UncompressedBlockCache;
            }
            if (config->Memory->CompressedBlockCache) {
                initDefault(config->ClusterConnection->Dynamic->BlockCache);
                config->ClusterConnection->Dynamic->BlockCache->CompressedData->Capacity = *config->Memory->CompressedBlockCache;
            }
            if (config->Memory->ChunkMetaCache) {
                initDefault(config->ClusterConnection->Dynamic->ChunkMetaCache);
                config->ClusterConnection->Dynamic->ChunkMetaCache->Capacity = *config->Memory->ChunkMetaCache;
            }

            if (config->Memory->MaxServerMemoryUsage) {
                config->ClickHouse->MaxServerMemoryUsage = *config->Memory->MaxServerMemoryUsage;
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
