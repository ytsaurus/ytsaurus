#include "config.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/election/config.h>

#include <yt/yt/server/lib/hive/config.h>

#include <yt/yt/server/lib/transaction_supervisor/config.h>

#include <yt/yt/ytlib/chaos_client/config.h>

#include <yt/yt/library/dynamic_config/config.h>

#include <yt/yt/library/query/engine_api/config.h>

namespace NYT::NTabletNode {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TTabletHydraManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("response_keeper", &TThis::ResponseKeeper)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TTransactionManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_transaction_timeout", &TThis::MaxTransactionTimeout)
        .GreaterThan(TDuration())
        .Default(TDuration::Seconds(60));
    registrar.Parameter("barrier_check_period", &TThis::BarrierCheckPeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("max_aborted_transaction_pool_size", &TThis::MaxAbortedTransactionPoolSize)
        .Default(1'000);
    registrar.Parameter("reject_incorrect_clock_cluster_tag", &TThis::RejectIncorrectClockClusterTag)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TTabletManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("pool_chunk_size", &TThis::PoolChunkSize)
        .GreaterThan(64_KB)
        .Default(1_MB);

    registrar.Parameter("max_blocked_row_wait_time", &TThis::MaxBlockedRowWaitTime)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("preload_backoff_time", &TThis::PreloadBackoffTime)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("compaction_backoff_time", &TThis::CompactionBackoffTime)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("partition_split_merge_backoff_time", &TThis::PartitionSplitMergeBackoffTime)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("flush_backoff_time", &TThis::FlushBackoffTime)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("changelog_codec", &TThis::ChangelogCodec)
        .Default(NCompression::ECodec::Lz4);

    registrar.Parameter("client_timestamp_threshold", &TThis::ClientTimestampThreshold)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("replicator_thread_pool_size", &TThis::ReplicatorThreadPoolSize)
        .GreaterThan(0)
        .Default(1);
    registrar.Parameter("replicator_soft_backoff_time", &TThis::ReplicatorSoftBackoffTime)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("replicator_hard_backoff_time", &TThis::ReplicatorHardBackoffTime)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("tablet_cell_decommission_check_period", &TThis::TabletCellDecommissionCheckPeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("tablet_cell_suspension_check_period", &TThis::TabletCellSuspensionCheckPeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("sleep_before_post_to_master", &TThis::SleepBeforePostToMaster)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TTabletManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("replicator_thread_pool_size", &TThis::ReplicatorThreadPoolSize)
        .GreaterThan(0)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TTabletCellWriteManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("write_failure_probability", &TThis::WriteFailureProbability)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TTabletHunkLockManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("hunk_store_extra_lifetime", &TThis::HunkStoreExtraLifeTime)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("unlock_check_period", &TThis::UnlockCheckPeriod)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

void TStoreBackgroundActivityOrchidConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_failed_task_count", &TThis::MaxFailedTaskCount)
        .GreaterThanOrEqual(0)
        .Default(100);
    registrar.Parameter("max_completed_task_count", &TThis::MaxCompletedTaskCount)
        .GreaterThanOrEqual(0)
        .Default(100);
}

////////////////////////////////////////////////////////////////////////////////

void TStoreFlusherConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_pool_size", &TThis::ThreadPoolSize)
        .GreaterThan(0)
        .Default(1);
    registrar.Parameter("max_concurrent_flushes", &TThis::MaxConcurrentFlushes)
        .GreaterThan(0)
        .Default(16);
    registrar.Parameter("min_forced_flush_data_size", &TThis::MinForcedFlushDataSize)
        .GreaterThan(0)
        .Default(1_MB);
}

////////////////////////////////////////////////////////////////////////////////

void TStoreFlusherDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);
    registrar.Parameter("forced_rotation_memory_ratio", &TThis::ForcedRotationMemoryRatio)
        .InRange(0.0, 1.0)
        .Optional();
    registrar.Parameter("thread_pool_size", &TThis::ThreadPoolSize)
        .GreaterThan(0)
        .Optional();
    registrar.Parameter("max_concurrent_flushes", &TThis::MaxConcurrentFlushes)
        .GreaterThan(0)
        .Optional();
    registrar.Parameter("min_forced_flush_data_size", &TThis::MinForcedFlushDataSize)
        .GreaterThan(0)
        .Optional();
    registrar.Parameter("orchid", &TThis::Orchid)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TStoreCompactorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_pool_size", &TThis::ThreadPoolSize)
        .GreaterThan(0)
        .Default(1);
    registrar.Parameter("max_concurrent_compactions", &TThis::MaxConcurrentCompactions)
        .GreaterThan(0)
        .Default(1);
    registrar.Parameter("max_concurrent_partitionings", &TThis::MaxConcurrentPartitionings)
        .GreaterThan(0)
        .Default(1);
}

////////////////////////////////////////////////////////////////////////////////

void TStoreCompactorDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);
    registrar.Parameter("thread_pool_size", &TThis::ThreadPoolSize)
        .GreaterThan(0)
        .Optional();
    registrar.Parameter("max_concurrent_compactions", &TThis::MaxConcurrentCompactions)
        .GreaterThan(0)
        .Optional();
    registrar.Parameter("max_concurrent_partitionings", &TThis::MaxConcurrentPartitionings)
        .GreaterThan(0)
        .Optional();

    registrar.Parameter("chunk_view_size_fetch_period", &TThis::ChunkViewSizeFetchPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("chunk_view_size_request_throttler", &TThis::ChunkViewSizeRequestThrottler)
        .DefaultNew();

    registrar.Parameter("row_digest_fetch_period", &TThis::RowDigestFetchPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("row_digest_request_throttler", &TThis::RowDigestRequestThrottler)
        .DefaultNew();
    registrar.Parameter("use_row_digests", &TThis::UseRowDigests)
        .Default(false);

    registrar.Parameter("max_compaction_structured_log_events", &TThis::MaxCompactionStructuredLogEvents)
        .GreaterThanOrEqual(0)
        .Default(500);
    registrar.Parameter("max_partitioning_structured_log_events", &TThis::MaxPartitioningStructuredLogEvents)
        .GreaterThanOrEqual(0)
        .Default(500);

    registrar.Parameter("orchid", &TThis::Orchid)
        .DefaultNew();

    registrar.Parameter("use_query_pool", &TThis::UseQueryPool)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TStoreTrimmerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void THunkChunkSweeperDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

TInMemoryManagerConfigPtr TInMemoryManagerConfig::ApplyDynamic(
    const TInMemoryManagerDynamicConfigPtr& dynamicConfig) const
{
    auto config = CloneYsonStruct(MakeStrong(this));
    UpdateYsonStructField(config->MaxConcurrentPreloads, dynamicConfig->MaxConcurrentPreloads);
    UpdateYsonStructField(config->InterceptedDataRetentionTime, dynamicConfig->InterceptedDataRetentionTime);
    UpdateYsonStructField(config->PingPeriod, dynamicConfig->PingPeriod);
    UpdateYsonStructField(config->ControlRpcTimeout, dynamicConfig->ControlRpcTimeout);
    UpdateYsonStructField(config->HeavyRpcTimeout, dynamicConfig->HeavyRpcTimeout);
    UpdateYsonStructField(config->RemoteSendBatchSize, dynamicConfig->RemoteSendBatchSize);
    UpdateYsonStructField(
        config->EnablePreliminaryNetworkThrottling,
        dynamicConfig->EnablePreliminaryNetworkThrottling);
    config->Postprocess();
    return config;
}

void TInMemoryManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_concurrent_preloads", &TThis::MaxConcurrentPreloads)
        .GreaterThan(0)
        .Default(1);
    registrar.Parameter("intercepted_data_retention_time", &TThis::InterceptedDataRetentionTime)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("ping_period", &TThis::PingPeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("control_rpc_timeout", &TThis::ControlRpcTimeout)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("heavy_rpc_timeout", &TThis::HeavyRpcTimeout)
        .Default(TDuration::Seconds(20));
    registrar.Parameter("remote_send_batch_size", &TThis::RemoteSendBatchSize)
        .Default(16_MB);
    registrar.Parameter("workload_descriptor", &TThis::WorkloadDescriptor)
        .Default(TWorkloadDescriptor(EWorkloadCategory::UserBatch));
    registrar.Parameter("preload_throttler", &TThis::PreloadThrottler)
        .Optional();
    registrar.Parameter("enable_preliminary_network_throttling", &TThis::EnablePreliminaryNetworkThrottling)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TInMemoryManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_concurrent_preloads", &TThis::MaxConcurrentPreloads)
        .GreaterThan(0)
        .Optional();
    registrar.Parameter("intercepted_data_retention_time", &TThis::InterceptedDataRetentionTime)
        .Optional();
    registrar.Parameter("ping_period", &TThis::PingPeriod)
        .Optional();
    registrar.Parameter("control_rpc_timeout", &TThis::ControlRpcTimeout)
        .Optional();
    registrar.Parameter("heavy_rpc_timeout", &TThis::HeavyRpcTimeout)
        .Optional();
    registrar.Parameter("remote_send_batch_size", &TThis::RemoteSendBatchSize)
        .Optional();
    registrar.Parameter("enable_preliminary_network_throttling", &TThis::EnablePreliminaryNetworkThrottling)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionBalancerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("chunk_location_throttler", &TThis::ChunkLocationThrottler)
        .DefaultNew();
    registrar.Parameter("chunk_scraper", &TThis::ChunkScraper)
        .DefaultNew();
    registrar.Parameter("samples_fetcher", &TThis::SamplesFetcher)
        .DefaultNew();
    registrar.Parameter("min_partitioning_sample_count", &TThis::MinPartitioningSampleCount)
        .Default(10)
        .GreaterThanOrEqual(3);
    registrar.Parameter("max_partitioning_sample_count", &TThis::MaxPartitioningSampleCount)
        .Default(1'000)
        .GreaterThanOrEqual(10);
    registrar.Parameter("max_concurrent_samplings", &TThis::MaxConcurrentSamplings)
        .GreaterThan(0)
        .Default(8);
    registrar.Parameter("resampling_period", &TThis::ResamplingPeriod)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("split_retry_delay", &TThis::SplitRetryDelay)
        .Default(TDuration::Seconds(30));
}

TPartitionBalancerConfigPtr TPartitionBalancerConfig::ApplyDynamic(
    const TPartitionBalancerDynamicConfigPtr& dynamicConfig) const
{
    auto config = CloneYsonStruct(MakeStrong(this));
    UpdateYsonStructField(config->MinPartitioningSampleCount, dynamicConfig->MinPartitioningSampleCount);
    UpdateYsonStructField(config->MaxPartitioningSampleCount, dynamicConfig->MaxPartitioningSampleCount);
    UpdateYsonStructField(config->SplitRetryDelay, dynamicConfig->SplitRetryDelay);
    config->Postprocess();
    return config;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionBalancerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);
    registrar.Parameter("min_partitioning_sample_count", &TThis::MinPartitioningSampleCount)
        .GreaterThanOrEqual(3)
        .Optional();
    registrar.Parameter("max_partitioning_sample_count", &TThis::MaxPartitioningSampleCount)
        .GreaterThanOrEqual(10)
        .Optional();
    registrar.Parameter("max_concurrent_samplings", &TThis::MaxConcurrentSamplings)
        .GreaterThan(0)
        .Optional();
    registrar.Parameter("split_retry_delay", &TThis::SplitRetryDelay)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TMasterConnectorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("heartbeat_period", &TThis::HeartbeatPeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("heartbeat_period_splay", &TThis::HeartbeatPeriodSplay)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("heartbeat_timeout", &TThis::HeartbeatTimeout)
        .Default(TDuration::Seconds(60));

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

void TMasterConnectorDynamicConfig::Register(TRegistrar registrar)
{
    // TODO(cherepashka): make this yson struct.
    registrar.Parameter("heartbeat_executor", &TThis::HeartbeatExecutor)
        .Default();
    registrar.Parameter("heartbeat_timeout", &TThis::HeartbeatTimeout)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

void TResourceLimitsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("slots", &TThis::Slots)
        .GreaterThanOrEqual(0)
        .Default(4);
    registrar.Parameter("tablet_static_memory", &TThis::TabletStaticMemory)
        .GreaterThanOrEqual(0)
        .Default(std::numeric_limits<i64>::max());
    registrar.Parameter("tablet_dynamic_memory", &TThis::TabletDynamicMemory)
        .GreaterThanOrEqual(0)
        .Default(std::numeric_limits<i64>::max());
}

////////////////////////////////////////////////////////////////////////////////

void TBackupManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("checkpoint_feasibility_check_batch_period", &TThis::CheckpointFeasibilityCheckBatchPeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("checkpoint_feasibility_check_backoff", &TThis::CheckpointFeasibilityCheckBackoff)
        .Default(TDuration::Seconds(1));
}

////////////////////////////////////////////////////////////////////////////////

void TStatisticsReporterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);
    registrar.Parameter("max_tablets_per_transaction", &TThis::MaxTabletsPerTransaction)
        .Default(10'000)
        .GreaterThan(0);
    registrar.Parameter("report_backoff_time", &TThis::ReportBackoffTime)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("table_path", &TThis::TablePath)
        .Default("//sys/tablet_balancer/performance_counters");

    registrar.Parameter("periodic_options",&TThis::PeriodicOptions)
        .Default({
            .Period = TDuration::Seconds(10),
            .Splay = TDuration::Seconds(5),
            .Jitter = 0.2,
        });
}

////////////////////////////////////////////////////////////////////////////////

void TMediumThrottlersConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_changelog_throttling", &TThis::EnableChangelogThrottling)
        .Default(false);
    registrar.Parameter("enable_blob_throttling", &TThis::EnableBlobThrottling)
        .Default(false);
    registrar.Parameter("throttle_timeout_fraction", &TThis::ThrottleTimeoutFraction)
        .Default(0.5);
    registrar.Parameter("max_throttling_time", &TThis::MaxThrottlingTime)
        .Default(TDuration::Minutes(5));
}

////////////////////////////////////////////////////////////////////////////////

void TCompressionDictionaryBuilderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_pool_size", &TThis::ThreadPoolSize)
        .GreaterThan(0)
        .Default(1);
    registrar.Parameter("max_concurrent_build_tasks", &TThis::MaxConcurrentBuildTasks)
        .GreaterThan(0)
        .Default(2);
}

////////////////////////////////////////////////////////////////////////////////

void TCompressionDictionaryBuilderDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);
    registrar.Parameter("thread_pool_size", &TThis::ThreadPoolSize)
        .GreaterThan(0)
        .Optional();
    registrar.Parameter("max_concurrent_build_tasks", &TThis::MaxConcurrentBuildTasks)
        .GreaterThan(0)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TSmoothMovementTrackerTestingConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("delay_after_stage_at_source", &TThis::DelayAfterStageAtSource)
        .Default();
    registrar.Parameter("delay_after_stage_at_target", &TThis::DelayAfterStageAtTarget)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TSmoothMovementTrackerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("testing", &TThis::Testing)
        .DefaultNew();

    registrar.Parameter("preload_wait_timeout", &TThis::PreloadWaitTimeout)
        .Default(TDuration::Minutes(1));
}

////////////////////////////////////////////////////////////////////////////////

void TErrorManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("deduplication_cache_timeout", &TThis::DeduplicationCacheTimeout)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("error_expiration_timeout", &TThis::ErrorExpirationTimeout)
        .Default(TDuration::Minutes(30));
    registrar.Parameter("log_no_context_interval", &TThis::LogNoContextInterval)
        .Default(TDuration::Seconds(5));
}

////////////////////////////////////////////////////////////////////////////////

void TTabletNodeDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("slots", &TThis::Slots)
        .Optional();

    registrar.Parameter("tablet_manager", &TThis::TabletManager)
        .DefaultNew();

    registrar.Parameter("tablet_cell_write_manager", &TThis::TabletCellWriteManager)
        .DefaultNew();

    registrar.Parameter("hunk_lock_manager", &TThis::HunkLockManager)
        .DefaultNew();

    registrar.Parameter("throttlers", &TThis::Throttlers)
        .Optional();

    registrar.Parameter("store_compactor", &TThis::StoreCompactor)
        .DefaultNew();
    registrar.Parameter("store_flusher", &TThis::StoreFlusher)
        .DefaultNew();
    registrar.Parameter("store_trimmer", &TThis::StoreTrimmer)
        .DefaultNew();
    registrar.Parameter("hunk_chunk_sweeper", &TThis::HunkChunkSweeper)
        .DefaultNew();
    registrar.Parameter("partition_balancer", &TThis::PartitionBalancer)
        .DefaultNew();
    registrar.Parameter("in_memory_manager", &TThis::InMemoryManager)
        .DefaultNew();
    registrar.Parameter("compression_dictionary_builder", &TThis::CompressionDictionaryBuilder)
        .DefaultNew();

    registrar.Parameter("versioned_chunk_meta_cache", &TThis::VersionedChunkMetaCache)
        .DefaultNew();

    registrar.Parameter("column_evaluator_cache", &TThis::ColumnEvaluatorCache)
        .DefaultNew();

    registrar.Parameter("enable_structured_logger", &TThis::EnableStructuredLogger)
        .Default(true);
    registrar.Parameter("full_structured_tablet_heartbeat_period", &TThis::FullStructuredTabletHeartbeatPeriod)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("incremental_structured_tablet_heartbeat_period", &TThis::IncrementalStructuredTabletHeartbeatPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("master_connector", &TThis::MasterConnector)
        .DefaultNew();

    registrar.Parameter("security_manager", &TThis::SecurityManager)
        .DefaultNew();

    registrar.Parameter("backup_manager", &TThis::BackupManager)
        .DefaultNew();

    registrar.Parameter("smooth_movement_tracker", &TThis::SmoothMovementTracker)
        .DefaultNew();

    registrar.Parameter("overload_controller", &TThis::OverloadController)
        .DefaultNew();

    registrar.Parameter("statistics_reporter", &TThis::StatisticsReporter)
        .DefaultNew();

    registrar.Parameter("error_manager", &TThis::ErrorManager)
        .DefaultNew();

    registrar.Parameter("enable_chunk_fragment_reader_throttling", &TThis::EnableChunkFragmentReaderThrottling)
        .Default(false);

    registrar.Parameter("medium_throttlers", &TThis::MediumThrottlers)
        .DefaultNew();

    registrar.Parameter("compression_dictionary_cache", &TThis::CompressionDictionaryCache)
        .DefaultNew();

    registrar.Parameter("enable_changelog_network_usage_accounting", &TThis::EnableChangelogNetworkUsageAccounting)
        .Default(false);

    registrar.Parameter("enable_collocated_dat_node_throttling", &TThis::EnableCollocatedDatNodeThrottling)
        .Default(false);

    registrar.Parameter("enable_snapshot_network_throttling", &TThis::EnableSnapshotNetworkThrottling)
        .Default(false);

    registrar.Parameter("replication_card_updates_batcher", &TThis::ChaosReplicationCardUpdatesBatcher)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void THintManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("replicator_hint_config_fetcher", &TThis::ReplicatorHintConfigFetcher)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TTabletNodeConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("forced_rotation_memory_ratio", &TThis::ForcedRotationMemoryRatio)
        .InRange(0.0, 1.0)
        .Default(0.8)
        .Alias("forced_rotations_memory_ratio");

    registrar.Parameter("resource_limits", &TThis::ResourceLimits)
        .DefaultNew();

    registrar.Parameter("snapshots", &TThis::Snapshots)
        .DefaultCtor([] { return New<NHydra::TRemoteSnapshotStoreConfig>(); });
    registrar.Parameter("changelogs", &TThis::Changelogs)
        .DefaultNew();
    registrar.Parameter("hydra_manager", &TThis::HydraManager)
        .DefaultNew();
    registrar.Parameter("election_manager", &TThis::ElectionManager)
        .DefaultNew();
    registrar.Parameter("hive_manager", &TThis::HiveManager)
        .DefaultNew();
    registrar.Parameter("transaction_manager", &TThis::TransactionManager)
        .DefaultNew();
    registrar.Parameter("transaction_supervisor", &TThis::TransactionSupervisor)
        .DefaultNew();
    registrar.Parameter("tablet_manager", &TThis::TabletManager)
        .DefaultNew();
    registrar.Parameter("store_flusher", &TThis::StoreFlusher)
        .DefaultNew();
    registrar.Parameter("store_compactor", &TThis::StoreCompactor)
        .DefaultNew();
    registrar.Parameter("in_memory_manager", &TThis::InMemoryManager)
        .DefaultNew();
    registrar.Parameter("partition_balancer", &TThis::PartitionBalancer)
        .DefaultNew();
    registrar.Parameter("security_manager", &TThis::SecurityManager)
        .DefaultNew();
    registrar.Parameter("hint_manager", &TThis::HintManager)
        .DefaultNew();
    registrar.Parameter("table_config_manager", &TThis::TableConfigManager)
        .DefaultNew();
    registrar.Parameter("compression_dictionary_builder", &TThis::CompressionDictionaryBuilder)
        .DefaultNew();

    registrar.Parameter("versioned_chunk_meta_cache", &TThis::VersionedChunkMetaCache)
        .DefaultNew();

    registrar.Parameter("throttlers", &TThis::Throttlers)
        .Optional();

    registrar.Parameter("slot_scan_period", &TThis::SlotScanPeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("tablet_snapshot_eviction_timeout", &TThis::TabletSnapshotEvictionTimeout)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("alien_cluster_client_cache_eviction_period", &TThis::AlienClusterClientCacheEvictionPeriod)
        .Default(TDuration::Days(1));

    registrar.Parameter("column_evaluator_cache", &TThis::ColumnEvaluatorCache)
        .DefaultNew();

    registrar.Parameter("master_connector", &TThis::MasterConnector)
        .DefaultNew();

    registrar.Parameter("compression_dictionary_cache", &TThis::CompressionDictionaryCache)
        .DefaultNew();

    registrar.Parameter("replication_card_updates_batcher", &TThis::ChaosReplicationCardUpdatesBatcher)
        .DefaultNew();

    registrar.Parameter("allow_reign_change", &TThis::AllowReignChange)
        .Default(true);

    registrar.Preprocessor([] (TThis* config) {
        config->VersionedChunkMetaCache->Capacity = 10_GB;
    });

    registrar.Postprocessor([] (TThis* config) {
        // Instantiate default throttler configs.
        for (auto kind : TEnumTraits<ETabletNodeThrottlerKind>::GetDomainValues()) {
            if (config->Throttlers[kind]) {
                continue;
            }

            switch (kind) {
                case ETabletNodeThrottlerKind::StaticStorePreloadIn:
                case ETabletNodeThrottlerKind::DynamicStoreReadOut:
                    config->Throttlers[kind] = NConcurrency::TRelativeThroughputThrottlerConfig::Create(100_MB);
                    break;

                default:
                    config->Throttlers[kind] = New<NConcurrency::TRelativeThroughputThrottlerConfig>();
            }
        }

        if (config->InMemoryManager->PreloadThrottler) {
            config->Throttlers[ETabletNodeThrottlerKind::StaticStorePreloadIn] = config->InMemoryManager->PreloadThrottler;
        }

        // COMPAT(akozhikhov): set to false when masters are updated too.
        config->HintManager->ReplicatorHintConfigFetcher->IgnoreConfigAbsence = true;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
