#include "config.h"

#include <yt/yt/server/lib/hive/config.h>

#include <yt/yt/server/lib/dynamic_config/config.h>

#include <yt/yt/server/lib/election/config.h>

#include <yt/yt/ytlib/query_client/config.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/concurrency/config.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TTabletHydraManagerConfig::TTabletHydraManagerConfig()
{
    RegisterParameter("response_keeper", ResponseKeeper)
        .DefaultNew();

    RegisterPreprocessor([&] {
        PreallocateChangelogs = true;
    });
}

////////////////////////////////////////////////////////////////////////////////

TTableMountConfig::TTableMountConfig()
{
    RegisterParameter("tablet_cell_bundle", TabletCellBundle)
        .Optional();
    RegisterParameter("max_dynamic_store_row_count", MaxDynamicStoreRowCount)
        .GreaterThan(0)
        .Default(1000000);
    RegisterParameter("max_dynamic_store_value_count", MaxDynamicStoreValueCount)
        .GreaterThan(0)
        .Default(1000000000);
    RegisterParameter("max_dynamic_store_timestamp_count", MaxDynamicStoreTimestampCount)
        .GreaterThan(0)
        .Default(10000000)
        // NB: This limit is really important; please consult babenko@
        // before changing it.
        .LessThanOrEqual(SoftRevisionsPerDynamicStoreLimit);
    RegisterParameter("max_dynamic_store_pool_size", MaxDynamicStorePoolSize)
        .GreaterThan(0)
        .Default(1_GB);
    RegisterParameter("max_dynamic_store_row_data_weight", MaxDynamicStoreRowDataWeight)
        .GreaterThan(0)
        .Default(NTableClient::MaxClientVersionedRowDataWeight)
        // NB: This limit is important: it ensures that store is flushable.
        // Please consult savrus@ before changing.
        .LessThanOrEqual(NTableClient::MaxServerVersionedRowDataWeight / 2);

    RegisterParameter("dynamic_store_overflow_threshold", DynamicStoreOverflowThreshold)
        .GreaterThan(0.0)
        .Default(0.7)
        .LessThanOrEqual(1.0);

    RegisterParameter("max_partition_data_size", MaxPartitionDataSize)
        .Default(320_MB)
        .GreaterThan(0);
    RegisterParameter("desired_partition_data_size", DesiredPartitionDataSize)
        .Default(256_MB)
        .GreaterThan(0);
    RegisterParameter("min_partition_data_size", MinPartitionDataSize)
        .Default(96_MB)
        .GreaterThan(0);

    RegisterParameter("max_partition_count", MaxPartitionCount)
        .Default(10240)
        .GreaterThan(0);

    RegisterParameter("min_partitioning_data_size", MinPartitioningDataSize)
        .Default(64_MB)
        .GreaterThan(0);
    RegisterParameter("min_partitioning_store_count", MinPartitioningStoreCount)
        .Default(1)
        .GreaterThan(0);
    RegisterParameter("max_partitioning_data_size", MaxPartitioningDataSize)
        .Default(1_GB)
        .GreaterThan(0);
    RegisterParameter("max_partitioning_store_count", MaxPartitioningStoreCount)
        .Default(5)
        .GreaterThan(0);

    RegisterParameter("min_compaction_store_count", MinCompactionStoreCount)
        .Default(3)
        .GreaterThan(1);
    RegisterParameter("max_compaction_store_count", MaxCompactionStoreCount)
        .Default(5)
        .GreaterThan(0);
    RegisterParameter("compaction_data_size_base", CompactionDataSizeBase)
        .Default(16_MB)
        .GreaterThan(0);
    RegisterParameter("compaction_data_size_ratio", CompactionDataSizeRatio)
        .Default(2.0)
        .GreaterThan(1.0);

    RegisterParameter("flush_throttler", FlushThrottler)
        .DefaultNew();
    RegisterParameter("compaction_throttler", CompactionThrottler)
        .DefaultNew();
    RegisterParameter("partitioning_throttler", PartitioningThrottler)
        .DefaultNew();

    RegisterParameter("throttlers", Throttlers)
        .Default();

    RegisterParameter("samples_per_partition", SamplesPerPartition)
        .Default(100)
        .GreaterThanOrEqual(0);

    RegisterParameter("backing_store_retention_time", BackingStoreRetentionTime)
        .Default(TDuration::Seconds(60));

    RegisterParameter("max_read_fan_in", MaxReadFanIn)
        .GreaterThan(0)
        .Default(30);

    RegisterParameter("max_overlapping_store_count", MaxOverlappingStoreCount)
        .GreaterThan(0)
        .Default(DefaultMaxOverlappingStoreCount);
    RegisterParameter("critical_overlapping_store_count", CriticalOverlappingStoreCount)
        .GreaterThan(0)
        .Optional();
    RegisterParameter("overlapping_store_immediate_split_threshold", OverlappingStoreImmediateSplitThreshold)
        .GreaterThan(0)
        .Default(20);

    RegisterParameter("in_memory_mode", InMemoryMode)
        .Default(NTabletClient::EInMemoryMode::None);

    RegisterParameter("max_stores_per_tablet", MaxStoresPerTablet)
        .Default(10000)
        .GreaterThan(0);
    RegisterParameter("max_eden_stores_per_tablet", MaxEdenStoresPerTablet)
        .Default(100)
        .GreaterThan(0);

    RegisterParameter("forced_compaction_revision", ForcedCompactionRevision)
        .Default();
    RegisterParameter("forced_store_compaction_revision", ForcedStoreCompactionRevision)
        .Default();
    RegisterParameter("forced_hunk_compaction_revision", ForcedHunkCompactionRevision)
        .Default();
    RegisterParameter("forced_chunk_view_compaction_revision", ForcedCompactionRevision)
        .Default();

    RegisterParameter("dynamic_store_auto_flush_period", DynamicStoreAutoFlushPeriod)
        .Default(TDuration::Minutes(15));
    RegisterParameter("dynamic_store_flush_period_splay", DynamicStoreFlushPeriodSplay)
        .Default(TDuration::Minutes(1));
    RegisterParameter("auto_compaction_period", AutoCompactionPeriod)
        .Default();
    RegisterParameter("auto_compaction_period_splay_ratio", AutoCompactionPeriodSplayRatio)
        .Default(0.3);
    RegisterParameter("periodic_compaction_mode", PeriodicCompactionMode)
        .Default(EPeriodicCompactionMode::Store);

    RegisterParameter("enable_lookup_hash_table", EnableLookupHashTable)
        .Default(false);

    RegisterParameter("lookup_cache_rows_per_tablet", LookupCacheRowsPerTablet)
        .Default(0);
    RegisterParameter("lookup_cache_rows_ratio", LookupCacheRowsRatio)
        .Default(0);
    RegisterParameter("enable_lookup_cache_by_default", EnableLookupCacheByDefault)
        .Default(false);

    RegisterParameter("row_count_to_keep", RowCountToKeep)
        .Default(0);

    RegisterParameter("replication_tick_period", ReplicationTickPeriod)
        .Default(TDuration::MilliSeconds(100));
    RegisterParameter("min_replication_log_ttl", MinReplicationLogTtl)
        .Default(TDuration::Minutes(5));
    RegisterParameter("max_timestamps_per_replication_commit", MaxTimestampsPerReplicationCommit)
        .Default(10000);
    RegisterParameter("max_rows_per_replication_commit", MaxRowsPerReplicationCommit)
        .Default(90000);
    RegisterParameter("max_data_weight_per_replication_commit", MaxDataWeightPerReplicationCommit)
        .Default(128_MB);
    RegisterParameter("replication_throttler", ReplicationThrottler)
        .DefaultNew();
    RegisterParameter("enable_replication_logging", EnableReplicationLogging)
        .Default(false);

    RegisterParameter("enable_profiling", EnableProfiling)
        .Default(false);
    RegisterParameter("profiling_mode", ProfilingMode)
        .Default(EDynamicTableProfilingMode::Path);
    RegisterParameter("profiling_tag", ProfilingTag)
        .Optional();

    RegisterParameter("enable_structured_logger", EnableStructuredLogger)
        .Default(true);

    RegisterParameter("enable_compaction_and_partitioning", EnableCompactionAndPartitioning)
        .Default(true);

    RegisterParameter("enable_store_rotation", EnableStoreRotation)
        .Default(true);

    RegisterParameter("merge_rows_on_flush", MergeRowsOnFlush)
        .Default(false);

    RegisterParameter("merge_deletions_on_flush", MergeDeletionsOnFlush)
        .Default(false);

    RegisterParameter("enable_lsm_verbose_logging", EnableLsmVerboseLogging)
        .Default(false);

    RegisterParameter("max_unversioned_block_size", MaxUnversionedBlockSize)
        .GreaterThan(0)
        .Optional();

    RegisterParameter("preserve_tablet_index", PreserveTabletIndex)
        .Default(false);

    RegisterParameter("enable_partition_split_while_eden_partitioning", EnablePartitionSplitWhileEdenPartitioning)
        .Default(false);

    RegisterParameter("enable_discarding_expired_partitions", EnableDiscardingExpiredPartitions)
        .Default(true);

    RegisterParameter("enable_data_node_lookup", EnableDataNodeLookup)
        .Default(false);

    RegisterParameter("enable_peer_probing_in_data_node_lookup", EnablePeerProbingInDataNodeLookup)
        .Default(false);

    RegisterParameter("max_parallel_partition_lookups", MaxParallelPartitionLookups)
        .Optional()
        .GreaterThan(0)
        .LessThanOrEqual(MaxParallelPartitionLookupsLimit);

    RegisterParameter("enable_rejects_in_data_node_lookup_if_throttling", EnableRejectsInDataNodeLookupIfThrottling)
        .Default(false);

    RegisterParameter("enable_dynamic_store_read", EnableDynamicStoreRead)
        .Default(false);

    RegisterParameter("enable_new_scan_reader_for_lookup", EnableNewScanReaderForLookup)
        .Default(false);
    RegisterParameter("enable_new_scan_reader_for_select", EnableNewScanReaderForSelect)
        .Default(false);

    RegisterParameter("enable_consistent_chunk_replica_placement", EnableConsistentChunkReplicaPlacement)
        .Default(false);

    RegisterParameter("enable_detailed_profiling", EnableDetailedProfiling)
        .Default(false);
    RegisterParameter("enable_hunk_columnar_profiling", EnableHunkColumnarProfiling)
        .Default(false);

    RegisterParameter("min_hunk_compaction_total_hunk_length", MinHunkCompactionTotalHunkLength)
        .GreaterThanOrEqual(0)
        .Default(1_MB);
    RegisterParameter("max_hunk_compaction_garbage_ratio", MaxHunkCompactionGarbageRatio)
        .InRange(0.0, 1.0)
        .Default(0.5);

    RegisterParameter("max_hunk_compaction_size", MaxHunkCompactionSize)
        .GreaterThan(0)
        .Default(8_MB);
    RegisterParameter("hunk_compaction_size_base", HunkCompactionSizeBase)
        .GreaterThan(0)
        .Default(16_MB);
    RegisterParameter("hunk_compaction_size_ratio", HunkCompactionSizeRatio)
        .GreaterThan(1.0)
        .Default(100.0);
    RegisterParameter("min_hunk_compaction_chunk_count", MinHunkCompactionChunkCount)
        .GreaterThan(1)
        .Default(2);
    RegisterParameter("max_hunk_compaction_chunk_count", MaxHunkCompactionChunkCount)
        .GreaterThan(1)
        .Default(5);

    RegisterPostprocessor([&] () {
        if (MaxDynamicStoreRowCount > MaxDynamicStoreValueCount) {
            THROW_ERROR_EXCEPTION("\"max_dynamic_store_row_count\" must be less than or equal to \"max_dynamic_store_value_count\"");
        }
        if (MinPartitionDataSize >= DesiredPartitionDataSize) {
            THROW_ERROR_EXCEPTION("\"min_partition_data_size\" must be less than \"desired_partition_data_size\"");
        }
        if (DesiredPartitionDataSize >= MaxPartitionDataSize) {
            THROW_ERROR_EXCEPTION("\"desired_partition_data_size\" must be less than \"max_partition_data_size\"");
        }
        if (MaxPartitioningStoreCount < MinPartitioningStoreCount) {
            THROW_ERROR_EXCEPTION("\"max_partitioning_store_count\" must be greater than or equal to \"min_partitioning_store_count\"");
        }
        if (MaxPartitioningDataSize < MinPartitioningDataSize) {
            THROW_ERROR_EXCEPTION("\"max_partitioning_data_size\" must be greater than or equal to \"min_partitioning_data_size\"");
        }
        if (MaxCompactionStoreCount < MinCompactionStoreCount) {
            THROW_ERROR_EXCEPTION("\"max_compaction_store_count\" must be greater than or equal to \"min_compaction_chunk_count\"");
        }
        if (MaxHunkCompactionChunkCount < MinHunkCompactionChunkCount) {
            THROW_ERROR_EXCEPTION("\"max_hunk_compaction_chunk_count\" must be greater than or equal to \"min_hunk_compaction_chunk_count\"");
        }
        if (EnableLookupHashTable && InMemoryMode != NTabletClient::EInMemoryMode::Uncompressed) {
            THROW_ERROR_EXCEPTION("\"enable_lookup_hash_table\" can only be true if \"in_memory_mode\" is \"uncompressed\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TTransactionManagerConfig::TTransactionManagerConfig()
{
    RegisterParameter("max_transaction_timeout", MaxTransactionTimeout)
        .GreaterThan(TDuration())
        .Default(TDuration::Seconds(60));
    RegisterParameter("barrier_check_period", BarrierCheckPeriod)
        .Default(TDuration::MilliSeconds(100));
    RegisterParameter("max_aborted_transaction_pool_size", MaxAbortedTransactionPoolSize)
        .Default(1000);
}

////////////////////////////////////////////////////////////////////////////////

TTabletStoreReaderConfig::TTabletStoreReaderConfig()
{
    RegisterParameter("prefer_local_replicas", PreferLocalReplicas)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

TTabletManagerConfig::TTabletManagerConfig()
{
    RegisterParameter("pool_chunk_size", PoolChunkSize)
        .GreaterThan(64_KB)
        .Default(1_MB);

    RegisterParameter("max_blocked_row_wait_time", MaxBlockedRowWaitTime)
        .Default(TDuration::Seconds(5));

    RegisterParameter("preload_backoff_time", PreloadBackoffTime)
        .Default(TDuration::Minutes(1));
    RegisterParameter("compaction_backoff_time", CompactionBackoffTime)
        .Default(TDuration::Minutes(1));
    RegisterParameter("flush_backoff_time", FlushBackoffTime)
        .Default(TDuration::Minutes(1));

    RegisterParameter("changelog_codec", ChangelogCodec)
        .Default(NCompression::ECodec::Lz4);

    RegisterParameter("client_timestamp_threshold", ClientTimestampThreshold)
        .Default(TDuration::Minutes(1));

    RegisterParameter("replicator_thread_pool_size", ReplicatorThreadPoolSize)
        .GreaterThan(0)
        .Default(1);
    RegisterParameter("replicator_soft_backoff_time", ReplicatorSoftBackoffTime)
        .Default(TDuration::Seconds(3));
    RegisterParameter("replicator_hard_backoff_time", ReplicatorHardBackoffTime)
        .Default(TDuration::Seconds(60));

    RegisterParameter("tablet_cell_decommission_check_period", TabletCellDecommissionCheckPeriod)
        .Default(TDuration::Seconds(10));

    RegisterParameter("sleep_before_post_to_master", SleepBeforePostToMaster)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TTabletManagerDynamicConfig::TTabletManagerDynamicConfig()
{
    RegisterParameter("replicator_thread_pool_size", ReplicatorThreadPoolSize)
        .GreaterThan(0)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TStoreFlusherConfig::TStoreFlusherConfig()
{
    RegisterParameter("thread_pool_size", ThreadPoolSize)
        .GreaterThan(0)
        .Default(1);
    RegisterParameter("max_concurrent_flushes", MaxConcurrentFlushes)
        .GreaterThan(0)
        .Default(16);
    RegisterParameter("min_forced_flush_data_size", MinForcedFlushDataSize)
        .GreaterThan(0)
        .Default(1_MB);
}

////////////////////////////////////////////////////////////////////////////////

TStoreFlusherDynamicConfig::TStoreFlusherDynamicConfig()
{
    RegisterParameter("enable", Enable)
        .Default(true);
    RegisterParameter("forced_rotation_memory_ratio", ForcedRotationMemoryRatio)
        .InRange(0.0, 1.0)
        .Optional();
    RegisterParameter("enable_forced_rotation_backing_memory_accounting", EnableForcedRotationBackingMemoryAccounting)
        .Optional();
    RegisterParameter("thread_pool_size", ThreadPoolSize)
        .GreaterThan(0)
        .Optional();
    RegisterParameter("max_concurrent_flushes", MaxConcurrentFlushes)
        .GreaterThan(0)
        .Optional();
    RegisterParameter("min_forced_flush_data_size", MinForcedFlushDataSize)
        .GreaterThan(0)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TStoreCompactorConfig::TStoreCompactorConfig()
{
    RegisterParameter("thread_pool_size", ThreadPoolSize)
        .GreaterThan(0)
        .Default(1);
    RegisterParameter("max_concurrent_compactions", MaxConcurrentCompactions)
        .GreaterThan(0)
        .Default(1);
    RegisterParameter("max_concurrent_partitionings", MaxConcurrentPartitionings)
        .GreaterThan(0)
        .Default(1);
}

////////////////////////////////////////////////////////////////////////////////

TStoreCompactorDynamicConfig::TStoreCompactorDynamicConfig()
{
    RegisterParameter("enable", Enable)
        .Default(true);
    RegisterParameter("thread_pool_size", ThreadPoolSize)
        .GreaterThan(0)
        .Optional();
    RegisterParameter("max_concurrent_compactions", MaxConcurrentCompactions)
        .GreaterThan(0)
        .Optional();
    RegisterParameter("max_concurrent_partitionings", MaxConcurrentPartitionings)
        .GreaterThan(0)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TStoreTrimmerDynamicConfig::TStoreTrimmerDynamicConfig()
{
    RegisterParameter("enable", Enable)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

THunkChunkSweeperDynamicConfig::THunkChunkSweeperDynamicConfig()
{
    RegisterParameter("enable", Enable)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

TInMemoryManagerConfig::TInMemoryManagerConfig()
{
    RegisterParameter("max_concurrent_preloads", MaxConcurrentPreloads)
        .GreaterThan(0)
        .Default(1);
    RegisterParameter("intercepted_data_retention_time", InterceptedDataRetentionTime)
        .Default(TDuration::Seconds(30));
    RegisterParameter("ping_period", PingPeriod)
        .Default(TDuration::Seconds(10));
    RegisterParameter("control_rpc_timeout", ControlRpcTimeout)
        .Default(TDuration::Seconds(10));
    RegisterParameter("heavy_rpc_timeout", HeavyRpcTimeout)
        .Default(TDuration::Seconds(20));
    RegisterParameter("batch_size", BatchSize)
        .Default(16_MB);
    RegisterParameter("workload_descriptor", WorkloadDescriptor)
        .Default(TWorkloadDescriptor(EWorkloadCategory::UserBatch));
    RegisterParameter("preload_throttler", PreloadThrottler)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TPartitionBalancerConfig::TPartitionBalancerConfig()
{
    RegisterParameter("chunk_location_throttler", ChunkLocationThrottler)
        .DefaultNew();
    RegisterParameter("chunk_scraper", ChunkScraper)
        .DefaultNew();
    RegisterParameter("samples_fetcher", SamplesFetcher)
        .DefaultNew();
    RegisterParameter("min_partitioning_sample_count", MinPartitioningSampleCount)
        .Default(10)
        .GreaterThanOrEqual(3);
    RegisterParameter("max_partitioning_sample_count", MaxPartitioningSampleCount)
        .Default(1000)
        .GreaterThanOrEqual(10);
    RegisterParameter("max_concurrent_samplings", MaxConcurrentSamplings)
        .GreaterThan(0)
        .Default(8);
    RegisterParameter("resampling_period", ResamplingPeriod)
        .Default(TDuration::Minutes(1));
    RegisterParameter("split_retry_delay", SplitRetryDelay)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

TPartitionBalancerDynamicConfig::TPartitionBalancerDynamicConfig()
{
    RegisterParameter("enable", Enable)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

TSecurityManagerConfig::TSecurityManagerConfig()
{
    RegisterParameter("resource_limits_cache", ResourceLimitsCache)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

TSecurityManagerDynamicConfig::TSecurityManagerDynamicConfig()
{
    RegisterParameter("resource_limits_cache", ResourceLimitsCache)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TMasterConnectorConfig::TMasterConnectorConfig()
{
    RegisterParameter("heartbeat_period", HeartbeatPeriod)
        .Default(TDuration::Seconds(30));
    RegisterParameter("heartbeat_period_splay", HeartbeatPeriodSplay)
        .Default(TDuration::Seconds(1));
    RegisterParameter("heartbeat_timeout", HeartbeatTimeout)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

TMasterConnectorDynamicConfig::TMasterConnectorDynamicConfig()
{
    RegisterParameter("heartbeat_period", HeartbeatPeriod)
        .Default();
    RegisterParameter("heartbeat_period_splay", HeartbeatPeriodSplay)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TResourceLimitsConfig::TResourceLimitsConfig()
{
    RegisterParameter("slots", Slots)
        .GreaterThanOrEqual(0)
        .Default(4);
    RegisterParameter("tablet_static_memory", TabletStaticMemory)
        .GreaterThanOrEqual(0)
        .Default(std::numeric_limits<i64>::max());
    RegisterParameter("tablet_dynamic_memory", TabletDynamicMemory)
        .GreaterThanOrEqual(0)
        .Default(std::numeric_limits<i64>::max());
}

////////////////////////////////////////////////////////////////////////////////

TBackupManagerDynamicConfig::TBackupManagerDynamicConfig()
{
    RegisterParameter("checkpoint_feasibility_check_batch_period", CheckpointFeasibilityCheckBatchPeriod)
        .Default(TDuration::MilliSeconds(100));
    RegisterParameter("checkpoint_feasibility_check_backoff", CheckpointFeasibilityCheckBackoff)
        .Default(TDuration::Seconds(1));
}

////////////////////////////////////////////////////////////////////////////////

TTabletNodeDynamicConfig::TTabletNodeDynamicConfig()
{
    RegisterParameter("slots", Slots)
        .Optional();

    RegisterParameter("tablet_manager", TabletManager)
        .DefaultNew();

    RegisterParameter("throttlers", Throttlers)
        .Optional();

    RegisterParameter("store_compactor", StoreCompactor)
        .DefaultNew();
    RegisterParameter("store_flusher", StoreFlusher)
        .DefaultNew();
    RegisterParameter("store_trimmer", StoreTrimmer)
        .DefaultNew();
    RegisterParameter("hunk_chunk_sweeper", HunkChunkSweeper)
        .DefaultNew();
    RegisterParameter("partition_balancer", PartitionBalancer)
        .DefaultNew();

    RegisterParameter("versioned_chunk_meta_cache", VersionedChunkMetaCache)
        .DefaultNew();

    RegisterParameter("column_evaluator_cache", ColumnEvaluatorCache)
        .DefaultNew();

    RegisterParameter("enable_structured_logger", EnableStructuredLogger)
        .Default(true);
    RegisterParameter("full_structured_tablet_heartbeat_period", FullStructuredTabletHeartbeatPeriod)
        .Default(TDuration::Minutes(5));
    RegisterParameter("incremental_structured_tablet_heartbeat_period", IncrementalStructuredTabletHeartbeatPeriod)
        .Default(TDuration::Seconds(5));

    RegisterParameter("master_connector", MasterConnector)
        .DefaultNew();

    RegisterParameter("security_manager", SecurityManager)
        .DefaultNew();

    RegisterParameter("backup_manager", BackupManager)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

THintManagerConfig::THintManagerConfig()
{
    RegisterParameter("replicator_hint_config_fetcher", ReplicatorHintConfigFetcher)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

TTabletNodeConfig::TTabletNodeConfig()
{
    RegisterParameter("forced_rotation_memory_ratio", ForcedRotationMemoryRatio)
        .InRange(0.0, 1.0)
        .Default(0.8)
        .Alias("forced_rotations_memory_ratio");
    RegisterParameter("enable_forced_rotation_backing_memory_accounting", EnableForcedRotationBackingMemoryAccounting)
        .Default(true);

    RegisterParameter("resource_limits", ResourceLimits)
        .DefaultNew();

    RegisterParameter("snapshots", Snapshots)
        .DefaultNew();
    RegisterParameter("changelogs", Changelogs)
        .DefaultNew();
    RegisterParameter("hydra_manager", HydraManager)
        .DefaultNew();
    RegisterParameter("election_manager", ElectionManager)
        .DefaultNew();
    RegisterParameter("hive_manager", HiveManager)
        .DefaultNew();
    RegisterParameter("transaction_manager", TransactionManager)
        .DefaultNew();
    RegisterParameter("transaction_supervisor", TransactionSupervisor)
        .DefaultNew();
    RegisterParameter("tablet_manager", TabletManager)
        .DefaultNew();
    RegisterParameter("store_flusher", StoreFlusher)
        .DefaultNew();
    RegisterParameter("store_compactor", StoreCompactor)
        .DefaultNew();
    RegisterParameter("in_memory_manager", InMemoryManager)
        .DefaultNew();
    RegisterParameter("partition_balancer", PartitionBalancer)
        .DefaultNew();
    RegisterParameter("security_manager", SecurityManager)
        .DefaultNew();
    RegisterParameter("hint_manager", HintManager)
        .DefaultNew();

    RegisterParameter("versioned_chunk_meta_cache", VersionedChunkMetaCache)
        .DefaultNew(10_GB);

    RegisterParameter("throttlers", Throttlers)
        .Optional();

    // COMPAT(babenko): use /tablet_node/throttlers instead.
    RegisterParameter("store_flush_out_throttler", Throttlers[ETabletNodeThrottlerKind::StoreFlushOut])
        .Optional();
    RegisterParameter("store_compaction_and_partitioning_in_throttler", Throttlers[ETabletNodeThrottlerKind::StoreCompactionAndPartitioningIn])
        .Optional();
    RegisterParameter("store_compaction_and_partitioning_out_throttler", Throttlers[ETabletNodeThrottlerKind::StoreCompactionAndPartitioningOut])
        .Optional();
    RegisterParameter("replication_in_throttler", Throttlers[ETabletNodeThrottlerKind::ReplicationIn])
        .Optional();
    RegisterParameter("replication_out_throttler", Throttlers[ETabletNodeThrottlerKind::ReplicationOut])
        .Optional();
    RegisterParameter("dynamic_store_read_out_throttler", Throttlers[ETabletNodeThrottlerKind::DynamicStoreReadOut])
        .Optional();

    RegisterParameter("slot_scan_period", SlotScanPeriod)
        .Default(TDuration::Seconds(1));

    RegisterParameter("tablet_snapshot_eviction_timeout", TabletSnapshotEvictionTimeout)
        .Default(TDuration::Seconds(5));

    RegisterParameter("column_evaluator_cache", ColumnEvaluatorCache)
        .DefaultNew();

    RegisterParameter("master_connector", MasterConnector)
        .DefaultNew();

    RegisterPreprocessor([&] {
        HydraManager->MaxCommitBatchDelay = TDuration::MilliSeconds(5);
    });

    RegisterPostprocessor([&] {
        // Instantiate default throttler configs.
        for (auto kind : TEnumTraits<ETabletNodeThrottlerKind>::GetDomainValues()) {
            if (Throttlers[kind]) {
                continue;
            }

            switch (kind) {
                case ETabletNodeThrottlerKind::StaticStorePreloadIn:
                case ETabletNodeThrottlerKind::DynamicStoreReadOut:
                    Throttlers[kind] = New<NConcurrency::TRelativeThroughputThrottlerConfig>(100_MB);
                    break;

                default:
                    Throttlers[kind] = New<NConcurrency::TRelativeThroughputThrottlerConfig>();
            }
        }

        if (InMemoryManager->PreloadThrottler) {
            Throttlers[ETabletNodeThrottlerKind::StaticStorePreloadIn] = InMemoryManager->PreloadThrottler;
        }

        // COMPAT(akozhikhov): set to false when masters are updated too.
        HintManager->ReplicatorHintConfigFetcher->IgnoreConfigAbsence = true;
    });
}

////////////////////////////////////////////////////////////////////////////////

TReplicatorHintConfig::TReplicatorHintConfig()
{
    RegisterParameter("banned_replica_clusters", BannedReplicaClusters)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
