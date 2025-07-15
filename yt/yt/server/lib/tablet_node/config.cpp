#include "config.h"

#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NTabletNode {

using namespace NConcurrency;
using namespace NYTree;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

void TRelativeReplicationThrottlerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);
    registrar.Parameter("ratio", &TThis::Ratio)
        .GreaterThan(0.0)
        .Default(2.0);
    registrar.Parameter("activation_threshold", &TThis::ActivationThreshold)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("window_size", &TThis::WindowSize)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("max_timestamps_to_keep", &TThis::MaxTimestampsToKeep)
        .GreaterThan(0)
        .Default(100);
}

////////////////////////////////////////////////////////////////////////////////

void TRowDigestCompactionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_obsolete_timestamp_ratio", &TThis::MaxObsoleteTimestampRatio)
        .Default(0.5);
    registrar.Parameter("max_timestamps_per_value", &TThis::MaxTimestampsPerValue)
        .GreaterThanOrEqual(1)
        .Default(8'192);
}

bool operator==(const TRowDigestCompactionConfig& lhs, const TRowDigestCompactionConfig& rhs)
{
    return lhs.MaxObsoleteTimestampRatio == rhs.MaxObsoleteTimestampRatio &&
        lhs.MaxTimestampsPerValue == rhs.MaxTimestampsPerValue;
}

////////////////////////////////////////////////////////////////////////////////

void TGradualCompactionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("start_time", &TThis::StartTime)
        .Default();
    registrar.Parameter("duration", &TThis::Duration)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TBuiltinTableMountConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("tablet_cell_bundle", &TThis::TabletCellBundle)
        .Default();
    registrar.Parameter("in_memory_mode", &TThis::InMemoryMode)
        .Default(NTabletClient::EInMemoryMode::None);
    registrar.Parameter("forced_compaction_revision", &TThis::ForcedCompactionRevision)
        .Default();
    registrar.Parameter("forced_store_compaction_revision", &TThis::ForcedStoreCompactionRevision)
        .Default();
    registrar.Parameter("forced_hunk_compaction_revision", &TThis::ForcedHunkCompactionRevision)
        .Default();
    registrar.Parameter("forced_chunk_view_compaction_revision", &TThis::ForcedChunkViewCompactionRevision)
        .Default();
    registrar.Parameter("profiling_mode", &TThis::ProfilingMode)
        .Default(EDynamicTableProfilingMode::Path);
    registrar.Parameter("profiling_tag", &TThis::ProfilingTag)
        .Optional();
    registrar.Parameter("enable_dynamic_store_read", &TThis::EnableDynamicStoreRead)
        .Default(false);
    registrar.Parameter("enable_consistent_chunk_replica_placement", &TThis::EnableConsistentChunkReplicaPlacement)
        .Default(false);
    registrar.Parameter("enable_detailed_profiling", &TThis::EnableDetailedProfiling)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TTestingTableMountConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("compaction_failure_probability", &TThis::CompactionFailureProbability)
        .Default(0)
        .InRange(0, 1);

    registrar.Parameter("partitioning_failure_probability", &TThis::PartitioningFailureProbability)
        .Default(0)
        .InRange(0, 1);

    registrar.Parameter("flush_failure_probability", &TThis::FlushFailureProbability)
        .Default(0)
        .InRange(0, 1);

    registrar.Parameter("simulated_tablet_snapshot_delay", &TThis::SimulatedTabletSnapshotDelay)
        .Default();

    registrar.Parameter("simulated_store_preload_delay", &TThis::SimulatedStorePreloadDelay)
        .Default();

    registrar.Parameter("sync_delay_in_write_transaction_commit", &TThis::SyncDelayInWriteTransactionCommit)
        .Default();

    registrar.Parameter("sorted_store_manager_hash_check_probability", &TThis::SortedStoreManagerRowHashCheckProbability)
        .Default(0)
        .InRange(0, 1);

    registrar.Parameter("table_puller_replica_ban_iteration_count", &TThis::TablePullerReplicaBanIterationCount)
        .GreaterThan(0)
        .Optional();

    registrar.Parameter("write_response_delay", &TThis::WriteResponseDelay)
        .Default();

    registrar.Parameter("opaque_stores_in_orchid", &TThis::OpaqueStoresInOrchid)
        .Default(true);

    registrar.Parameter("opaque_settings_in_orchid", &TThis::OpaqueSettingsInOrchid)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TCustomTableMountConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_dynamic_store_row_count", &TThis::MaxDynamicStoreRowCount)
        .GreaterThan(0)
        .Default(1'000'000);
    registrar.Parameter("max_dynamic_store_value_count", &TThis::MaxDynamicStoreValueCount)
        .GreaterThan(0)
        .Default(1'000'000'000);
    registrar.Parameter("max_dynamic_store_timestamp_count", &TThis::MaxDynamicStoreTimestampCount)
        .GreaterThan(TMaxDynamicStoreTimestampCount(0))
        .Default(TMaxDynamicStoreTimestampCount(10'000'000))
        // NB: This limit is really important; please consult babenko@
        // before changing it.
        // NB: Additional clumps relevant to specific revision provider will be applied.
        .LessThanOrEqual(TMaxDynamicStoreTimestampCount(SoftRevisionsPerDynamicStoreLimit));
    registrar.Parameter("max_dynamic_store_pool_size", &TThis::MaxDynamicStorePoolSize)
        .GreaterThan(0)
        .Default(1_GB);
    registrar.Parameter("max_dynamic_store_row_data_weight", &TThis::MaxDynamicStoreRowDataWeight)
        .GreaterThan(0)
        .Default(NTableClient::MaxClientVersionedRowDataWeight)
        // NB: This limit is important: it ensures that store is flushable.
        // Please consult savrus@ before changing.
        .LessThanOrEqual(NTableClient::MaxServerVersionedRowDataWeight / 2);

    registrar.Parameter("dynamic_store_overflow_threshold", &TThis::DynamicStoreOverflowThreshold)
        .GreaterThan(0.0)
        .Default(0.7)
        .LessThanOrEqual(1.0);

    registrar.Parameter("max_partition_data_size", &TThis::MaxPartitionDataSize)
        .Default(320_MB)
        .GreaterThan(0);
    registrar.Parameter("desired_partition_data_size", &TThis::DesiredPartitionDataSize)
        .Default(256_MB)
        .GreaterThan(0);
    registrar.Parameter("min_partition_data_size", &TThis::MinPartitionDataSize)
        .Default(96_MB)
        .GreaterThan(0);

    registrar.Parameter("max_partition_count", &TThis::MaxPartitionCount)
        .Default(10'240)
        .GreaterThan(0);

    registrar.Parameter("min_partitioning_data_size", &TThis::MinPartitioningDataSize)
        .Default(64_MB)
        .GreaterThan(0);
    registrar.Parameter("min_partitioning_store_count", &TThis::MinPartitioningStoreCount)
        .Default(1)
        .GreaterThan(0);
    registrar.Parameter("max_partitioning_data_size", &TThis::MaxPartitioningDataSize)
        .Default(1_GB)
        .GreaterThan(0);
    registrar.Parameter("max_partitioning_store_count", &TThis::MaxPartitioningStoreCount)
        .Default(5)
        .GreaterThan(0);

    registrar.Parameter("min_compaction_store_count", &TThis::MinCompactionStoreCount)
        .Default(3)
        .GreaterThan(1);
    registrar.Parameter("max_compaction_store_count", &TThis::MaxCompactionStoreCount)
        .Default(5)
        .GreaterThan(0);
    registrar.Parameter("compaction_data_size_base", &TThis::CompactionDataSizeBase)
        .Default(16_MB)
        .GreaterThan(0);
    registrar.Parameter("compaction_data_size_ratio", &TThis::CompactionDataSizeRatio)
        .Default(2.0)
        .GreaterThan(1.0);

    registrar.Parameter("global_compaction", &TThis::GlobalCompaction)
        .Default();

    registrar.Parameter("flush_throttler", &TThis::FlushThrottler)
        .DefaultNew();
    registrar.Parameter("compaction_throttler", &TThis::CompactionThrottler)
        .DefaultNew();
    registrar.Parameter("partitioning_throttler", &TThis::PartitioningThrottler)
        .DefaultNew();

    registrar.Parameter("throttlers", &TThis::Throttlers)
        .Default();

    registrar.Parameter("samples_per_partition", &TThis::SamplesPerPartition)
        .Default(100)
        .GreaterThanOrEqual(0);

    registrar.Parameter("backing_store_retention_time", &TThis::BackingStoreRetentionTime)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("max_read_fan_in", &TThis::MaxReadFanIn)
        .GreaterThan(0)
        .Default(30);

    registrar.Parameter("max_overlapping_store_count", &TThis::MaxOverlappingStoreCount)
        .GreaterThan(0)
        .Default(DefaultMaxOverlappingStoreCount);
    registrar.Parameter("critical_overlapping_store_count", &TThis::CriticalOverlappingStoreCount)
        .GreaterThan(0)
        .Optional();
    registrar.Parameter("overlapping_store_immediate_split_threshold", &TThis::OverlappingStoreImmediateSplitThreshold)
        .GreaterThan(0)
        .Default(20);

    registrar.Parameter("max_stores_per_tablet", &TThis::MaxStoresPerTablet)
        .Default(10'000)
        .GreaterThan(0);
    registrar.Parameter("max_eden_stores_per_tablet", &TThis::MaxEdenStoresPerTablet)
        .Default(100)
        .GreaterThan(0);

    registrar.Parameter("dynamic_store_auto_flush_period", &TThis::DynamicStoreAutoFlushPeriod)
        .Default(TDuration::Minutes(15));
    registrar.Parameter("dynamic_store_flush_period_splay", &TThis::DynamicStoreFlushPeriodSplay)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("auto_compaction_period", &TThis::AutoCompactionPeriod)
        .Default();
    registrar.Parameter("auto_compaction_period_splay_ratio", &TThis::AutoCompactionPeriodSplayRatio)
        .Default(0.3);
    registrar.Parameter("periodic_compaction_mode", &TThis::PeriodicCompactionMode)
        .Default(EPeriodicCompactionMode::Store);
    registrar.Parameter("row_digest_compaction", &TThis::RowDigestCompaction)
        .DefaultNew();

    registrar.Parameter("enable_lookup_hash_table", &TThis::EnableLookupHashTable)
        .Default(false);

    registrar.Parameter("lookup_cache_rows_per_tablet", &TThis::LookupCacheRowsPerTablet)
        .Default(0);
    registrar.Parameter("lookup_cache_rows_ratio", &TThis::LookupCacheRowsRatio)
        .Default(0)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(1);
    registrar.Parameter("enable_lookup_cache_by_default", &TThis::EnableLookupCacheByDefault)
        .Default(false);

    registrar.Parameter("row_count_to_keep", &TThis::RowCountToKeep)
        .Default(0);

    registrar.Parameter("replication_tick_period", &TThis::ReplicationTickPeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("min_replication_log_ttl", &TThis::MinReplicationLogTtl)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("max_timestamps_per_replication_commit", &TThis::MaxTimestampsPerReplicationCommit)
        .Default(10'000);
    registrar.Parameter("max_rows_per_replication_commit", &TThis::MaxRowsPerReplicationCommit)
        .Default(90'000);
    registrar.Parameter("max_data_weight_per_replication_commit", &TThis::MaxDataWeightPerReplicationCommit)
        .Default(128_MB);
    registrar.Parameter("max_replication_batch_span", &TThis::MaxReplicationBatchSpan)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("replication_throttler", &TThis::ReplicationThrottler)
        .DefaultNew();
    registrar.Parameter("relative_replication_throttler", &TThis::RelativeReplicationThrottler)
        .DefaultNew();
    registrar.Parameter("enable_replication_logging", &TThis::EnableReplicationLogging)
        .Default(false);

    registrar.Parameter("replication_progress_update_tick_period", &TThis::ReplicationProgressUpdateTickPeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("enable_profiling", &TThis::EnableProfiling)
        .Default(false);

    registrar.Parameter("enable_structured_logger", &TThis::EnableStructuredLogger)
        .Default(true);

    registrar.Parameter("enable_compaction_and_partitioning", &TThis::EnableCompactionAndPartitioning)
        .Default(true);

    registrar.Parameter("enable_partitioning", &TThis::EnablePartitioning)
        .Default(true);

    registrar.Parameter("enable_store_rotation", &TThis::EnableStoreRotation)
        .Default(true);

    registrar.Parameter("enable_store_flush", &TThis::EnableStoreFlush)
        .Default(true);

    registrar.Parameter("merge_rows_on_flush", &TThis::MergeRowsOnFlush)
        .Default(false);

    registrar.Parameter("merge_deletions_on_flush", &TThis::MergeDeletionsOnFlush)
        .Default(false);

    registrar.Parameter("row_merger_type", &TThis::RowMergerType)
        .Default(ERowMergerType::Legacy);

    registrar.Parameter("enable_lsm_verbose_logging", &TThis::EnableLsmVerboseLogging)
        .Default(false);

    registrar.Parameter("max_unversioned_block_size", &TThis::MaxUnversionedBlockSize)
        .GreaterThan(0)
        .Optional();

    registrar.Parameter("preserve_tablet_index", &TThis::PreserveTabletIndex)
        .Default(false);

    registrar.Parameter("enable_partition_split_while_eden_partitioning", &TThis::EnablePartitionSplitWhileEdenPartitioning)
        .Default(false);

    registrar.Parameter("enable_discarding_expired_partitions", &TThis::EnableDiscardingExpiredPartitions)
        .Default(true);

    registrar.Parameter("prioritize_eden_forced_compaction", &TThis::PrioritizeEdenForcedCompaction)
        .Default(false);

    registrar.Parameter("always_flush_to_eden", &TThis::AlwaysFlushToEden)
        .Default(false);

    registrar.Parameter("enable_data_node_lookup", &TThis::EnableDataNodeLookup)
        .Default(false);

    registrar.Parameter("enable_hash_chunk_index_for_lookup", &TThis::EnableHashChunkIndexForLookup)
        .Default(false);
    registrar.Parameter("enable_key_filter_for_lookup", &TThis::EnableKeyFilterForLookup)
        .Default(false);

    registrar.Parameter("lookup_rpc_multiplexing_parallelism", &TThis::LookupRpcMultiplexingParallelism)
        .Default(1)
        .InRange(1, 16);

    registrar.Parameter("single_column_group_by_default", &TThis::SingleColumnGroupByDefault)
        .Default(true);

    registrar.Parameter("enable_segment_meta_in_blocks", &TThis::EnableSegmentMetaInBlocks)
        .Default(false);
    registrar.Parameter("enable_column_meta_in_chunk_meta", &TThis::EnableColumnMetaInChunkMeta)
        .Default(true);

    registrar.Parameter("enable_hunk_columnar_profiling", &TThis::EnableHunkColumnarProfiling)
        .Default(false);

    registrar.Parameter("max_hunk_compaction_garbage_ratio", &TThis::MaxHunkCompactionGarbageRatio)
        .InRange(0.0, 1.0)
        .Default(0.5);

    registrar.Parameter("max_hunk_compaction_size", &TThis::MaxHunkCompactionSize)
        .GreaterThan(0)
        .Default(8_MB);
    registrar.Parameter("hunk_compaction_size_base", &TThis::HunkCompactionSizeBase)
        .GreaterThan(0)
        .Default(16_MB);
    registrar.Parameter("hunk_compaction_size_ratio", &TThis::HunkCompactionSizeRatio)
        .GreaterThan(1.0)
        .Default(100.0);
    registrar.Parameter("min_hunk_compaction_chunk_count", &TThis::MinHunkCompactionChunkCount)
        .GreaterThan(1)
        .Default(2);
    registrar.Parameter("max_hunk_compaction_chunk_count", &TThis::MaxHunkCompactionChunkCount)
        .GreaterThan(1)
        .Default(5);

    registrar.Parameter("enable_narrow_chunk_view_compaction", &TThis::EnableNarrowChunkViewCompaction)
        .Default(true);
    registrar.Parameter("max_chunk_view_size_ratio", &TThis::MaxChunkViewSizeRatio)
        .InRange(0.0, 1.0)
        .Default(0.5);

    registrar.Parameter("retry_read_on_ordered_store_rotation", &TThis::RetryReadOnOrderedStoreRotation)
        .Default(false);

    registrar.Parameter("precache_chunk_replicas_on_mount", &TThis::PrecacheChunkReplicasOnMount)
        .Default(true);
    registrar.Parameter("register_chunk_replicas_on_stores_update", &TThis::RegisterChunkReplicasOnStoresUpdate)
        .Default(true);

    registrar.Parameter("enable_replication_progress_advance_to_barrier", &TThis::EnableReplicationProgressAdvanceToBarrier)
        .Default(true);

    registrar.Parameter("max_ordered_tablet_data_weight", &TThis::MaxOrderedTabletDataWeight)
        .GreaterThan(0)
        .Default();

    registrar.Parameter("value_dictionary_compression", &TThis::ValueDictionaryCompression)
        .DefaultNew();

    registrar.Parameter("insert_meta_upon_store_update", &TThis::InsertMetaUponStoreUpdate)
        .Default(true)
        .DontSerializeDefault();

    registrar.Parameter("partition_reader_prefetch_key_limit", &TThis::PartitionReaderPrefetchKeyLimit)
        .Default()
        .DontSerializeDefault();

    registrar.Parameter("testing", &TThis::Testing)
        .Default();

    registrar.Postprocessor([&] (TCustomTableMountConfig* config) {
        if (config->MaxDynamicStoreRowCount > config->MaxDynamicStoreValueCount) {
            THROW_ERROR_EXCEPTION("\"max_dynamic_store_row_count\" must be less than or equal to \"max_dynamic_store_value_count\"");
        }
        if (config->MinPartitionDataSize >= config->DesiredPartitionDataSize) {
            THROW_ERROR_EXCEPTION("\"min_partition_data_size\" must be less than \"desired_partition_data_size\"");
        }
        if (config->DesiredPartitionDataSize >= config->MaxPartitionDataSize) {
            THROW_ERROR_EXCEPTION("\"desired_partition_data_size\" must be less than \"max_partition_data_size\"");
        }
        if (config->MaxPartitioningStoreCount < config->MinPartitioningStoreCount) {
            THROW_ERROR_EXCEPTION("\"max_partitioning_store_count\" must be greater than or equal to \"min_partitioning_store_count\"");
        }
        if (config->MaxPartitioningDataSize < config->MinPartitioningDataSize) {
            THROW_ERROR_EXCEPTION("\"max_partitioning_data_size\" must be greater than or equal to \"min_partitioning_data_size\"");
        }
        if (config->MaxCompactionStoreCount < config->MinCompactionStoreCount) {
            THROW_ERROR_EXCEPTION("\"max_compaction_store_count\" must be greater than or equal to \"min_compaction_store_count\"");
        }
        if (config->MaxHunkCompactionChunkCount < config->MinHunkCompactionChunkCount) {
            THROW_ERROR_EXCEPTION("\"max_hunk_compaction_chunk_count\" must be greater than or equal to \"min_hunk_compaction_chunk_count\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TTableMountConfig::Register(TRegistrar registrar)
{
    registrar.Postprocessor([&] (TTableMountConfig* config) {
        if (config->EnableLookupHashTable && config->InMemoryMode != NTabletClient::EInMemoryMode::Uncompressed) {
            THROW_ERROR_EXCEPTION("\"enable_lookup_hash_table\" can only be true if \"in_memory_mode\" is \"uncompressed\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TTabletStoreReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("prefer_local_replicas", &TThis::PreferLocalReplicas)
        .Default(true);
    registrar.Parameter("hedging_manager", &TThis::HedgingManager)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TTabletHunkReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("hedging_manager", &TThis::HedgingManager)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TTabletHunkWriterConfig::Register(TRegistrar registrar)
{
    registrar.Preprocessor([&] (TTabletHunkWriterConfig* config) {
        config->EnableStripedErasure = true;
    });

    registrar.Postprocessor([&] (TTabletHunkWriterConfig* config) {
        if (!config->EnableStripedErasure) {
            THROW_ERROR_EXCEPTION("Hunk chunk writer must use striped erasure writer");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TSecurityManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("resource_limits_cache", &TThis::ResourceLimitsCache)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TSecurityManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("resource_limits_cache", &TThis::ResourceLimitsCache)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TReplicatorHintConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("banned_replica_clusters", &TThis::BannedReplicaClusters)
        .Default();

    registrar.Parameter("enable_incoming_replication", &TThis::EnableIncomingReplication)
        .Default(true);

    registrar.Parameter("preferred_sync_replica_clusters", &TThis::PreferredSyncReplicaClusters)
        .Default()
        .DontSerializeDefault();
}

////////////////////////////////////////////////////////////////////////////////

void THunkStorageMountConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("desired_allocated_store_count", &TThis::DesiredAllocatedStoreCount)
        .Default(1);

    registrar.Parameter("store_rotation_period", &TThis::StoreRotationPeriod)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("store_removal_grace_period", &TThis::StoreRemovalGracePeriod)
        .Default(TDuration::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

void THunkStoreWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("desired_hunk_count_per_chunk", &TThis::DesiredHunkCountPerChunk)
        .Default(10'000'000);
    registrar.Parameter("desired_chunk_size", &TThis::DesiredChunkSize)
        .Default(1_GB);
}

////////////////////////////////////////////////////////////////////////////////

void THunkStoreWriterOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("medium_name", &TThis::MediumName);
    registrar.Parameter("account", &TThis::Account);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
