#pragma once

#include "public.h"

#include <yt/server/data_node/config.h>

#include <yt/server/hive/config.h>

#include <yt/server/hydra/config.h>

#include <yt/server/tablet_node/config.h>

#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/misc/workload.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/core/compression/public.h>

#include <yt/core/misc/config.h>

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/concurrency/config.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTabletHydraManagerConfig
    : public NHydra::TDistributedHydraManagerConfig
{
public:
    NRpc::TResponseKeeperConfigPtr ResponseKeeper;

    TTabletHydraManagerConfig()
    {
        RegisterParameter("response_keeper", ResponseKeeper)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletHydraManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTableMountConfig
    : public NTableClient::TRetentionConfig
{
public:
    i64 MaxDynamicStoreRowCount;
    i64 MaxDynamicStoreValueCount;
    i64 MaxDynamicStoreTimestampCount;
    i64 MaxDynamicStorePoolSize;
    i64 MaxDynamicStoreRowDataWeight;

    double DynamicStoreOverflowThreshold;

    i64 MaxPartitionDataSize;
    i64 DesiredPartitionDataSize;
    i64 MinPartitionDataSize;

    int MaxPartitionCount;

    i64 MinPartitioningDataSize;
    int MinPartitioningStoreCount;
    i64 MaxPartitioningDataSize;
    int MaxPartitioningStoreCount;

    int MinCompactionStoreCount;
    int MaxCompactionStoreCount;
    i64 CompactionDataSizeBase;
    double CompactionDataSizeRatio;
    i64 MaxCompactionDataSize;

    int SamplesPerPartition;

    TDuration BackingStoreRetentionTime;

    int MaxReadFanIn;

    int MaxOverlappingStoreCount;

    NTabletClient::EInMemoryMode InMemoryMode;

    int MaxStoresPerTablet;

    TNullable<ui64> ForcedCompactionRevision;

    TDuration DynamicStoreAutoFlushPeriod;
    TNullable<TDuration> AutoCompactionPeriod;

    bool EnableLookupHashTable;

    TDuration MinReplicationLogTtl;
    int MaxTimestampsPerReplicationCommit;
    int MaxRowsPerReplicationCommit;
    i64 MaxDataWeightPerReplicationCommit;
    NConcurrency::TThroughputThrottlerConfigPtr ReplicationThrottler;
    bool EnableReplicationLogging;

    bool EnableProfiling;
    EDynamicTableProfilingMode ProfilingMode;
    TString ProfilingTag;

    bool EnableCompactionAndPartitioning;

    bool MergeRowsOnFlush;

    TTableMountConfig()
    {
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
        RegisterParameter("max_compaction_data_size", MaxCompactionDataSize)
            .Default(320_MB)
            .GreaterThan(0);

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
            .Default(30);

        RegisterParameter("in_memory_mode", InMemoryMode)
            .Default(NTabletClient::EInMemoryMode::None);

        RegisterParameter("max_stores_per_tablet", MaxStoresPerTablet)
            .Default(10000)
            .GreaterThan(0);

        RegisterParameter("forced_compaction_revision", ForcedCompactionRevision)
            .Default(Null);

        RegisterParameter("dynamic_store_auto_flush_period", DynamicStoreAutoFlushPeriod)
            .Default(TDuration::Minutes(15));
        RegisterParameter("auto_compaction_period", AutoCompactionPeriod)
            .Default(Null);

        RegisterParameter("enable_lookup_hash_table", EnableLookupHashTable)
            .Default(false);

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

        RegisterParameter("enable_compaction_and_partitioning", EnableCompactionAndPartitioning)
            .Default(true);

        RegisterParameter("merge_rows_on_flush", MergeRowsOnFlush)
            .Default(false);

        RegisterValidator([&] () {
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
            if (EnableLookupHashTable && InMemoryMode != NTabletClient::EInMemoryMode::Uncompressed) {
                THROW_ERROR_EXCEPTION("\"enable_lookup_hash_table\" can only be true if \"in_memory_mode\" is \"uncompressed\"");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TTableMountConfig)

////////////////////////////////////////////////////////////////////////////////

class TTransactionManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MaxTransactionTimeout;
    TDuration BarrierCheckPeriod;
    int MaxAbortedTransactionPoolSize;

    TTransactionManagerConfig()
    {
        RegisterParameter("max_transaction_timeout", MaxTransactionTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(60));
        RegisterParameter("barrier_check_period", BarrierCheckPeriod)
            .Default(TDuration::MilliSeconds(100));
        RegisterParameter("max_aborted_transaction_pool_size", MaxAbortedTransactionPoolSize)
            .Default(1000);
    }
};

DEFINE_REFCOUNTED_TYPE(TTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletChunkReaderConfig
    : public NTableClient::TChunkReaderConfig
    , public NChunkClient::TErasureReaderConfig
{
public:
    bool PreferLocalReplicas;

    TTabletChunkReaderConfig()
    {
        RegisterParameter("prefer_local_replicas", PreferLocalReplicas)
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletChunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    i64 PoolChunkSize;
    double MaxPoolSmallBlockRatio;

    TDuration PreloadBackoffTime;
    TDuration CompactionBackoffTime;
    TDuration FlushBackoffTime;

    TDuration MaxBlockedRowWaitTime;

    NCompression::ECodec ChangelogCodec;

    //! When committing a non-atomic transaction, clients provide timestamps based
    //! on wall clock readings. These timestamps are checked for sanity using the server-side
    //! timestamp estimates.
    TDuration ClientTimestampThreshold;

    int ReplicatorThreadPoolSize;
    TDuration ReplicatorSoftBackoffTime;
    TDuration ReplicatorHardBackoffTime;

    TTabletManagerConfig()
    {
        RegisterParameter("pool_chunk_size", PoolChunkSize)
            .GreaterThan(64_KB)
            .Default(1_MB);

        RegisterParameter("max_pool_small_block_ratio", MaxPoolSmallBlockRatio)
            .InRange(0.0, 1.0)
            .Default(0.25);

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
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TStoreFlusherConfig
    : public NYTree::TYsonSerializable
{
public:
    int ThreadPoolSize;
    int MaxConcurrentFlushes;
    i64 MinForcedFlushDataSize;

    TStoreFlusherConfig()
    {
        RegisterParameter("thread_pool_size", ThreadPoolSize)
            .GreaterThan(0)
            .Default(1);
        RegisterParameter("max_concurrent_flushes", MaxConcurrentFlushes)
            .GreaterThan(0)
            .Default(1);
        RegisterParameter("min_forced_flush_data_size", MinForcedFlushDataSize)
            .GreaterThan(0)
            .Default(1_MB);
    }
};

DEFINE_REFCOUNTED_TYPE(TStoreFlusherConfig)

////////////////////////////////////////////////////////////////////////////////

class TStoreCompactorConfig
    : public NYTree::TYsonSerializable
{
public:
    int ThreadPoolSize;
    int MaxConcurrentCompactions;
    int MaxConcurrentPartitionings;
    int PartitioningWriterPoolSize;

    TStoreCompactorConfig()
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
        RegisterParameter("partitioning_writer_pool_size", PartitioningWriterPoolSize)
            .GreaterThan(0)
            .Default(10);
    }
};

DEFINE_REFCOUNTED_TYPE(TStoreCompactorConfig)

////////////////////////////////////////////////////////////////////////////////

class TInMemoryManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    int MaxConcurrentPreloads;
    TDuration InterceptedDataRetentionTime;
    TWorkloadDescriptor WorkloadDescriptor;

    TInMemoryManagerConfig()
    {
        RegisterParameter("max_concurrent_preloads", MaxConcurrentPreloads)
            .GreaterThan(0)
            .Default(1);
        RegisterParameter("intercepted_data_retention_time", InterceptedDataRetentionTime)
            .Default(TDuration::Seconds(30));
        RegisterParameter("workload_descriptor", WorkloadDescriptor)
            .Default(TWorkloadDescriptor(EWorkloadCategory::UserBatch));
    }
};

DEFINE_REFCOUNTED_TYPE(TInMemoryManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TPartitionBalancerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Limits the rate (measured in chunks) of location requests issued by all active chunk scrapers.
    NConcurrency::TThroughputThrottlerConfigPtr ChunkLocationThrottler;

    //! Scraps unavailable chunks.
    NChunkClient::TChunkScraperConfigPtr ChunkScraper;

    //! Fetches samples from remote chunks.
    NChunkClient::TFetcherConfigPtr SamplesFetcher;

    //! Minimum number of samples needed for partitioning.
    int MinPartitioningSampleCount;

    //! Maximum number of samples to request for partitioning.
    int MaxPartitioningSampleCount;

    //! Maximum number of concurrent partition samplings.
    int MaxConcurrentSamplings;

    //! Minimum interval between resampling.
    TDuration ResamplingPeriod;


    TPartitionBalancerConfig()
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
    }
};

DEFINE_REFCOUNTED_TYPE(TPartitionBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

class TSecurityManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TExpiringCacheConfigPtr TablePermissionCache;
    TExpiringCacheConfigPtr ResourceLimitsCache;

    TSecurityManagerConfig()
    {
        RegisterParameter("table_permission_cache", TablePermissionCache)
            .DefaultNew();
        RegisterParameter("resource_limits_cache", ResourceLimitsCache)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TSecurityManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TResourceLimitsConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Maximum number of Tablet Managers to run.
    int Slots;

    //! Maximum amount of memory static tablets (i.e. "in-memory tables") are allowed to occupy.
    i64 TabletStaticMemory;

    //! Maximum amount of memory dynamics tablets are allowed to occupy.
    i64 TabletDynamicMemory;

    TResourceLimitsConfig()
    {
        RegisterParameter("slots", Slots)
            .GreaterThanOrEqual(0)
            .Default(4);
        RegisterParameter("tablet_static_memory", TabletStaticMemory)
            .Default(std::numeric_limits<i64>::max());
        RegisterParameter("tablet_dynamic_memory", TabletDynamicMemory)
            .GreaterThanOrEqual(0)
            .Default(1_GB);
    }
};

DEFINE_REFCOUNTED_TYPE(TResourceLimitsConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletNodeConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Fraction of #MemoryLimit when tablets must be forcefully flushed.
    double ForcedRotationsMemoryRatio;

    //! Limits resources consumed by tablets.
    TResourceLimitsConfigPtr ResourceLimits;

    //! Remote snapshots.
    NHydra::TRemoteSnapshotStoreConfigPtr Snapshots;

    //! Remote changelogs.
    NHydra::TRemoteChangelogStoreConfigPtr Changelogs;

    //! Generic configuration for all Hydra instances.
    TTabletHydraManagerConfigPtr HydraManager;

    //! Generic configuration for all Hive instances.
    NHiveServer::THiveManagerConfigPtr HiveManager;

    TTransactionManagerConfigPtr TransactionManager;
    NHiveServer::TTransactionSupervisorConfigPtr TransactionSupervisor;

    TTabletManagerConfigPtr TabletManager;
    TStoreFlusherConfigPtr StoreFlusher;
    TStoreCompactorConfigPtr StoreCompactor;
    TInMemoryManagerConfigPtr InMemoryManager;
    TPartitionBalancerConfigPtr PartitionBalancer;
    TSecurityManagerConfigPtr SecurityManager;

    //! Controls outcoming bandwidth used by store flushes.
    NConcurrency::TThroughputThrottlerConfigPtr StoreFlushOutThrottler;

    //! Controls incoming bandwidth used by store compactions.
    NConcurrency::TThroughputThrottlerConfigPtr StoreCompactionInThrottler;

    //! Controls outcoming bandwidth used by store compactions.
    NConcurrency::TThroughputThrottlerConfigPtr StoreCompactionOutThrottler;

    //! Interval between slots examination.
    TDuration SlotScanPeriod;

    //! Toggles background tablet compaction and partitioning (turning off is useful for debugging purposes).
    bool EnableStoreCompactor;

    //! Toggles background Eden flushing (disabling is useful for debugging purposes).
    bool EnableStoreFlusher;

    //! Toggles background store trimming (disabling is useful for debugging purposes).
    bool EnableStoreTrimmer;

    //! Toggles background partition balancing (disabling is useful for debugging purposes).
    bool EnablePartitionBalancer;

    TTabletNodeConfig()
    {
        RegisterParameter("forced_rotations_memory_ratio", ForcedRotationsMemoryRatio)
            .InRange(0.0, 1.0)
            .Default(0.8);

        RegisterParameter("resource_limits", ResourceLimits)
            .DefaultNew();

        RegisterParameter("snapshots", Snapshots)
            .DefaultNew();
        RegisterParameter("changelogs", Changelogs)
            .DefaultNew();
        RegisterParameter("hydra_manager", HydraManager)
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

        RegisterParameter("store_flush_out_throttler", StoreFlushOutThrottler)
            .DefaultNew();

        RegisterParameter("store_compaction_in_throttler", StoreCompactionInThrottler)
            .DefaultNew();
        RegisterParameter("store_compaction_out_throttler", StoreCompactionOutThrottler)
            .DefaultNew();

        RegisterParameter("slot_scan_period", SlotScanPeriod)
            .Default(TDuration::Seconds(1));

        RegisterParameter("enable_store_compactor", EnableStoreCompactor)
            .Default(true);
        RegisterParameter("enable_store_flusher", EnableStoreFlusher)
            .Default(true);
        RegisterParameter("enable_store_trimmer", EnableStoreTrimmer)
            .Default(true);
        RegisterParameter("enable_partition_balancer", EnablePartitionBalancer)
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletNodeConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
