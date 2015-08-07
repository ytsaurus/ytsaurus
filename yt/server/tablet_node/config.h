#pragma once

#include "public.h"

#include <core/misc/config.h>

#include <core/ytree/yson_serializable.h>

#include <core/compression/public.h>

#include <core/rpc/config.h>

#include <ytlib/new_table_client/config.h>

#include <ytlib/chunk_client/config.h>

#include <server/hydra/config.h>

#include <server/hive/config.h>

#include <server/data_node/config.h>

#include <server/tablet_node/config.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTabletHydraManageConfig
    : public NHydra::TDistributedHydraManagerConfig
{
public:
    NRpc::TResponseKeeperConfigPtr ResponseKeeper;

    TTabletHydraManageConfig()
    {
        RegisterParameter("response_keeper", ResponseKeeper)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletHydraManageConfig)

////////////////////////////////////////////////////////////////////////////////

class TTableMountConfig
    : public NVersionedTableClient::TRetentionConfig
{
public:
    int MaxMemoryStoreKeyCount;
    int MaxMemoryStoreValueCount;
    i64 MaxMemoryStorePoolSize;
    TDuration MemoryStoreAutoFlushPeriod;

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

    int SamplesPerPartition;

    TDuration BackingStoreRetentionTime;

    int MaxReadFanIn;

    EInMemoryMode InMemoryMode;

    int MaxStoresPerTablet;

    TNullable<ui64> ForcedCompactionRevision;

    TTableMountConfig()
    {
        RegisterParameter("max_memory_store_key_count", MaxMemoryStoreKeyCount)
            .GreaterThan(0)
            .Default(1000000);
        RegisterParameter("max_memory_store_value_count", MaxMemoryStoreValueCount)
            .GreaterThan(0)
            .Default(10000000)
            // NB: This limit is really important; please consult babenko@
            // before changing it.
            .LessThanOrEqual(SoftRevisionsPerDynamicMemoryStoreLimit);
        RegisterParameter("max_memory_store_pool_size", MaxMemoryStorePoolSize)
            .GreaterThan(0)
            .Default((i64) 1024 * 1024 * 1024);
        RegisterParameter("memory_store_auto_flush_period", MemoryStoreAutoFlushPeriod)
            .Default(TDuration::Hours(1));

        RegisterParameter("max_partition_data_size", MaxPartitionDataSize)
            .Default((i64) 320 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("desired_partition_data_size", DesiredPartitionDataSize)
            .Default((i64) 256 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("min_partition_data_size", MinPartitionDataSize)
            .Default((i64) 96 * 1024 * 1024)
            .GreaterThan(0);

        RegisterParameter("max_partition_count", MaxPartitionCount)
            .Default(10240)
            .GreaterThan(0);

        RegisterParameter("min_partitioning_data_size", MinPartitioningDataSize)
            .Default((i64) 64 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("min_partitioning_store_count", MinPartitioningStoreCount)
            .Default(1)
            .GreaterThan(0);
        RegisterParameter("max_partitioning_data_size", MaxPartitioningDataSize)
            .Default((i64) 1024 * 1024 * 1024)
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
            .Default((i64) 16 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("compaction_data_size_ratio", CompactionDataSizeRatio)
            .Default(2.0)
            .GreaterThan(1.0);

        RegisterParameter("samples_per_partition", SamplesPerPartition)
            .Default(100)
            .GreaterThanOrEqual(0);

        RegisterParameter("backing_store_retention_time", BackingStoreRetentionTime)
            .Default(TDuration::Seconds(60));

        RegisterParameter("max_read_fan_in", MaxReadFanIn)
            .GreaterThan(0)
            .Default(30);

        RegisterParameter("in_memory_mode", InMemoryMode)
            .Default(EInMemoryMode::None);

        RegisterParameter("max_stores_per_tablet", MaxStoresPerTablet)
            .Default(10000)
            .GreaterThan(0);

        RegisterParameter("forced_compaction_revision", ForcedCompactionRevision)
            .Default(Null);

        RegisterValidator([&] () {
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
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TTableMountConfig)

///////////////////////////////////////////////////////////////////////////////

class TTransactionManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MaxTransactionTimeout;
    TDuration MaxTransactionDuration;

    TTransactionManagerConfig()
    {
        RegisterParameter("max_transaction_timeout", MaxTransactionTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(60));
        RegisterParameter("max_transaction_duration", MaxTransactionDuration)
            .Default(TDuration::Seconds(60));
    }
};

DEFINE_REFCOUNTED_TYPE(TTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    i64 PoolChunkSize;
    double MaxPoolSmallBlockRatio;

    TDuration ErrorBackoffTime;

    TDuration MaxBlockedRowWaitTime;

    NCompression::ECodec ChangelogCodec;

    //! When committing a non-atomic transaction, clients provide timestamps based
    //! on wall clock readings. These timestamps are checked for sanity using the server-side
    //! timestamp estimates.
    TDuration ClientTimestampThreshold;


    TTabletManagerConfig()
    {
        RegisterParameter("pool_chunk_size", PoolChunkSize)
            .GreaterThan(0)
            .Default(64 * 1024);
        RegisterParameter("max_pool_small_block_ratio", MaxPoolSmallBlockRatio)
            .InRange(0.0, 1.0)
            .Default(0.25);

        RegisterParameter("max_blocked_row_wait_time", MaxBlockedRowWaitTime)
            .Default(TDuration::Seconds(5));

        RegisterParameter("error_backoff_time", ErrorBackoffTime)
            .Default(TDuration::Minutes(1));

        RegisterParameter("changelog_codec", ChangelogCodec)
            .Default(NCompression::ECodec::Lz4);

        RegisterParameter("client_timestamp_threshold", ClientTimestampThreshold)
            .Default(TDuration::Minutes(1));
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
            .Default((i64) 1024 * 1024);
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

    TInMemoryManagerConfig()
    {
        RegisterParameter("max_concurrent_preloads", MaxConcurrentPreloads)
            .GreaterThan(0)
            .Default(1);
        RegisterParameter("intercepted_data_retention_time", InterceptedDataRetentionTime)
            .Default(TDuration::Seconds(30));
    }
};

DEFINE_REFCOUNTED_TYPE(TInMemoryManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TPartitionBalancerConfig
    : public NYTree::TYsonSerializable
{
public:
    NChunkClient::TFetcherConfigPtr SamplesFetcher;

    //! Minimum number of samples needed for partitioning.
    int MinPartitioningSampleCount;

    //! Maximum number of samples to request for partitioning.
    int MaxPartitioningSampleCount;

    //! Maximum number of concurrent partition samplings.
    int MaxConcurrentSamplings;

    //! Mininmum intervals between resampling.
    TDuration ResamplingPeriod;

    TPartitionBalancerConfig()
    {
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

class TTabletChunkReaderConfig
    : public NVersionedTableClient::TChunkReaderConfig
    , public NChunkClient::TReplicationReaderConfig
{ };

DEFINE_REFCOUNTED_TYPE(TTabletChunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TSecurityManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TExpiringCacheConfigPtr TablePermissionCache;

    TSecurityManagerConfig()
    {
        RegisterParameter("table_permission_cache", TablePermissionCache)
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
            .Default((i64) 1024 * 1024 * 1024);
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
    TTabletHydraManageConfigPtr HydraManager;

    //! Generic configuration for all Hive instances.
    NHive::THiveManagerConfigPtr HiveManager;

    TTransactionManagerConfigPtr TransactionManager;
    NHive::TTransactionSupervisorConfigPtr TransactionSupervisor;

    TTabletManagerConfigPtr TabletManager;
    TStoreFlusherConfigPtr StoreFlusher;
    TStoreCompactorConfigPtr StoreCompactor;
    TInMemoryManagerConfigPtr InMemoryManager;
    TPartitionBalancerConfigPtr PartitionBalancer;
    TSecurityManagerConfigPtr SecurityManager;

    TTabletChunkReaderConfigPtr ChunkReader;
    NVersionedTableClient::TTableWriterConfigPtr ChunkWriter;

    //! Controls outcoming bandwidth used by store flushes.
    NConcurrency::TThroughputThrottlerConfigPtr StoreFlushOutThrottler;


    //! Controls incoming bandwidth used by store compactions.
    NConcurrency::TThroughputThrottlerConfigPtr StoreCompactionInThrottler;

    //! Controls outcoming bandwidth used by store compactions.
    NConcurrency::TThroughputThrottlerConfigPtr StoreCompactionOutThrottler;


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

        RegisterParameter("chunk_reader", ChunkReader)
            .DefaultNew();
        RegisterParameter("chunk_writer", ChunkWriter)
            .DefaultNew();

        RegisterParameter("store_flush_out_throttler", StoreFlushOutThrottler)
            .DefaultNew();

        RegisterParameter("store_compaction_in_throttler", StoreCompactionInThrottler)
            .DefaultNew();
        RegisterParameter("store_compaction_out_throttler", StoreCompactionOutThrottler)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletNodeConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
