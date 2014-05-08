#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <core/compression/public.h>

#include <ytlib/new_table_client/config.h>

#include <ytlib/chunk_client/config.h>

#include <server/hydra/config.h>

#include <server/hive/config.h>

#include <server/data_node/config.h>

#include <server/tablet_node/config.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTableMountConfig
    : public TYsonSerializable
{
public:
    int MaxMemoryStoreKeyCount;
    int MaxMemoryStoreValueCount;
    i64 MaxMemoryStoreAlignedPoolSize;
    i64 MaxMemoryStoreUnalignedPoolSize;

    i64 MaxPartitionDataSize;
    i64 DesiredPartitionDataSize;
    i64 MinPartitionDataSize;
    int MaxPartitionChunkCount;

    int MaxPartitionCount;

    i64 MaxEdenDataSize;
    int MaxEdenChunkCount;

    int SamplesPerPartition;

    TTableMountConfig()
    {
        RegisterParameter("max_memory_store_key_count", MaxMemoryStoreKeyCount)
            .GreaterThan(0)
            .Default(1000000);
        RegisterParameter("max_memory_store_value_count", MaxMemoryStoreValueCount)
            .GreaterThan(0)
            .Default(10000000);
        RegisterParameter("max_memory_store_aligned_pool_size", MaxMemoryStoreAlignedPoolSize)
            .GreaterThan(0)
            .Default((i64) 256 * 1024 * 1024);
        RegisterParameter("max_memory_store_unaligned_pool_size", MaxMemoryStoreUnalignedPoolSize)
            .GreaterThan(0)
            .Default((i64) 256 * 1024 * 1024);

        RegisterParameter("max_partition_data_size", MaxPartitionDataSize)
            .Default((i64) 256 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("desired_partition_data_size", DesiredPartitionDataSize)
            .Default((i64) 192 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("min_partition_data_size", MinPartitionDataSize)
            .Default((i64) 16 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("max_partition_chunk_count", MaxPartitionChunkCount)
            .Default(3)
            .GreaterThan(0);

        RegisterParameter("max_partition_count", MaxPartitionCount)
            .Default(64)
            .GreaterThan(0);

        RegisterParameter("max_eden_data_size", MaxEdenDataSize)
            .Default((i64) 256 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("max_eden_chunk_count", MaxEdenChunkCount)
            .Default(8)
            .GreaterThan(0);

        RegisterParameter("samples_per_partition", SamplesPerPartition)
            .Default(1)
            .GreaterThanOrEqual(1);

        RegisterValidator([&] () {
            if (MinPartitionDataSize >= DesiredPartitionDataSize) {
                THROW_ERROR_EXCEPTION("\"min_partition_data_size\" must be less than \"desired_partition_data_size\"");
            }
            if (DesiredPartitionDataSize >= MaxPartitionDataSize) {
                THROW_ERROR_EXCEPTION("\"desired_partition_data_size\" must be less than \"max_partition_data_size\"");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TTableMountConfig)

///////////////////////////////////////////////////////////////////////////////

class TTransactionManagerConfig
    : public TYsonSerializable
{
public:
    TDuration DefaultTransactionTimeout;
    TDuration MaxTransactionTimeout;

    TTransactionManagerConfig()
    {
        RegisterParameter("default_transaction_timeout", DefaultTransactionTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(15));
        RegisterParameter("max_transaction_timeout", MaxTransactionTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Minutes(60));
    }
};

DEFINE_REFCOUNTED_TYPE(TTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletManagerConfig
    : public TYsonSerializable
{
public:
    i64 AlignedPoolChunkSize;
    i64 UnalignedPoolChunkSize;
    double MaxPoolSmallBlockRatio;

    TDuration ErrorBackoffTime;

    TDuration AutoFlushPeriod;

    NCompression::ECodec ChangelogCodec;

    TTabletManagerConfig()
    {
        RegisterParameter("aligned_pool_chunk_size", AlignedPoolChunkSize)
            .GreaterThan(0)
            .Default(64 * 1024);
        RegisterParameter("unaligned_pool_chunk_size", UnalignedPoolChunkSize)
            .GreaterThan(0)
            .Default(64 * 1024);
        RegisterParameter("max_pool_small_block_ratio", MaxPoolSmallBlockRatio)
            .InRange(0.0, 1.0)
            .Default(0.25);

        RegisterParameter("error_backoff_time", ErrorBackoffTime)
            .Default(TDuration::Minutes(1));

        RegisterParameter("auto_flush_period", AutoFlushPeriod)
            .Default(TDuration::Minutes(5));

        RegisterParameter("changelog_codec", ChangelogCodec)
            .Default(NCompression::ECodec::Lz4);
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TStoreFlusherConfig
    : public TYsonSerializable
{
public:
    int ThreadPoolSize;
    int MaxConcurrentFlushes;

    NVersionedTableClient::TTableWriterConfigPtr Writer;

    TStoreFlusherConfig()
    {
        RegisterParameter("thread_pool_size", ThreadPoolSize)
            .GreaterThan(0)
            .Default(1);
        RegisterParameter("max_concurrent_flushes", MaxConcurrentFlushes)
            .GreaterThan(0)
            .Default(1);

        RegisterParameter("writer", Writer)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TStoreFlusherConfig)

class TStoreCompactorConfig
    : public TYsonSerializable
{
public:
    int ThreadPoolSize;
    int MaxConcurrentCompactions;
    int MaxChunksPerCompaction;

    NVersionedTableClient::TTableWriterConfigPtr Writer;

    TStoreCompactorConfig()
    {
        RegisterParameter("thread_pool_size", ThreadPoolSize)
            .GreaterThan(0)
            .Default(1);
        RegisterParameter("max_concurrent_compactions", MaxConcurrentCompactions)
            .GreaterThan(0)
            .Default(1);
        RegisterParameter("max_chunks_per_compaction", MaxChunksPerCompaction)
            .GreaterThan(0)
            .Default(10);

        RegisterParameter("writer", Writer)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TStoreCompactorConfig)

class TPartitionBalancerConfig
    : public TYsonSerializable
{
public:
    NChunkClient::TFetcherConfigPtr SamplesFetcher;

    //! Minimum number of samples needed for partitioning.
    int MinPartitioningSampleCount;

    //! Maximum number of samples to request for partitioning.
    int MaxPartitioningSampleCount;

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
        RegisterParameter("resampling_period", ResamplingPeriod)
            .Default(TDuration::Minutes(1));
    }
};

DEFINE_REFCOUNTED_TYPE(TPartitionBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletNodeConfig
    : public TYsonSerializable
{
public:
    //! Maximum number of tablet managers to run.
    int Slots;

    //! Maximum amount of memory tablets are allowed to occupy.
    i64 MemoryLimit;

    //! Fraction of #MemoryLimit when tablets must be forcefully flushed.
    double ForcedRotationsMemoryRatio;

    //! Changelog catalog.
    NHydra::TFileChangelogCatalogConfigPtr Changelogs;

    //! Remote snapshots.
    NHydra::TRemoteSnapshotStoreConfigPtr Snapshots;

    //! Generic configuration for all Hydra instances.
    NHydra::TDistributedHydraManagerConfigPtr HydraManager;

    //! Generic configuration for all Hive instances.
    NHive::THiveManagerConfigPtr HiveManager;

    TTransactionManagerConfigPtr TransactionManager;
    NHive::TTransactionSupervisorConfigPtr TransactionSupervisor;

    TTabletManagerConfigPtr TabletManager;
    TStoreFlusherConfigPtr StoreFlusher;
    TStoreCompactorConfigPtr StoreCompactor;
    TPartitionBalancerConfigPtr PartitionBalancer;

    TTabletNodeConfig()
    {
        RegisterParameter("slots", Slots)
            .GreaterThanOrEqual(0)
            .Default(4);

        RegisterParameter("memory_limit", MemoryLimit)
            .GreaterThanOrEqual(0)
            .Default((i64) 1024 * 1024 * 1024);
        RegisterParameter("forced_rotations_memory_ratio", ForcedRotationsMemoryRatio)
            .InRange(0.0, 1.0)
            .Default(0.8);

        RegisterParameter("changelogs", Changelogs);
        RegisterParameter("snapshots", Snapshots);

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
        RegisterParameter("partition_balancer", PartitionBalancer)
            .DefaultNew();

        RegisterInitializer([&] () {
            // Tablet snapshots are stored in Cypress.
            // Must not build multiple copies simultaneously.
            HydraManager->BuildSnapshotsAtFollowers = false;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletNodeConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
