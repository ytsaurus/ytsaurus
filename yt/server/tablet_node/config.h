#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <ytlib/new_table_client/config.h>

#include <ytlib/chunk_client/config.h>

#include <server/hydra/config.h>

#include <server/hive/config.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

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

class TTabletChunkReaderConfig
    : public NVersionedTableClient::TChunkReaderConfig
    , public NChunkClient::TReplicationReaderConfig
{ };

class TTabletManagerConfig
    : public TYsonSerializable
{
public:
    i64 AlignedPoolChunkSize;
    i64 UnalignedPoolChunkSize;
    double MaxPoolSmallBlockRatio;

    int KeyCountRotationThreshold;
    int ValueCountRotationThreshold;
    i64 AlignedPoolSizeRotationThreshold;
    i64 UnalignedPoolSizeRotationThreshold;

    TDuration ErrorBackoffTime;

    TIntrusivePtr<TTabletChunkReaderConfig> ChunkReader;

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

        RegisterParameter("key_count_rotation_threshold", KeyCountRotationThreshold)
            .GreaterThan(0)
            .Default(1000000);
        RegisterParameter("value_count_rotation_threshold", ValueCountRotationThreshold)
            .GreaterThan(0)
            .Default(10000000);
        RegisterParameter("aligned_pool_size_rotation_threshold", AlignedPoolSizeRotationThreshold)
            .GreaterThan(0)
            .Default((i64) 256 * 1024 * 1024);
        RegisterParameter("unaligned_pool_size_rotation_threshold", UnalignedPoolSizeRotationThreshold)
            .GreaterThan(0)
            .Default((i64) 256 * 1024 * 1024);

        RegisterParameter("error_backoff_time", ErrorBackoffTime)
            .Default(TDuration::Minutes(1));

        RegisterParameter("chunk_reader", ChunkReader)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TStoreWriterConfig
    : public NVersionedTableClient::TChunkWriterConfig
    , public NChunkClient::TMultiChunkWriterConfig
{ };

class TStoreFlusherConfig
    : public TYsonSerializable
{
public:
    int ThreadPoolSize;

    TIntrusivePtr<TStoreWriterConfig> Writer;

    TStoreFlusherConfig()
    {
        RegisterParameter("thread_pool_size", ThreadPoolSize)
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

    TIntrusivePtr<TStoreWriterConfig> Writer;

    TStoreCompactorConfig()
    {
        RegisterParameter("thread_pool_size", ThreadPoolSize)
            .GreaterThan(0)
            .Default(1);

        RegisterParameter("writer", Writer)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TStoreCompactorConfig)

class TPartitionBalancerConfig
    : public TYsonSerializable
{
public:
    TPartitionBalancerConfig()
    { }
};

DEFINE_REFCOUNTED_TYPE(TPartitionBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletNodeConfig
    : public TYsonSerializable
{
public:
    //! Maximum number of tablet managers to run.
    int Slots;

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
