#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

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

////////////////////////////////////////////////////////////////////////////////

class TTabletManagerConfig
    : public TYsonSerializable
{
public:
    size_t AlignedPoolChunkSize;
    size_t UnalignedPoolChunkSize;
    double MaxPoolSmallBlockRatio;

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
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTabletNodeConfig
    : public TYsonSerializable
{
public:
    //! Maximum number of tablet managers to run.
    int Slots;

    //! Changelog catalog.
    NHydra::TFileChangelogCatalogConfigPtr Changelogs;

    //! Snapshot catalog.
    NHydra::TFileSnapshotCatalogConfigPtr Snapshots;

    //! Generic configuration for all Hydra instances.
    NHydra::TDistributedHydraManagerConfigPtr Hydra;

    //! Generic configuration for all Hive instances.
    NHive::THiveManagerConfigPtr Hive;

    TTransactionManagerConfigPtr TransactionManager;
    NHive::TTransactionSupervisorConfigPtr TransactionSupervisor;

    TTabletManagerConfigPtr TabletManager;

    TTabletNodeConfig()
    {
        RegisterParameter("slots", Slots)
            .GreaterThanOrEqual(0)
            .Default(4);
        RegisterParameter("changelogs", Changelogs);
        RegisterParameter("snapshots", Snapshots);
        RegisterParameter("hydra", Hydra)
            .DefaultNew();
        RegisterParameter("hive", Hive)
            .DefaultNew();
        RegisterParameter("transaction_manager", TransactionManager)
            .DefaultNew();
        RegisterParameter("transaction_supervisor", TransactionSupervisor)
            .DefaultNew();
        RegisterParameter("tablet_manager", TabletManager)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
