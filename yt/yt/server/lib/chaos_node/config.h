#pragma once

#include "public.h"

#include <yt/yt/server/lib/cellar_agent/config.h>

#include <yt/yt/core/misc/config.h>

#include <yt/yt/core/concurrency/config.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

class TChaosCellSynchronizerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration SyncPeriod;

    TChaosCellSynchronizerConfig()
    {
        RegisterParameter("sync_period", SyncPeriod)
            .Default(TDuration::Seconds(30));
    }
};

DEFINE_REFCOUNTED_TYPE(TChaosCellSynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

class TChaosManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TChaosCellSynchronizerConfigPtr ChaosCellSynchronizer;
    TDuration EraCommencingPeriod;

    TChaosManagerConfig()
    {
        RegisterParameter("chaos_cell_synchronizer", ChaosCellSynchronizer)
            .DefaultNew();
        RegisterParameter("era_commencing_period", EraCommencingPeriod)
            .Default(TDuration::Seconds(15));
    }
};

DEFINE_REFCOUNTED_TYPE(TChaosManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TCoordinatorManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TCoordinatorManagerConfig()
    { }
};

DEFINE_REFCOUNTED_TYPE(TCoordinatorManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTransactionManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MaxTransactionTimeout;
    int MaxAbortedTransactionPoolSize;

    TTransactionManagerConfig()
    {
        RegisterParameter("max_transaction_timeout", MaxTransactionTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(60));
        RegisterParameter("max_aborted_transaction_pool_size", MaxAbortedTransactionPoolSize)
            .Default(1000);
    }
};

DEFINE_REFCOUNTED_TYPE(TTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TChaosNodeConfig
    : public NYTree::TYsonSerializable
{
public:
    NCellarAgent::TCellarOccupantConfigPtr CellarOccupant;

    TTransactionManagerConfigPtr TransactionManager;

    TChaosManagerConfigPtr ChaosManager;

    TCoordinatorManagerConfigPtr CoordinatorManager;

    TDuration SlotScanPeriod;

    int SnapshotStoreReadPoolSize;

    TChaosNodeConfig()
    {
        RegisterParameter("cellar_occupant", CellarOccupant)
            .DefaultNew();
        RegisterParameter("transaction_manager", TransactionManager)
            .DefaultNew();
        RegisterParameter("chaos_manager", ChaosManager)
            .DefaultNew();
        RegisterParameter("coordinator_manager", CoordinatorManager)
            .DefaultNew();
        RegisterParameter("slot_scan_period", SlotScanPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("snapshot_store_read_pool_size", SnapshotStoreReadPoolSize)
            .Default(8);
    }
};

DEFINE_REFCOUNTED_TYPE(TChaosNodeConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
