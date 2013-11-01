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
    TTransactionManagerConfig()
    {
    }
};

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
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
