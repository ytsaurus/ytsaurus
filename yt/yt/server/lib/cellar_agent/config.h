#pragma once

#include "public.h"

#include <yt/server/lib/hive/config.h>

#include <yt/server/lib/election/config.h>

#include <yt/server/lib/hydra/config.h>

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

class TCellarManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    std::vector<TCellarConfigPtr> Cellars;

    TCellarManagerConfig()
    {
        RegisterParameter("cellars", Cellars)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TCellarManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellarConfig
    : public NYTree::TYsonSerializable
{
public:
    ECellarType Type;
    int Size;
    TCellarOccupantConfigPtr Occupant;

    TCellarConfig()
    {
        RegisterParameter("type", Type);
        RegisterParameter("size", Size)
            .GreaterThanOrEqual(0);
        RegisterParameter("occupant", Occupant)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TCellarConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicCellarManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    std::vector<TDynamicCellarConfigPtr> Cellars;

    TDynamicCellarManagerConfig()
    {
        RegisterParameter("cellars", Cellars)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicCellarManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicCellarConfig
    : public NYTree::TYsonSerializable
{
public:
    ECellarType Type;
    std::optional<int> Size;

    TDynamicCellarConfig()
    {
        RegisterParameter("type", Type);
        RegisterParameter("size", Size)
            .GreaterThanOrEqual(0)
            .Optional();
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicCellarConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellarOccupantConfig
    : public NYTree::TYsonSerializable
{
public:
    NHydra::TRemoteSnapshotStoreConfigPtr Snapshots;

    NHydra::TRemoteChangelogStoreConfigPtr Changelogs;

    NHydra::TDistributedHydraManagerConfigPtr HydraManager;

    NElection::TDistributedElectionManagerConfigPtr ElectionManager;

    NHiveServer::THiveManagerConfigPtr HiveManager;

    NHiveServer::TTransactionSupervisorConfigPtr TransactionSupervisor;

    NRpc::TResponseKeeperConfigPtr ResponseKeeper;

    TCellarOccupantConfig()
    {
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
        RegisterParameter("transaction_supervisor", TransactionSupervisor)
            .DefaultNew();
        RegisterParameter("response_keeper", ResponseKeeper)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TCellarOccupantConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
