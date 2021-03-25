#pragma once

#include "public.h"

#include <yt/yt/server/lib/hive/config.h>

#include <yt/yt/server/lib/election/config.h>

#include <yt/yt/server/lib/hydra/config.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

class TCellarConfig
    : public NYTree::TYsonSerializable
{
public:
    int Size;
    TCellarOccupantConfigPtr Occupant;

    TCellarConfig()
    {
        RegisterParameter("size", Size)
            .GreaterThanOrEqual(0);
        RegisterParameter("occupant", Occupant)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TCellarConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellarManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    THashMap<NCellarClient::ECellarType, TCellarConfigPtr> Cellars;

    TCellarManagerConfig()
    {
        RegisterParameter("cellars", Cellars)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TCellarManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellarDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<int> Size;

    TCellarDynamicConfig()
    {
        RegisterParameter("size", Size)
            .GreaterThanOrEqual(0)
            .Optional();
    }
};

DEFINE_REFCOUNTED_TYPE(TCellarDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellarManagerDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    THashMap<NCellarClient::ECellarType, TCellarDynamicConfigPtr> Cellars;

    TCellarManagerDynamicConfig()
    {
        RegisterParameter("cellars", Cellars)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TCellarManagerDynamicConfig)

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
