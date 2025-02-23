#pragma once

#include "public.h"

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/server/lib/election/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/server/lib/lease_server/public.h>

#include <yt/yt/server/lib/transaction_supervisor/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

struct TCellarConfig
    : public NYTree::TYsonStruct
{
    int Size;
    TCellarOccupantConfigPtr Occupant;

    REGISTER_YSON_STRUCT(TCellarConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellarConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCellarManagerConfig
    : public NYTree::TYsonStruct
{
    THashMap<NCellarClient::ECellarType, TCellarConfigPtr> Cellars;

    REGISTER_YSON_STRUCT(TCellarManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellarManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCellarDynamicConfig
    : public NYTree::TYsonStruct
{
    std::optional<int> Size;

    NHydra::TDynamicDistributedHydraManagerConfigPtr HydraManager;

    REGISTER_YSON_STRUCT(TCellarDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellarDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCellarManagerDynamicConfig
    : public NYTree::TYsonStruct
{
    THashMap<NCellarClient::ECellarType, TCellarDynamicConfigPtr> Cellars;

    REGISTER_YSON_STRUCT(TCellarManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellarManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCellarOccupantConfig
    : public NYTree::TYsonStruct
{
    // Remote store by default, but can be configured as a local one for dry run.
    NHydra::TSnapshotStoreConfigBasePtr Snapshots;

    NHydra::TRemoteChangelogStoreConfigPtr Changelogs;

    NHydra::TDistributedHydraManagerConfigPtr HydraManager;

    NElection::TDistributedElectionManagerConfigPtr ElectionManager;

    NHiveServer::THiveManagerConfigPtr HiveManager;

    NLeaseServer::TLeaseManagerConfigPtr LeaseManager;

    NTransactionSupervisor::TTransactionSupervisorConfigPtr TransactionSupervisor;

    NRpc::TResponseKeeperConfigPtr ResponseKeeper;

    bool EnableDryRun;

    REGISTER_YSON_STRUCT(TCellarOccupantConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellarOccupantConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
