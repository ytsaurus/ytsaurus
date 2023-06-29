#include "config.h"

#include <yt/yt/server/lib/hive/config.h>

#include <yt/yt/server/lib/election/config.h>

#include <yt/yt/server/lib/hydra_common/config.h>

#include <yt/yt/server/lib/transaction_supervisor/config.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

void TCellarConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("size", &TThis::Size)
        .GreaterThanOrEqual(0);
    registrar.Parameter("occupant", &TThis::Occupant)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TCellarManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cellars", &TThis::Cellars)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TCellarDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("size", &TThis::Size)
        .GreaterThanOrEqual(0)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TCellarManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cellars", &TThis::Cellars)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TCellarOccupantConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("snapshots", &TThis::Snapshots)
        .DefaultCtor([] { return New<NHydra::TRemoteSnapshotStoreConfig>(); });
    registrar.Parameter("changelogs", &TThis::Changelogs)
        .DefaultNew();
    registrar.Parameter("hydra_manager", &TThis::HydraManager)
        .DefaultNew();
    registrar.Parameter("election_manager", &TThis::ElectionManager)
        .DefaultNew();
    registrar.Parameter("hive_manager", &TThis::HiveManager)
        .DefaultNew();
    registrar.Parameter("transaction_supervisor", &TThis::TransactionSupervisor)
        .DefaultNew();
    registrar.Parameter("response_keeper", &TThis::ResponseKeeper)
        .DefaultNew();
    registrar.Parameter("use_new_hydra", &TThis::UseNewHydra)
        .Default(true);
    registrar.Parameter("enable_dry_run", &TThis::EnableDryRun)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
