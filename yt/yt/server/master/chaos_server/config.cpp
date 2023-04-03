#include "config.h"

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

void TChaosPeerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("alien_cluster", &TThis::AlienCluster)
        .Optional();
}

void TChaosHydraConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("peers", &TThis::Peers);
}

////////////////////////////////////////////////////////////////////////////////

void TAlienCellSynchronizerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);
    registrar.Parameter("sync_period", &TThis::SyncPeriod)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("full_sync_period", &TThis::FullSyncPeriod)
        .Default(TDuration::Minutes(5));
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicChaosManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("alien_cell_synchronizer", &TThis::AlienCellSynchronizer)
        .DefaultNew();
    registrar.Parameter("enable_metadata_cells", &TThis::EnableMetadataCells)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
