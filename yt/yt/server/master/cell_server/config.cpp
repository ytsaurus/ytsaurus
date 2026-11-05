#include "config.h"

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

void TCellBalancerBootstrapConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_tablet_cell_smoothing", &TThis::EnableTabletCellSmoothing)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicCellarNodeTrackerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_concurrent_heartbeats", &TThis::MaxConcurrentHeartbeats)
        .Default(10)
        .GreaterThan(0);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicCellManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cellar_node_tracker", &TThis::CellarNodeTracker)
        .DefaultNew();
    registrar.Parameter("cell_health_history_max_size", &TThis::CellHealthHistoryMaxSize)
        .Default(100)
        .GreaterThanOrEqual(0);
    registrar.Parameter("cell_health_history_expiration_time", &TThis::CellHealthHistoryExpirationTime)
        .Default(TDuration::Days(7));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
