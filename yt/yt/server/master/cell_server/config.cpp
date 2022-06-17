#include "config.h"

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

void TCellBalancerConfig::Register(TRegistrar registrar)
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
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
