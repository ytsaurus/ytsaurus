#include "config.h"

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

TCellBalancerConfig::TCellBalancerConfig()
{
    RegisterParameter("enable_tablet_cell_smoothing", EnableTabletCellSmoothing)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

TDynamicCellarNodeTrackerConfig::TDynamicCellarNodeTrackerConfig()
{
    RegisterParameter("max_concurrent_heartbeats", MaxConcurrentHeartbeats)
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
