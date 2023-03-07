#include "config.h"

#include <yt/client/object_client/helpers.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NClusterClock {

using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TClusterClockConfig::TClusterClockConfig()
{
    RegisterParameter("clock_cell", ClockCell)
        .Default();
    RegisterParameter("election_manager", ElectionManager)
        .DefaultNew();
    RegisterParameter("changelogs", Changelogs);
    RegisterParameter("snapshots", Snapshots);
    RegisterParameter("hydra_manager", HydraManager)
        .DefaultNew();
    RegisterParameter("timestamp_manager", TimestampManager)
        .DefaultNew();
    RegisterParameter("bus_client", BusClient)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock
