#include "config.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NClusterClock {

using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TClusterClockConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("clock_cell", &TThis::ClockCell)
        .Default();
    registrar.Parameter("election_manager", &TThis::ElectionManager)
        .DefaultNew();
    registrar.Parameter("changelogs", &TThis::Changelogs);
    registrar.Parameter("snapshots", &TThis::Snapshots);
    registrar.Parameter("hydra_manager", &TThis::HydraManager)
        .DefaultNew();
    registrar.Parameter("timestamp_manager", &TThis::TimestampManager)
        .DefaultNew();
    registrar.Parameter("bus_client", &TThis::BusClient)
        .DefaultNew();
    registrar.Parameter("use_new_hydra", &TThis::UseNewHydra)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TClockHydraManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("response_keeper", &TThis::ResponseKeeper)
        .DefaultNew();
}

} // namespace NYT::NClusterClock
