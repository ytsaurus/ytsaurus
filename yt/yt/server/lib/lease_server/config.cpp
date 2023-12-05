#include "config.h"

namespace NYT::NLeaseServer {

////////////////////////////////////////////////////////////////////////////////

void TLeaseManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("lease_removal_period", &TThis::LeaseRemovalPeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("max_leases_per_removal", &TThis::MaxLeasesPerRemoval)
        .Default(10'000);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLeaseServer
