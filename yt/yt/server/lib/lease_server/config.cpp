#include "config.h"

namespace NYT::NLeaseServer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TLeaseManagerConfig::ApplyDynamicInplace(const TLeaseManagerDynamicConfig& dynamicConfig)
{
    UpdateYsonStructField(LeaseRemovalPeriod, dynamicConfig.LeaseRemovalPeriod);
    UpdateYsonStructField(MaxLeasesPerRemoval, dynamicConfig.MaxLeasesPerRemoval);
}

////////////////////////////////////////////////////////////////////////////////

void TLeaseManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("lease_removal_period", &TThis::LeaseRemovalPeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("max_leases_per_removal", &TThis::MaxLeasesPerRemoval)
        .GreaterThan(0)
        .Default(10'000);
}

////////////////////////////////////////////////////////////////////////////////

void TLeaseManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("lease_removal_period", &TThis::LeaseRemovalPeriod)
        .Default();

    registrar.Parameter("max_leases_per_removal", &TThis::MaxLeasesPerRemoval)
        .GreaterThan(0)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLeaseServer
