#include "config.h"

namespace NYT::NChaosElection {

////////////////////////////////////////////////////////////////////////////////

void TChaosElectionManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("lock_table_path", &TThis::LockTablePath)
        .Default();
    registrar.Parameter("chaos_cell_bundle", &TThis::ChaosCellBundle)
        .Default();

    registrar.Parameter("lease_timeout", &TThis::LeaseTimeout)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("lease_ping_period", &TThis::LeasePingPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("lock_acquisition_period", &TThis::LockAcquisitionPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("leader_cache_update_period", &TThis::LeaderCacheUpdatePeriod)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosElection
