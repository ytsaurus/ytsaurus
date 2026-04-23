#include "config.h"

namespace NYT::NCypressElection {

////////////////////////////////////////////////////////////////////////////////

void TCypressElectionManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("lock_path", &TThis::LockPath)
        .Default();

    registrar.Parameter("transaction_timeout", &TThis::TransactionTimeout)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("transaction_ping_period", &TThis::TransactionPingPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("lock_acquisition_period", &TThis::LockAcquisitionPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("leader_cache_update_period", &TThis::LeaderCacheUpdatePeriod)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressElection
