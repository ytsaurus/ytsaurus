#include "config.h"

namespace NYT::NCypressElection {

////////////////////////////////////////////////////////////////////////////////

TCypressElectionManagerConfig::TCypressElectionManagerConfig()
{
    RegisterParameter("lock_path", LockPath)
        .Default();

    RegisterParameter("transaction_timeout", TransactionTimeout)
        .Default(TDuration::Minutes(1));
    RegisterParameter("transaction_ping_period", TransactionPingPeriod)
        .Default(TDuration::Seconds(15));
    RegisterParameter("lock_acquisition_period", LockAcquisitionPeriod)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressElection
