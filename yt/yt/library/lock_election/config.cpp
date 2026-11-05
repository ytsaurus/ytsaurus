#include "config.h"

namespace NYT::NLockElection {

////////////////////////////////////////////////////////////////////////////////

void TLockElectionManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("lock_acquisition_period", &TThis::LockAcquisitionPeriod)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLockElection
