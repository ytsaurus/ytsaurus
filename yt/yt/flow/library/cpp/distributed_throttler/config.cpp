#include "config.h"

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

void TDistributedThrottlerServiceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("throttlers", &TThis::Throttlers)
        .Default();
    registrar.Parameter("queue_timeout", &TThis::QueueTimeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("drain_period", &TThis::DrainPeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("response_keeper", &TThis::ResponseKeeper)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
