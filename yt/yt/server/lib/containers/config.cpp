#include "config.h"

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

void TPortoExecutorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("retries_timeout", &TThis::RetriesTimeout)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("poll_period", &TThis::PollPeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("api_timeout", &TThis::ApiTimeout)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("api_disk_timeout", &TThis::ApiDiskTimeout)
        .Default(TDuration::Minutes(30));
    registrar.Parameter("enable_network_isolation", &TThis::EnableNetworkIsolation)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
