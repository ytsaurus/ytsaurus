#include "config.h"

namespace NYT::NProfiling {

void TProfileManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("global_tags", &TThis::GlobalTags)
            .Default();
    registrar.Parameter("max_keep_interval", &TThis::MaxKeepInterval)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("deque_period", &TThis::DequeuePeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("sample_rate_limit", &TThis::SampleRateLimit)
        .Default(TDuration::MilliSeconds(5));
}

} // namespace NYT::NProfiling
