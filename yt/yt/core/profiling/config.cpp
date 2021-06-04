#include "config.h"

namespace NYT::NProfiling {

TProfileManagerConfig::TProfileManagerConfig() 
{
    RegisterParameter("global_tags", GlobalTags)
            .Default();
    RegisterParameter("max_keep_interval", MaxKeepInterval)
        .Default(TDuration::Minutes(5));
    RegisterParameter("deque_period", DequeuePeriod)
        .Default(TDuration::MilliSeconds(100));
    RegisterParameter("sample_rate_limit", SampleRateLimit)
        .Default(TDuration::MilliSeconds(5));
    RegisterParameter("enabled_perf_events", EnabledPerfEvents)
        .Default();
}

TProfileManagerDynamicConfig::TProfileManagerDynamicConfig() 
{
    RegisterParameter("enabled_perf_events", EnabledPerfEvents)
        .Default();
}

} // namespace NYT::NProfiling
