#include "config.h"

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

TIOTrackerConfig::TIOTrackerConfig()
{
    RegisterParameter("enable", Enable)
        .Default(false);
    RegisterParameter("enable_raw", EnableRaw)
        .Default(false);
    RegisterParameter("queue_size_limit", QueueSizeLimit)
        .Default(10'000);
    RegisterParameter("aggregation_size_limit", AggregationSizeLimit)
        .Default(1'000'000);
    RegisterParameter("aggregation_period", AggregationPeriod)
        .Default(TDuration::Minutes(15));
    RegisterParameter("period_quant", PeriodQuant)
        .Default(TDuration::MilliSeconds(50));
    RegisterParameter("enable_event_dequeue", EnableEventDequeue)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
