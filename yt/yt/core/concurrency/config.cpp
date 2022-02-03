#include "config.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TThroughputThrottlerConfig::TThroughputThrottlerConfig(
    std::optional<double> limit,
    TDuration period)
{
    RegisterParameter("limit", Limit)
        .Default(limit)
        .GreaterThanOrEqual(0);
    RegisterParameter("period", Period)
        .Default(period);
}

////////////////////////////////////////////////////////////////////////////////

TRelativeThroughputThrottlerConfig::TRelativeThroughputThrottlerConfig(
    std::optional<double> limit,
    TDuration period)
    : TThroughputThrottlerConfig(limit, period)
{
    RegisterParameter("relative_limit", RelativeLimit)
        .InRange(0.0, 1.0)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
