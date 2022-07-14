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

TPrefetchingThrottlerConfig::TPrefetchingThrottlerConfig()
{
    RegisterParameter("target_rps", TargetRps)
        .Default(1.0)
        .GreaterThan(1e-3);
    RegisterParameter("min_prefetch_amount", MinPrefetchAmount)
        .Default(1)
        .GreaterThanOrEqual(1);
    RegisterParameter("max_prefetch_amount", MaxPrefetchAmount)
        .Default(10)
        .GreaterThanOrEqual(1);
    RegisterParameter("window", Window)
        .GreaterThan(TDuration::MilliSeconds(1))
        .Default(TDuration::Seconds(1));

    RegisterPostprocessor([=] {
        if (MinPrefetchAmount > MaxPrefetchAmount) {
            THROW_ERROR_EXCEPTION("\"min_prefetch_amount\" should be less than or equal \"max_prefetch_amount\"")
                << TErrorAttribute("min_prefetch_amount", MinPrefetchAmount)
                << TErrorAttribute("max_prefetch_amount", MaxPrefetchAmount);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
