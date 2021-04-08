#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TThroughputThrottlerConfig
    : public NYTree::TYsonSerializable
{
public:
    explicit TThroughputThrottlerConfig(
        std::optional<double> limit = std::nullopt,
        TDuration period = TDuration::MilliSeconds(1000))
    {
        RegisterParameter("limit", Limit)
            .Default(limit)
            .GreaterThanOrEqual(0);
        RegisterParameter("period", Period)
            .Default(period);
    }

    //! Limit on average throughput (per sec). Null means unlimited.
    std::optional<double> Limit;

    //! Period for which the bandwidth limit applies.
    TDuration Period;
};

DEFINE_REFCOUNTED_TYPE(TThroughputThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

//! This wrapper may be helpful if we have some parametrization over the limit
//! (e.g. in network bandwith limit on nodes).
//! The exact logic of limit/relative_limit clash resolution
//! and the parameter access are external to the config itself.
class TRelativeThroughputThrottlerConfig
    : public TThroughputThrottlerConfig
{
public:
    explicit TRelativeThroughputThrottlerConfig(
        std::optional<double> limit = std::nullopt,
        TDuration period = TDuration::MilliSeconds(1000))
        : TThroughputThrottlerConfig(limit, period)
    {
        RegisterParameter("relative_limit", RelativeLimit)
            .InRange(0.0, 1.0)
            .Default();
    }

    std::optional<double> RelativeLimit;
};

DEFINE_REFCOUNTED_TYPE(TRelativeThroughputThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
