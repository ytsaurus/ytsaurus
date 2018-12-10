#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TThroughputThrottlerConfig
    : public NYTree::TYsonSerializable
{
public:
    explicit TThroughputThrottlerConfig(
        std::optional<i64> limit = std::nullopt,
        TDuration period = TDuration::MilliSeconds(1000))
    {
        RegisterParameter("limit", Limit)
            .Default(limit)
            .GreaterThanOrEqual(0);
        RegisterParameter("period", Period)
            .Default(period);
    }

    //! Limit on average throughput (per sec). Null means unlimited.
    std::optional<i64> Limit;

    //! Period for which the bandwidth limit applies.
    TDuration Period;
};

DEFINE_REFCOUNTED_TYPE(TThroughputThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
