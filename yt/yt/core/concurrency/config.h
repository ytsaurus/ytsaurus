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
        std::optional<double> limit = std::nullopt,
        TDuration period = TDuration::MilliSeconds(1000))
    {
        RegisterParameter("limit", Limit)
            .Default(limit)
            .GreaterThanOrEqual(0);
        RegisterParameter("period", Period)
            .Default(period);
        RegisterParameter("enable_fifo_order", EnableFifoOrder)
            .Default(true)
            .DontSerializeDefault();
    }

    //! Limit on average throughput (per sec). Null means unlimited.
    std::optional<double> Limit;

    //! Period for which the bandwidth limit applies.
    TDuration Period;

    //! If true, a request will be throttled even when bandwidth is immediately
    //! available but there're other previously throttled requests waiting in
    //! the queue.
    bool EnableFifoOrder;
};

DEFINE_REFCOUNTED_TYPE(TThroughputThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
