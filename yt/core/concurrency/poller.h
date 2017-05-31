#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TThroughputThrottlerConfig
    : public NYTree::TYsonSerializable
{
public:
    explicit TThroughputThrottlerConfig(
        TNullable<i64> limit = Null,
        TDuration period = TDuration::MilliSeconds(1000))
    {
        RegisterParameter("limit", Limit)
            .Default(limit)
            .GreaterThanOrEqual(0);
        RegisterParameter("period", Period)
            .Default(period);
    }

    //! Limit on average throughput (per sec). Null means unlimited.
    TNullable<i64> Limit;

    //! Period for which the bandwidth limit applies.
    TDuration Period;
};

DEFINE_REFCOUNTED_TYPE(TThroughputThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
