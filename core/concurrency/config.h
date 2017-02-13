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
    TThroughputThrottlerConfig()
    {
        RegisterParameter("period", Period)
            .Default(TDuration::MilliSeconds(1000));
        RegisterParameter("limit", Limit)
            .Default()
            .GreaterThanOrEqual(0);
    }

    //! Period for which the bandwidth limit applies.
    TDuration Period;

    //! Limit on average throughput (per sec). Null means unlimited.
    TNullable<i64> Limit;
};

DEFINE_REFCOUNTED_TYPE(TThroughputThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
