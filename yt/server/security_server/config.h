#pragma once

#include "public.h"

#include <ytlib/ytree/yson_serializable.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TSecurityManagerConfig
    : public TYsonSerializable
{
public:
    TDuration StatisticsFlushPeriod;
    TDuration RequestRateSmoothingPeriod;

    TSecurityManagerConfig()
    {
        RegisterParameter("statistics_flush_period", StatisticsFlushPeriod)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(1));
        RegisterParameter("request_rate_smoothing_period", RequestRateSmoothingPeriod)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(10));
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
