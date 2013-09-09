#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TCypressManagerConfig
    : public TYsonSerializable
{
public:
    TDuration AccessStatisticsFlushPeriod;

    TCypressManagerConfig()
    {
        RegisterParameter("access_statistics_flush_period", AccessStatisticsFlushPeriod)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(1));
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
