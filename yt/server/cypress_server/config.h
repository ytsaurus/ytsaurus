#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TCypressManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration StatisticsFlushPeriod;
    TNullable<int> MaxNodeChildCount;

    TCypressManagerConfig()
    {
        RegisterParameter("statistics_flush_period", StatisticsFlushPeriod)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(1));
        RegisterParameter("max_node_child_count", MaxNodeChildCount)
            .GreaterThan(20)
            .Default(50000);
    }
};

DEFINE_REFCOUNTED_TYPE(TCypressManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
