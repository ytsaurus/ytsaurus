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
    //! Period between Cypress access statistics commits.
    TDuration StatisticsFlushPeriod;

    //! Maximum number of children map and list nodes are allowed to contain.
    //! NB: Changing these values will invalidate all changelogs!
    int MaxNodeChildCount;

    //! Maximum allowed length of string nodes.
    //! NB: Changing these values will invalidate all changelogs!
    int MaxStringNodeLength;

    TCypressManagerConfig()
    {
        RegisterParameter("statistics_flush_period", StatisticsFlushPeriod)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(1));
        RegisterParameter("max_node_child_count", MaxNodeChildCount)
            .GreaterThan(20)
            .Default(50000);
        RegisterParameter("max_string_node_length", MaxStringNodeLength)
            .GreaterThan(256)
            .Default(65536);
    }
};

DEFINE_REFCOUNTED_TYPE(TCypressManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
