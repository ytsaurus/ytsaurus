#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

class TNodeTrackerConfig
    : public NYTree::TYsonSerializable
{
public:
    int MaxConcurrentNodeRegistrations;
    int MaxConcurrentNodeUnregistrations;
    int MaxConcurrentFullHeartbeats;
    int MaxConcurrentIncrementalHeartbeats;
    TDuration NodeStatesGossipPeriod;

    TNodeTrackerConfig()
    {
        RegisterParameter("max_concurrent_node_registrations", MaxConcurrentNodeRegistrations)
            .Default(5)
            .GreaterThan(0);
        RegisterParameter("max_concurrent_node_unregistrations", MaxConcurrentNodeUnregistrations)
            .Default(5)
            .GreaterThan(0);
        RegisterParameter("max_concurrent_full_heartbeats", MaxConcurrentFullHeartbeats)
            .Default(1)
            .GreaterThan(0);
        RegisterParameter("max_concurrent_incremental_heartbeats", MaxConcurrentIncrementalHeartbeats)
            .Default(10)
            .GreaterThan(0);
        RegisterParameter("node_states_gossip_period", NodeStatesGossipPeriod)
            .Default(TDuration::Seconds(1));
    }
};

DEFINE_REFCOUNTED_TYPE(TNodeTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
