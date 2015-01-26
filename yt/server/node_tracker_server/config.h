#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

class TNodeTrackerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration RegisteredNodeTimeout;
    TDuration OnlineNodeTimeout;

    //! Limits the number of queued |FullHeartbeat| requests plus the number of registered nodes.
    //! When this limit is reached, |RegisterNode| method starts throttling.
    int MaxFullHeartbeatQueueSize;

    //! Limits the number of concurrent node removal mutations.
    int MaxConcurrentNodeRemoveMutations;

    TNodeTrackerConfig()
    {
        RegisterParameter("registered_node_timeout", RegisteredNodeTimeout)
            .Default(TDuration::Seconds(60));
        RegisterParameter("online_node_timeout", OnlineNodeTimeout)
            .Default(TDuration::Seconds(60));

        RegisterParameter("max_heartbeat_queue_size", MaxFullHeartbeatQueueSize)
            .Default(10)
            .GreaterThan(0);
        RegisterParameter("max_concurrent_node_remove_mutations", MaxConcurrentNodeRemoveMutations)
            .Default(10)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TNodeTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

class TNodeConfig
    : public NYTree::TYsonSerializable
{
public:
    bool Banned;
    bool Decommissioned;
    TNullable<Stroka> Rack;

    TNodeConfig()
    {
        RegisterParameter("banned", Banned)
            .Default(false);
        RegisterParameter("decommissioned", Decommissioned)
            .Default(false);
        RegisterParameter("rack", Rack)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TNodeConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
