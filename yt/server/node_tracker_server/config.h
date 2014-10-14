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

    //! Limit for the number of queued FullHeartbeat requests plus the number of registered nodes before
    //! |RegisterNode| method starts replying |EErrorCode::Unavailable|.
    int MaxFullHeartbeatQueueSizeLimit;

    TNodeTrackerConfig()
    {
        RegisterParameter("registered_node_timeout", RegisteredNodeTimeout)
            .Default(TDuration::Seconds(10));
        RegisterParameter("online_node_timeout", OnlineNodeTimeout)
            .Default(TDuration::Seconds(60));

        RegisterParameter("max_full_heartbeat_queue_size", MaxFullHeartbeatQueueSizeLimit)
            .Default(10)
            .GreaterThan(0);
    }
};

class TNodeConfig
    : public NYTree::TYsonSerializable
{
public:
    bool Banned;
    bool Decommissioned;

    TNodeConfig()
    {
        RegisterParameter("banned", Banned)
            .Default(false);
        RegisterParameter("decommissioned", Decommissioned)
            .Default(false);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
