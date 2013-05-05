#pragma once

#include "public.h"

#include <ytlib/ytree/yson_serializable.h>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

class TNodeTrackerConfig
    : public TYsonSerializable
{
public:
    TDuration OnlineNodeTimeout;
    TDuration RegisteredNodeTimeout;
    TDuration UnconfirmedNodeTimeout;

    //! Limit for the number of queued FullHeartbeat requests plus the number of registered nodes before
    //! RegisterNode starts replying EErrorCode::Unavailable.
    int FullHeartbeatQueueSizeLimit;

    TNodeTrackerConfig()
    {
        RegisterParameter("online_node_timeout", OnlineNodeTimeout)
            .Default(TDuration::Seconds(60));
        RegisterParameter("registered_node_timeout", RegisteredNodeTimeout)
            .Default(TDuration::Seconds(10));
        RegisterParameter("unconfirmed_node_timeout", UnconfirmedNodeTimeout)
            .Default(TDuration::Seconds(30));

        RegisterParameter("full_heartbeat_queue_size_limit", FullHeartbeatQueueSizeLimit)
            .Default(20)
            .GreaterThan(0);
    }
};

class TNodeConfig
    : public TYsonSerializable
{
public:
    bool Banned;

    TNodeConfig()
    {
        RegisterParameter("banned", Banned)
            .Default(false);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
