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
        Register("online_node_timeout", OnlineNodeTimeout)
            .Default(TDuration::Seconds(60));
        Register("registered_node_timeout", RegisteredNodeTimeout)
            .Default(TDuration::Seconds(10));
        Register("unconfirmed_node_timeout", UnconfirmedNodeTimeout)
            .Default(TDuration::Seconds(30));

        Register("full_heartbeat_queue_size_limit", FullHeartbeatQueueSizeLimit)
            .Default(20)
            .GreaterThan(0);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
