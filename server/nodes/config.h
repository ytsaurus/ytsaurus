#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYP {
namespace NServer {
namespace NNodes {

////////////////////////////////////////////////////////////////////////////////

class TNodeTrackerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    bool ValidateAgentIdentity;
    bool EnableAgentNotification;
    TDuration AgentNotificationRpcTimeout;

    TNodeTrackerConfig()
    {
        RegisterParameter("validate_agent_identity", ValidateAgentIdentity)
            .Default(false);
        RegisterParameter("enable_agent_notification", EnableAgentNotification)
            .Default(true);
        RegisterParameter("notify_rpc_timeout", AgentNotificationRpcTimeout)
            .Default(TDuration::Seconds(5));
    }
};

DEFINE_REFCOUNTED_TYPE(TNodeTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodes
} // namespace NServer
} // namespace NYP
