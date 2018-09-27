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
    bool ValidateAgentCertificate;
    TNullable<TString> AgentCertificateIssuer;

    bool EnableAgentNotification;
    TDuration AgentNotificationRpcTimeout;

    TNodeTrackerConfig()
    {
        RegisterParameter("validate_agent_certificate", ValidateAgentCertificate)
            // COMPAT(babenko)
            .Alias("validate_agent_identity")
            .Default(false);
        RegisterParameter("agent_certificate_issuer", AgentCertificateIssuer)
            .Optional();
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
