#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/concurrency/config.h>

namespace NYP::NServer::NNodes {

////////////////////////////////////////////////////////////////////////////////

class TNodeTrackerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    bool ValidateAgentCertificate;
    std::optional<TString> AgentCertificateIssuer;

    bool EnableAgentNotification;
    TDuration AgentNotificationRpcTimeout;
    NConcurrency::TThroughputThrottlerConfigPtr AgentNotificationThrottler;

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
        RegisterParameter("agent_notification_rpc_timeout", AgentNotificationRpcTimeout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("agent_notification_throttler", AgentNotificationThrottler)
            .DefaultNew();
        RegisterPreprocessor([&] {
            AgentNotificationThrottler->Limit = 10;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TNodeTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NNodes
