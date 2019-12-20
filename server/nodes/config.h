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
    bool ValidateCallerCertificate;
    std::optional<TString> CallerCertificateIssuer;

    bool EnableAgentNotification;
    TDuration AgentNotificationRpcTimeout;
    NConcurrency::TThroughputThrottlerConfigPtr AgentNotificationThrottler;

    int ProcessHeartbeatAttemptCount;

    TNodeTrackerConfig()
    {
        RegisterParameter("validate_caller_certificate", ValidateCallerCertificate)
            // COMPAT(babenko)
            .Alias("validate_agent_certificate")
            .Alias("validate_agent_identity")
            .Default(false);
        RegisterParameter("caller_certificate_issuer", CallerCertificateIssuer)
            // COMPAT(babenko)
            .Alias("agent_certificate_issuer")
            .Optional();
        RegisterParameter("enable_agent_notification", EnableAgentNotification)
            .Default(true);
        RegisterParameter("agent_notification_rpc_timeout", AgentNotificationRpcTimeout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("agent_notification_throttler", AgentNotificationThrottler)
            .DefaultNew();
        RegisterParameter("process_heartbeat_attempt_count", ProcessHeartbeatAttemptCount)
            .GreaterThanOrEqual(1)
            .Default(3);
        RegisterPreprocessor([&] {
            AgentNotificationThrottler->Limit = 10;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TNodeTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NNodes
