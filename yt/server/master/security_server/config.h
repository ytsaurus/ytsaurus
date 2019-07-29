#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TDynamicSecurityManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration AccountStatisticsGossipPeriod;
    TDuration RequestRateSmoothingPeriod;

    bool EnableDelayedMembershipClosureRecomputation;
    bool EnableAccessLog;
    TDuration MembershipClosureRecomputePeriod;

    TDynamicSecurityManagerConfig()
    {
        RegisterParameter("account_statistics_gossip_period", AccountStatisticsGossipPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("request_rate_smoothing_period", RequestRateSmoothingPeriod)
            .Default(TDuration::Seconds(1));

        RegisterParameter("enable_delayed_membership_closure_recomputation", EnableDelayedMembershipClosureRecomputation)
            .Default(false);
        RegisterParameter("membership_closure_recomputation_period", MembershipClosureRecomputePeriod)
            .Default(TDuration::Seconds(3));
        RegisterParameter("enable_access_log", EnableAccessLog)
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicSecurityManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
