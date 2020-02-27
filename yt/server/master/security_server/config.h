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
    TDuration AccountMasterMemoryUsageUpdatePeriod;

    bool EnableDelayedMembershipClosureRecomputation;
    bool EnableAccessLog;
    TDuration MembershipClosureRecomputePeriod;
    bool EnableMasterMemoryUsageValidation;
    bool EnableMasterMemoryUsageAccountOvercommitValidation;

    TDynamicSecurityManagerConfig()
    {
        RegisterParameter("account_statistics_gossip_period", AccountStatisticsGossipPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("request_rate_smoothing_period", RequestRateSmoothingPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("account_master_memory_usage_update_period", AccountMasterMemoryUsageUpdatePeriod)
            .Default(TDuration::Seconds(60));

        RegisterParameter("enable_delayed_membership_closure_recomputation", EnableDelayedMembershipClosureRecomputation)
            .Default(true);
        RegisterParameter("membership_closure_recomputation_period", MembershipClosureRecomputePeriod)
            .Default(TDuration::Seconds(3));
        RegisterParameter("enable_access_log", EnableAccessLog)
            .Default(true);
        RegisterParameter("enable_master_memory_usage_validation", EnableMasterMemoryUsageValidation)
            .Default(false);
        RegisterParameter("enable_master_memory_usage_account_overcommit_validation", EnableMasterMemoryUsageAccountOvercommitValidation)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicSecurityManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
