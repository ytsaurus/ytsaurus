#pragma once

#include "public.h"

#include <yt/yt/core/ytree/attributes.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/ytlib/discovery_client/public.h>

namespace NYT::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDistributedThrottlerMode,
    (Uniform)
    (Adaptive)
    (Precise)
);

DEFINE_ENUM(EDistributedThrottlerMemberPriority,
    (StartTime)
    (Random)
);

struct TDistributedThrottlerConfig
    : public NYTree::TYsonStruct
{
    NDiscoveryClient::TMemberClientConfigPtr MemberClient;
    NDiscoveryClient::TDiscoveryClientConfigPtr DiscoveryClient;

    TDuration ControlRpcTimeout;

    TDuration LimitUpdatePeriod;
    TDuration LeaderUpdatePeriod;
    TDuration ThrottlersAttributesUpdatePeriod;

    TDuration ThrottlerExpirationTime;

    TDuration AdjustedEmaHalflife;

    EDistributedThrottlerMode Mode;
    EDistributedThrottlerMemberPriority MemberPriority;
    double ExtraLimitRatio;

    int HeartbeatThrottlerCountLimit;
    int SkipUnusedThrottlersCountLimit;

    bool InitializeThrottlersOnCreation;
    bool UpdateLimitsForZeroRateThrottlers;

    REGISTER_YSON_STRUCT(TDistributedThrottlerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDistributedThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedThrottler
