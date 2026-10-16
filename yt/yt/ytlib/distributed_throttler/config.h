#pragma once

#include "public.h"

#include <yt/yt/core/ytree/attributes.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/discovery_client/public.h>

namespace NYT::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDistributedThrottlerMode,
    (Uniform)
    (Adaptive)
    (Precise)
);

// It's not really a generator; it's generation policy.
// TODO(h0pless): rename.
DEFINE_ENUM(EDistributedThrottlerMemberPriorityGenerator,
    (StartTime)
    (Random)
);

DEFINE_ENUM(EDistributedThrottlerMemberWeightMode,
    (Uniform)
    (QueueBased)
    (QueueBasedFree)
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
    TDuration ObsoleteMembersRemovalPeriod;

    TDuration ThrottlerExpirationTime;
    TDuration MemberExpirationTime;

    TDuration AdjustedEmaHalflife;

    EDistributedThrottlerMode Mode;
    EDistributedThrottlerMemberWeightMode MemberWeightMode;
    double MemberWeightExponentialSmoothingFactor;
    EDistributedThrottlerMemberPriorityGenerator MemberPriorityGenerator;
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
