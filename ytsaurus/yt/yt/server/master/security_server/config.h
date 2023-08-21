#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/ytlib/distributed_throttler/config.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TSecurityManagerConfig
    : public NYTree::TYsonStruct
{
public:
    NDistributedThrottler::TDistributedThrottlerConfigPtr UserThrottler;

    // COMPAT(gritukan): Remove after RecomputeAccountRefCounters.
    bool AlertOnAccountRefCounterMismatch;

    REGISTER_YSON_STRUCT(TSecurityManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSecurityManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicSecurityManagerConfig
    : public NYTree::TYsonStruct
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
    // COMPAT(ifsmirnov)
    bool EnableTabletResourceValidation;

    bool EnableDistributedThrottler;

    int MaxAccountSubtreeSize;

    REGISTER_YSON_STRUCT(TDynamicSecurityManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicSecurityManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
