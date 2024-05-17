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

class  TDynamicSecurityManagerConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration AccountStatisticsGossipPeriod;
    TDuration AccountsProfilingPeriod;
    TDuration RequestRateSmoothingPeriod;
    TDuration AccountMasterMemoryUsageUpdatePeriod;

    // COMPAT(h0pless): This is a panic button, in case account profiling breaks.
    bool EnableAccountsProfiling;
    bool EnableDelayedMembershipClosureRecomputation;
    bool EnableAccessLog;
    TDuration MembershipClosureRecomputePeriod;
    bool EnableMasterMemoryUsageValidation;
    bool EnableMasterMemoryUsageAccountOvercommitValidation;
    // COMPAT(ifsmirnov)
    bool EnableTabletResourceValidation;

    bool EnableDistributedThrottler;

    int MaxAccountSubtreeSize;

    //! A bound for number of tags per user.
    int MaxUserTagCount;

    //! A bound for user tag size.
    int MaxUserTagSize;

    //! A bound for user tag filter size.
    int MaxSubjectTagFilterSize;

    bool ForbidIrreversibleAclChanges;

    //! Period between user statistics commits.
    TDuration UserStatisticsFlushPeriod;

    //! COMPAT(cherepashka)
    bool DisableUpdateUserLastSeen;

    // COMPAT(h0pless): This is a flag that makes the related commit rollable. See ace_iterator.cpp.
    bool FixSubjectTagFilterIteratorNeverSkippingFirstAce;

    // This is a panic button.
    bool EnableSubjectTagFilters;

    REGISTER_YSON_STRUCT(TDynamicSecurityManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicSecurityManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
