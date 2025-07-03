#include "config.h"

#include <yt/yt/ytlib/discovery_client/config.h>

#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/misc/adjusted_exponential_moving_average.h>

namespace NYT::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

void TDistributedThrottlerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("member_client", &TThis::MemberClient)
        .DefaultNew();
    registrar.Parameter("discovery_client", &TThis::DiscoveryClient)
        .DefaultNew();

    registrar.Parameter("control_rpc_timeout", &TThis::ControlRpcTimeout)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("limit_update_period", &TThis::LimitUpdatePeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("leader_update_period", &TThis::LeaderUpdatePeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("throttlers_attributes_update_period", &TThis::ThrottlersAttributesUpdatePeriod)
        .Default(TDuration::Seconds(20));

    registrar.Parameter("throttler_expiration_time", &TThis::ThrottlerExpirationTime)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("mode", &TThis::Mode)
        .Default(EDistributedThrottlerMode::Adaptive);
    registrar.Parameter("member_priority", &TThis::MemberPriority)
        .Default(EDistributedThrottlerMemberPriority::StartTime);
    registrar.Parameter("extra_limit_ratio", &TThis::ExtraLimitRatio)
        .Default(0.1)
        .LessThanOrEqual(1.0)
        .GreaterThanOrEqual(0);
    registrar.Parameter("adjusted_ema_halflife", &TThis::AdjustedEmaHalflife)
        .Default(TAdjustedExponentialMovingAverage::DefaultHalflife);

    registrar.Parameter("heartbeat_throttler_count_limit", &TThis::HeartbeatThrottlerCountLimit)
        .Default(100);

    registrar.Parameter("skip_unused_throttlers_count_limit", &TThis::SkipUnusedThrottlersCountLimit)
        .Default(50);

    registrar.Parameter("initialize_throttlers_on_creation", &TThis::InitializeThrottlersOnCreation)
        .Default(false);

    registrar.Parameter("update_limits_for_zero_rate_throttlers", &TThis::UpdateLimitsForZeroRateThrottlers)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedThrottler

