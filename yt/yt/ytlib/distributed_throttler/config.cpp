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

    registrar.Parameter("throttler_expiration_time", &TThis::ThrottlerExpirationTime)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("mode", &TThis::Mode)
        .Default(EDistributedThrottlerMode::Adaptive);
    registrar.Parameter("extra_limit_ratio", &TThis::ExtraLimitRatio)
        .Default(0.1);
    registrar.Parameter("adjusted_ema_halflife", &TThis::AdjustedEmaHalflife)
        .Default(TAdjustedExponentialMovingAverage::DefaultHalflife);

    registrar.Parameter("heartbeat_throttler_count_limit", &TThis::HeartbeatThrottlerCountLimit)
        .Default(100);

    registrar.Parameter("skip_unused_throttlers_count_limit", &TThis::SkipUnusedThrottlersCountLimit)
        .Default(50);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedThrottler

