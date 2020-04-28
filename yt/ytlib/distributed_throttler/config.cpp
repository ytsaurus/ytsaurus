#include "config.h"

#include <yt/ytlib/discovery_client/config.h>

#include <yt/core/concurrency/config.h>

namespace NYT::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

TDistributedThrottlerConfig::TDistributedThrottlerConfig()
{
    RegisterParameter("member_client", MemberClient)
        .DefaultNew();
    RegisterParameter("discovery_client", DiscoveryClient)
        .DefaultNew();

    RegisterParameter("rpc_timeout", RpcTimeout)
        .Default(TDuration::Seconds(5));

    RegisterParameter("limit_update_period", LimitUpdatePeriod)
        .Default(TDuration::Seconds(1));
    RegisterParameter("leader_update_period", LeaderUpdatePeriod)
        .Default(TDuration::Seconds(10));

    RegisterParameter("throttler_expiration_time", ThrottlerExpirationTime)
        .Default(TDuration::Seconds(30));

    RegisterParameter("shard_count", ShardCount)
        .Default(16);

    RegisterParameter("distribute_limits_uniformly", DistributeLimitsUniformly)
        .Default(true);
    RegisterParameter("extra_limit_ratio", ExtraLimitRatio)
        .Default(0.1);
    RegisterParameter("ema_alpha", EmaAlpha)
        .Default(0.1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedThrottler

