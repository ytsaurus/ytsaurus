#pragma once

#include "public.h"

#include <yt/core/ytree/attributes.h>

#include <yt/core/ytree/yson_serializable.h>

#include <yt/ytlib/discovery_client/public.h>

namespace NYT::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

class TDistributedThrottlerConfig
    : public NYTree::TYsonSerializable
{
public:
    NDiscoveryClient::TMemberClientConfigPtr MemberClient;
    NDiscoveryClient::TDiscoveryClientConfigPtr DiscoveryClient;

    TDuration RpcTimeout;

    TDuration LimitUpdatePeriod;
    TDuration LeaderUpdatePeriod;

    TDuration ThrottlerExpirationTime;

    int ShardCount;

    bool DistributeLimitsUniformly;
    double ExtraLimitRatio;
    double EmaAlpha;

    TDistributedThrottlerConfig();
};

DEFINE_REFCOUNTED_TYPE(TDistributedThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedThrottler

