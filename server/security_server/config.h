#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TSecurityManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration UserStatisticsFlushPeriod;
    TDuration UserStatisticsGossipPeriod;
    TDuration AccountStatisticsGossipPeriod;
    TDuration RequestRateSmoothingPeriod;

    TSecurityManagerConfig()
    {
        RegisterParameter("user_statistics_gossip_period", UserStatisticsGossipPeriod)
            .Default(TDuration::Seconds(10));
        RegisterParameter("user_statistics_flush_period", UserStatisticsFlushPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("account_statistics_gossip_period", AccountStatisticsGossipPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("request_rate_smoothing_period", RequestRateSmoothingPeriod)
            .Default(TDuration::Seconds(1));
    }
};

DEFINE_REFCOUNTED_TYPE(TSecurityManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
