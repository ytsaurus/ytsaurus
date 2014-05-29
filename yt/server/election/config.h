#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

class TElectionManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration VotingRoundInterval;
    TDuration RpcTimeout;
    TDuration FollowerPingInterval;
    TDuration FollowerPingTimeout;
    TDuration ReadyToFollowTimeout;
    TDuration FollowerGracePeriod;

    TElectionManagerConfig()
    {
        RegisterParameter("voting_round_interval", VotingRoundInterval)
            .GreaterThan(TDuration())
            .Default(TDuration::MilliSeconds(100));
        RegisterParameter("rpc_timeout", RpcTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::MilliSeconds(1000));
        RegisterParameter("follower_ping_interval", FollowerPingInterval)
            .GreaterThan(TDuration())
            .Default(TDuration::MilliSeconds(1000));
        RegisterParameter("follower_ping_timeout", FollowerPingTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::MilliSeconds(5000));
        RegisterParameter("ready_to_follow_timeout", ReadyToFollowTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::MilliSeconds(5000));
        RegisterParameter("follower_grace_period", FollowerGracePeriod)
            .GreaterThan(TDuration())
            .Default(TDuration::MilliSeconds(5000));
    }
};

DEFINE_REFCOUNTED_TYPE(TElectionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
