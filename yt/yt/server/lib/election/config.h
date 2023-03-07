#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

class TDistributedElectionManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration VotingRoundPeriod;
    TDuration ControlRpcTimeout;
    TDuration FollowerPingPeriod;
    TDuration FollowerPingRpcTimeout;
    TDuration LeaderPingTimeout;
    TDuration FollowerGraceTimeout;

    TDistributedElectionManagerConfig()
    {
        RegisterParameter("voting_round_period", VotingRoundPeriod)
            .Default(TDuration::MilliSeconds(100));
        RegisterParameter("control_rpc_timeout", ControlRpcTimeout)
            .Default(TDuration::MilliSeconds(5000));
        RegisterParameter("follower_ping_period", FollowerPingPeriod)
            .Default(TDuration::MilliSeconds(1000));
        RegisterParameter("follower_ping_rpc_timeout", FollowerPingRpcTimeout)
            .Default(TDuration::MilliSeconds(1000));
        RegisterParameter("leader_ping_timeout", LeaderPingTimeout)
            .Default(TDuration::MilliSeconds(5000));
        RegisterParameter("follower_grace_timeout", FollowerGraceTimeout)
            .Default(TDuration::MilliSeconds(5000));
    }
};

DEFINE_REFCOUNTED_TYPE(TDistributedElectionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
