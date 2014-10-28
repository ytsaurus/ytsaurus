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
    TDuration VotingRoundPeriod;
    TDuration ControlRpcTimeout;
    TDuration FollowerPingPeriod;
    TDuration FollowerPingRpcTimeout;
    TDuration LeaderLeaseTimeout;
    TDuration FollowerGraceTimeout;

    TElectionManagerConfig()
    {
        RegisterParameter("voting_round_period", VotingRoundPeriod)
            .Default(TDuration::MilliSeconds(100));
        RegisterParameter("control_rpc_timeout", ControlRpcTimeout)
            .Default(TDuration::MilliSeconds(1000));
        RegisterParameter("follower_ping_period", FollowerPingPeriod)
            .Default(TDuration::MilliSeconds(1000));
        RegisterParameter("follower_ping_rpc_timeout", FollowerPingRpcTimeout)
            .Default(TDuration::MilliSeconds(5000));
        RegisterParameter("leader_lease_timeout", LeaderLeaseTimeout)
            .Default(TDuration::MilliSeconds(5000));
        RegisterParameter("follower_grace_timeout", FollowerGraceTimeout)
            .Default(TDuration::MilliSeconds(5000));
    }
};

DEFINE_REFCOUNTED_TYPE(TElectionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
