#pragma once

#include "common.h"

#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

struct TElectionManagerConfig
    : public TConfigurable
{
    TDuration RpcTimeout;
    TDuration FollowerPingInterval;
    TDuration FollowerPingTimeout;
    TDuration ReadyToFollowTimeout;
    TDuration PotentialFollowerTimeout;

    TElectionManagerConfig()
    {
        Register("rpc_timeout", RpcTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::MilliSeconds(1000));
        Register("follower_ping_interval", FollowerPingInterval)
            .GreaterThan(TDuration())
            .Default(TDuration::MilliSeconds(1000));
        Register("follower_ping_timeout", FollowerPingTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::MilliSeconds(5000));
        Register("ready_to_follow_timeout", ReadyToFollowTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::MilliSeconds(5000));
        Register("potential_follower_timeout", PotentialFollowerTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::MilliSeconds(5000));
    }
};

typedef TIntrusivePtr<TElectionManagerConfig> TElectionManagerConfigPtr;

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NElection
} // namespace NYT
