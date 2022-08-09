#include "config.h"

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

void TDistributedElectionManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("voting_round_period", &TThis::VotingRoundPeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("control_rpc_timeout", &TThis::ControlRpcTimeout)
        .Default(TDuration::MilliSeconds(5000));
    registrar.Parameter("follower_ping_period", &TThis::FollowerPingPeriod)
        .Default(TDuration::MilliSeconds(1000));
    registrar.Parameter("follower_ping_rpc_timeout", &TThis::FollowerPingRpcTimeout)
        .Default(TDuration::MilliSeconds(1000));
    registrar.Parameter("leader_ping_timeout", &TThis::LeaderPingTimeout)
        .Default(TDuration::MilliSeconds(5000));
    registrar.Parameter("follower_grace_timeout", &TThis::FollowerGraceTimeout)
        .Default(TDuration::MilliSeconds(5000));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
