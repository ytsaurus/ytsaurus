#pragma once

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

class TDistributedElectionManagerConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration VotingRoundPeriod;
    TDuration ControlRpcTimeout;
    TDuration FollowerPingPeriod;
    TDuration FollowerPingRpcTimeout;
    TDuration LeaderPingTimeout;
    TDuration FollowerGraceTimeout;
    TDuration DiscombobulatedLeaderPingTimeout;

    REGISTER_YSON_STRUCT(TDistributedElectionManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDistributedElectionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
