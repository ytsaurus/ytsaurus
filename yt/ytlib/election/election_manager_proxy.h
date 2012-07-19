#pragma once

#include "common.h"
#include <ytlib/election/election_manager.pb.h>

#include <ytlib/rpc/client.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

class TElectionManagerProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TElectionManagerProxy> TPtr;

    DECLARE_ENUM(EState,
        (Stopped)
        (Voting)
        (Leading)
        (Following)
    );

    static Stroka GetServiceName()
    {
        return "ElectionManager";
    }

    DECLARE_ENUM(EErrorCode,
        ((InvalidState)(1))
        ((InvalidLeader)(2))
        ((InvalidEpoch)(3))
    );

    TElectionManagerProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NElection::NProto, PingFollower)
    DEFINE_RPC_PROXY_METHOD(NElection::NProto, GetStatus)
    DEFINE_RPC_PROXY_METHOD(NElection::NProto, GetQuorum)
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
