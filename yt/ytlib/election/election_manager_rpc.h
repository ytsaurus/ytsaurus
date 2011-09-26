#pragma once

#include "common.h"
#include "election_manager_rpc.pb.h"

#include "../rpc/service.h"
#include "../rpc/client.h"

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

    DECLARE_POLY_ENUM2(EErrorCode, NRpc::EErrorCode,
        ((InvalidState)(1))
        ((InvalidLeader)(2))
        ((InvalidEpoch)(3))
    );

    static Stroka GetServiceName()
    {
        return "ElectionManager";
    }

    TElectionManagerProxy(NRpc::IChannel::TPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    RPC_PROXY_METHOD(NElection::NProto, PingFollower)
    RPC_PROXY_METHOD(NElection::NProto, GetStatus)

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
