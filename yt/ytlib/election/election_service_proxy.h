#pragma once

#include "public.h"

#include <ytlib/election/election_service.pb.h>

#include <ytlib/rpc/client.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

class TElectionServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName();

    explicit TElectionServiceProxy(NRpc::IChannelPtr channel);

    DEFINE_RPC_PROXY_METHOD(NElection::NProto, PingFollower);
    DEFINE_RPC_PROXY_METHOD(NElection::NProto, GetStatus);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
