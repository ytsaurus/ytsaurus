#pragma once

#include "public.h"

#include <yt/ytlib/election/election_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

class TElectionServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TElectionServiceProxy, RPC_PROXY_DESC(ElectionService)
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NElection::NProto, PingFollower);
    DEFINE_RPC_PROXY_METHOD(NElection::NProto, GetStatus);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
