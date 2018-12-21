#pragma once

#include "public.h"

#include <yt/ytlib/election/election_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

class TElectionServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TElectionServiceProxy, ElectionService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NElection::NProto, PingFollower,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NElection::NProto, GetStatus,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
