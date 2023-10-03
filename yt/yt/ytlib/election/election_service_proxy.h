#pragma once

#include "public.h"

#include <yt/yt/ytlib/election/proto/election_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

class TElectionServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TElectionServiceProxy, ElectionService,
        .SetProtocolVersion(2));

    DEFINE_RPC_PROXY_METHOD(NElection::NProto, PingFollower,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NElection::NProto, GetStatus,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NElection::NProto, Discombobulate,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
