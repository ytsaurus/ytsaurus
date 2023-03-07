#pragma once

#include "public.h"

#include <yt/ytlib/hydra/proto/hydra_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class THydraServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(THydraServiceProxy, HydraService,
        .SetProtocolVersion(3));

    DEFINE_RPC_PROXY_METHOD(NProto, ReadChangeLog);
    DEFINE_RPC_PROXY_METHOD(NProto, LookupChangelog,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, AcceptMutations);
    DEFINE_RPC_PROXY_METHOD(NProto, BuildSnapshot,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, ForceBuildSnapshot,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, RotateChangelog,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, PingFollower,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, SyncWithLeader,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, CommitMutation);
    DEFINE_RPC_PROXY_METHOD(NProto, Poke,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, AbandonLeaderLease,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
