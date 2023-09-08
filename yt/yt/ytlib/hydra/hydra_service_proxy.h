#pragma once

#include "public.h"

#include <yt/yt/ytlib/hydra/proto/hydra_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class THydraServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(THydraServiceProxy, HydraService,
        .SetProtocolVersion(3)
        .SetAcceptsBaggage(false));

    DEFINE_RPC_PROXY_METHOD(NProto, ForceBuildSnapshot,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, ForceSyncWithLeader,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, Poke,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, PrepareLeaderSwitch,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, ForceRestart,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, GetPeerState,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, ResetStateHash,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, ExitReadOnly,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
