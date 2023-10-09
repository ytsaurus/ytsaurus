#pragma once

#include <yt/yt/server/lib/hydra/proto/hydra_service.pb.h>

#include <yt/yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TInternalHydraServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TInternalHydraServiceProxy, InternalHydraService,
        .SetProtocolVersion(4)
        .SetAcceptsBaggage(false));

    DEFINE_RPC_PROXY_METHOD(NProto, LookupSnapshot,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, ReadSnapshot,
        .SetStreamingEnabled(true));
    DEFINE_RPC_PROXY_METHOD(NProto, ReadChangeLog);
    DEFINE_RPC_PROXY_METHOD(NProto, LookupChangelog,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, GetLatestChangelogId,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, AcceptMutations);
    DEFINE_RPC_PROXY_METHOD(NProto, AcquireChangelog,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, PingFollower,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, SyncWithLeader,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, CommitMutation);
    DEFINE_RPC_PROXY_METHOD(NProto, AbandonLeaderLease,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
    DEFINE_RPC_PROXY_METHOD(NProto, ReportMutationsStateHashes,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
