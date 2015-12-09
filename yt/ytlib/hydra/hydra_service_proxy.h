#pragma once

#include "public.h"

#include <yt/ytlib/hydra/hydra_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class THydraServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "HydraService";
    }

    explicit THydraServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, ReadChangeLog);
    DEFINE_RPC_PROXY_METHOD(NProto, LookupChangelog);
    DEFINE_RPC_PROXY_METHOD(NProto, LogMutations);
    DEFINE_RPC_PROXY_METHOD(NProto, BuildSnapshot);
    DEFINE_RPC_PROXY_METHOD(NProto, ForceBuildSnapshot);
    DEFINE_RPC_PROXY_METHOD(NProto, RotateChangelog);
    DEFINE_RPC_PROXY_METHOD(NProto, PingFollower);
    DEFINE_RPC_PROXY_METHOD(NProto, SyncWithLeader);
    DEFINE_RPC_PROXY_METHOD(NProto, CommitMutation);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
