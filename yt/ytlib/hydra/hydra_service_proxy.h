#pragma once

#include "public.h"

#include <core/rpc/client.h>

#include <ytlib/hydra/hydra_service.pb.h>

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
    DEFINE_RPC_PROXY_METHOD(NProto, RotateChangelog);
    DEFINE_RPC_PROXY_METHOD(NProto, PingFollower);
    DEFINE_RPC_PROXY_METHOD(NProto, SyncWithLeader);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
