#pragma once

#include "public.h"

#include <yt/server/hive/hive_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

class THiveServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "HiveService";
    }

    explicit THiveServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, Ping);
    DEFINE_RPC_PROXY_METHOD(NProto, PostMessages);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
