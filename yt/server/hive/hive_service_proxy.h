#pragma once

#include "public.h"

#include <core/rpc/client.h>

#include <server/hive/hive_service.pb.h>

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
