#pragma once

#include "public.h"

#include <yt/ytlib/query_client/query_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TQueryServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "QueryService";
    }

    static int GetProtocolVersion()
    {
        return 27;
    }

    explicit TQueryServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName(), GetProtocolVersion())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, Execute);
    DEFINE_RPC_PROXY_METHOD(NProto, Read);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
