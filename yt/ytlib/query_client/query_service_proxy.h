#pragma once

#include "public.h"

#include <core/rpc/client.h>

#include <ytlib/query_client/query_service.pb.h>

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
        return 7;
    }

    explicit TQueryServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName(), GetProtocolVersion())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, Execute);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
