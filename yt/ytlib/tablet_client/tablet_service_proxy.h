#pragma once

#include "public.h"

#include <core/rpc/client.h>

#include <ytlib/tablet_client/tablet_service.pb.h>

namespace NYT {
namespace NTabletClient {

////////////////////////////////////////////////////////////////////////////////

class TTabletServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "TabletService";
    }

    static int GetProtocolVersion()
    {
        return 4;
    }

    explicit TTabletServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName(), GetProtocolVersion())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, StartTransaction);
    DEFINE_RPC_PROXY_METHOD(NProto, Read);
    DEFINE_RPC_PROXY_METHOD(NProto, Write);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

