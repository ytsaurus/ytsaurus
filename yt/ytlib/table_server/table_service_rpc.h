#pragma once

#include "common.h"
#include "table_service_rpc.pb.h"

#include "../rpc/service.h"
#include "../rpc/client.h"

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TTableServiceProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TTableServiceProxy> TPtr;

    RPC_DECLARE_PROXY(TableService,
        ((NoSuchTransaction)(1))
        ((NoSuchNode)(2))
        ((NoSuchChunk)(3))
        ((NotATable)(4))
    );

    TTableServiceProxy(NRpc::IChannel* channel)
        : TProxyBase(channel, GetServiceName())
    { }

    RPC_PROXY_METHOD(NProto, SetTableChunk);
    RPC_PROXY_METHOD(NProto, GetTableChunk);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

