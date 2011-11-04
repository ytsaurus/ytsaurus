#pragma once

#include "common.h"
#include "orchid_service_rpc.pb.h"

#include "../rpc/service.h"
#include "../rpc/client.h"

namespace NYT {
namespace NOrchid {

////////////////////////////////////////////////////////////////////////////////

class TOrchidServiceProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TOrchidServiceProxy> TPtr;

    RPC_DECLARE_PROXY(OrchidService,
        ((YPathError)(1))
    );

    TOrchidServiceProxy(NRpc::IChannel::TPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    RPC_PROXY_METHOD(NProto, Get);
    RPC_PROXY_METHOD(NProto, Set);
    RPC_PROXY_METHOD(NProto, Remove);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT
