#pragma once

#include "common.h"
#include "cypress_service_rpc.pb.h"

#include "../rpc/service.h"
#include "../rpc/client.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

class TCypressServiceProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TCypressServiceProxy> TPtr;

    RPC_DECLARE_PROXY(CypressService,
        ((NoSuchTransaction)(1))
        ((RecoverableError)(2))
        ((UnrecoverableError)(3))
    );

    TCypressServiceProxy(NRpc::IChannel::TPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    RPC_PROXY_METHOD(NProto, Get);
    RPC_PROXY_METHOD(NProto, Set);
    RPC_PROXY_METHOD(NProto, Remove);
    RPC_PROXY_METHOD(NProto, Lock);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
