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

    DECLARE_POLY_ENUM2(EErrorCode, NRpc::EErrorCode,
        ((ShitHappens)(1))
    );

    static Stroka GetServiceName()
    {
        return "CypressService";
    }

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
