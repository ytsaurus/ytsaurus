#pragma once

#include "common.h"
#include "registry_service_rpc.pb.h"

#include "../rpc/service.h"
#include "../rpc/client.h"

namespace NYT {
namespace NRegistry {

////////////////////////////////////////////////////////////////////////////////

class TRegistryServiceProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TRegistryServiceProxy> TPtr;

    DECLARE_POLY_ENUM2(EErrorCode, NRpc::EErrorCode,
        ((ShitHappens)(1))
    );

    static Stroka GetServiceName()
    {
        return "RegistryService";
    }

    TRegistryServiceProxy(NRpc::IChannel::TPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    RPC_PROXY_METHOD(NProto, Get);
    RPC_PROXY_METHOD(NProto, Set);
    RPC_PROXY_METHOD(NProto, Remove);
    RPC_PROXY_METHOD(NProto, Lock);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRegistry
} // namespace NYT
