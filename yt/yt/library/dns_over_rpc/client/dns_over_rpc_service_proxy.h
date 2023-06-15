#include "public.h"

#include <yt/yt/core/rpc/client.h>

#include <yt/yt/library/dns_over_rpc/client/proto/dns_over_rpc_service.pb.h>

#pragma once

namespace NYT::NDns {

////////////////////////////////////////////////////////////////////////////////

class TDnsOverRpcServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TDnsOverRpcServiceProxy, DnsOverRpcService);

    DEFINE_RPC_PROXY_METHOD(NProto, Resolve);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns
