#pragma once

#include "public.h"

#include <yt/yt/core/rpc/client.h>

namespace NYT::NTabletBalancerClient {

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TTabletBalancerServiceProxy, TTabletBalancerService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NProto, RequestBalancing);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancerClient
