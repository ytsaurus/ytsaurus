#pragma once

#include "public.h"

#include <yt/yt/ytlib/auth/proto/tvm_bridge_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

class TTvmBridgeServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TTvmBridgeServiceProxy, TvmBridgeService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NProto, FetchTickets);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
