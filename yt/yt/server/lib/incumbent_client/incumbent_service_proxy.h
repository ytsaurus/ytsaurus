#pragma once

#include <yt/yt/server/lib/incumbent_client/proto/incumbent_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NIncumbentClient {

////////////////////////////////////////////////////////////////////////////////

class TIncumbentServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TIncumbentServiceProxy, IncumbentService);

    DEFINE_RPC_PROXY_METHOD(NProto, Heartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIncumbentClient
