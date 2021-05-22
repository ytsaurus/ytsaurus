#pragma once

#include <yt/yt/core/rpc/client.h>

#include <yt/yt/ytlib/distributed_throttler/proto/distributed_throttler.pb.h>

namespace NYT::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

class TDistributedThrottlerProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TDistributedThrottlerProxy, DistributedThrottlerService);

    DEFINE_RPC_PROXY_METHOD(NProto, Heartbeat);
    DEFINE_RPC_PROXY_METHOD(NProto, Throttle);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedThrottler
