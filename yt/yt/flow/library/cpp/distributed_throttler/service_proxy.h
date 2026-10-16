#pragma once

#include <yt/yt/flow/library/cpp/distributed_throttler/proto/distributed_throttler_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

class TDistributedThrottlerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TDistributedThrottlerServiceProxy, DistributedThrottlerService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NProto, RequestQuota);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
