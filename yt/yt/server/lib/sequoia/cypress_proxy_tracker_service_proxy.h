#pragma once

#include <yt/yt/server/lib/sequoia/proto/cypress_proxy_tracker.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

class TCypressProxyTrackerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TCypressProxyTrackerServiceProxy, CypressProxyTrackerService);

    DEFINE_RPC_PROXY_METHOD(NProto, Heartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
