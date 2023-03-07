#pragma once

#include "public.h"

#include <yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentTrackerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TControllerAgentTrackerServiceProxy, ControllerAgentTrackerService,
        .SetProtocolVersion(2));

    DEFINE_RPC_PROXY_METHOD(NProto, Handshake);
    DEFINE_RPC_PROXY_METHOD(NProto, Heartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

