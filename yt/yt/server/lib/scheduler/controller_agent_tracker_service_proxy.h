#pragma once

#include "public.h"

#include <yt/yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentTrackerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TControllerAgentTrackerServiceProxy, ControllerAgentTrackerService,
        .SetProtocolVersion(26));

    DEFINE_RPC_PROXY_METHOD(NProto, Handshake);
    DEFINE_RPC_PROXY_METHOD(NProto, Heartbeat);
    DEFINE_RPC_PROXY_METHOD(NProto, ScheduleAllocationHeartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

