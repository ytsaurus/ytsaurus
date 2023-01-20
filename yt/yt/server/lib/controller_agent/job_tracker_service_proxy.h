#pragma once

#include "public.h"

#include <yt/yt/server/lib/controller_agent/proto/job_tracker_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

// COMPAT(pogorelov): Remove when all nodes will be 23.1.
class TOldJobTrackerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TOldJobTrackerServiceProxy, ControllerAgentConnectorService);

    DEFINE_RPC_PROXY_METHOD(NProto, Heartbeat);
};

class TJobTrackerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TJobTrackerServiceProxy, JobTrackerService);

    DEFINE_RPC_PROXY_METHOD(NProto, Heartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
