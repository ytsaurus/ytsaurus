#pragma once

#include "public.h"

#include <yt/yt/server/lib/controller_agent/proto/job_tracker_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TJobTrackerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TJobTrackerServiceProxy, JobTrackerService);

    DEFINE_RPC_PROXY_METHOD(NProto, Heartbeat);

    DEFINE_RPC_PROXY_METHOD(NProto, SettleJob);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
