#pragma once

#include "public.h"

#include <yt/yt/server/lib/scheduler/proto/allocation_tracker_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TAllocationTrackerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TAllocationTrackerServiceProxy, AllocationTrackerService);

    DEFINE_RPC_PROXY_METHOD(NProto::NNode, Heartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
