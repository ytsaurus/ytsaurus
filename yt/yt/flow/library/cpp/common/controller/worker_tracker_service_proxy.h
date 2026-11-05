#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/controller/proto/worker_tracker_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

class TWorkerTrackerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TWorkerTrackerServiceProxy, WorkerTrackerService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NProto, Handshake);
    DEFINE_RPC_PROXY_METHOD(NProto, Heartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
