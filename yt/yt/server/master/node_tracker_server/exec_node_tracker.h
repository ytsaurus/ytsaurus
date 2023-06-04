#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/ytlib/exec_node_tracker_client/proto/exec_node_tracker_service.pb.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

struct IExecNodeTracker
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    using TCtxHeartbeat = NRpc::TTypedServiceContext<
        NExecNodeTrackerClient::NProto::TReqHeartbeat,
        NExecNodeTrackerClient::NProto::TRspHeartbeat>;
    using TCtxHeartbeatPtr = TIntrusivePtr<TCtxHeartbeat>;
    virtual void ProcessHeartbeat(TCtxHeartbeatPtr context) = 0;

    // COMPAT(gritukan)
    virtual void ProcessHeartbeat(
        TNode* node,
        NExecNodeTrackerClient::NProto::TReqHeartbeat* request,
        NExecNodeTrackerClient::NProto::TRspHeartbeat* response) = 0;
};

DEFINE_REFCOUNTED_TYPE(IExecNodeTracker)

////////////////////////////////////////////////////////////////////////////////

IExecNodeTrackerPtr CreateExecNodeTracker(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
