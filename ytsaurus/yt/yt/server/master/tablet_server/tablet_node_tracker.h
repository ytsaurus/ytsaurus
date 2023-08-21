#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/ytlib/tablet_node_tracker_client/proto/tablet_node_tracker_service.pb.h>

#include <yt/yt/core/actions/signal.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct ITabletNodeTracker
    : public virtual TRefCounted
{
    //! Fired when an incremental heartbeat is received from a node.
    DECLARE_INTERFACE_SIGNAL(void(
        NNodeTrackerServer::TNode* node,
        NTabletNodeTrackerClient::NProto::TReqHeartbeat* request,
        NTabletNodeTrackerClient::NProto::TRspHeartbeat* response),
        Heartbeat);

    virtual void Initialize() = 0;

    using TCtxHeartbeat = NRpc::TTypedServiceContext<
        NTabletNodeTrackerClient::NProto::TReqHeartbeat,
        NTabletNodeTrackerClient::NProto::TRspHeartbeat>;
    using TCtxHeartbeatPtr = TIntrusivePtr<TCtxHeartbeat>;
    virtual void ProcessHeartbeat(TCtxHeartbeatPtr context) = 0;

    // COMPAT(gritukan)
    virtual void ProcessHeartbeat(
        NNodeTrackerServer::TNode* node,
        NTabletNodeTrackerClient::NProto::TReqHeartbeat* request,
        NTabletNodeTrackerClient::NProto::TRspHeartbeat* response,
        bool legacyFullHeartbeat = false) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletNodeTracker)

////////////////////////////////////////////////////////////////////////////////

ITabletNodeTrackerPtr CreateTabletNodeTracker(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
