#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/ytlib/cellar_node_tracker_client/proto/cellar_node_tracker_service.pb.h>

#include <yt/yt/core/actions/signal.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

struct ICellarNodeTracker
    : public virtual TRefCounted
{
    //! Fired when an incremental heartbeat is received from a node.
    DECLARE_INTERFACE_SIGNAL(void(
        NNodeTrackerServer::TNode* node,
        NCellarNodeTrackerClient::NProto::TReqHeartbeat* request,
        NCellarNodeTrackerClient::NProto::TRspHeartbeat* response),
        Heartbeat);

    virtual void Initialize() = 0;

    using TCtxHeartbeat = NRpc::TTypedServiceContext<
        NCellarNodeTrackerClient::NProto::TReqHeartbeat,
        NCellarNodeTrackerClient::NProto::TRspHeartbeat>;
    using TCtxHeartbeatPtr = TIntrusivePtr<TCtxHeartbeat>;
    virtual void ProcessHeartbeat(TCtxHeartbeatPtr context) = 0;

    // COMPAT(savrus)
    virtual void ProcessHeartbeat(
        NNodeTrackerServer::TNode* node,
        NCellarNodeTrackerClient::NProto::TReqHeartbeat* request,
        NCellarNodeTrackerClient::NProto::TRspHeartbeat* response,
        bool legacyFullHeartbeat = false) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellarNodeTracker)

////////////////////////////////////////////////////////////////////////////////

ICellarNodeTrackerPtr CreateCellarNodeTracker(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
