#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/ytlib/data_node_tracker_client/proto/data_node_tracker_service.pb.h>

#include <yt/yt/core/actions/signal.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct IDataNodeTracker
    : public virtual TRefCounted
{
    //! Fired when a full data node heartbeat is received.
    DECLARE_INTERFACE_SIGNAL(void(
        TNode* node,
        NDataNodeTrackerClient::NProto::TReqFullHeartbeat* request),
        FullHeartbeat);

    //! Fired when an incremental data node heartbeat is received.
    DECLARE_INTERFACE_SIGNAL(void(
        TNode* node,
        NDataNodeTrackerClient::NProto::TReqIncrementalHeartbeat* request),
        IncrementalHeartbeat);

    virtual void Initialize() = 0;

    using TCtxFullHeartbeat = NRpc::TTypedServiceContext<
        NDataNodeTrackerClient::NProto::TReqFullHeartbeat,
        NDataNodeTrackerClient::NProto::TRspFullHeartbeat>;
    using TCtxFullHeartbeatPtr = TIntrusivePtr<TCtxFullHeartbeat>;
    virtual void ProcessFullHeartbeat(TCtxFullHeartbeatPtr context) = 0;

    // COMPAT(gritukan)
    virtual void ProcessFullHeartbeat(
        NNodeTrackerServer::TNode* node,
        NDataNodeTrackerClient::NProto::TReqFullHeartbeat* request) = 0;

    using TCtxIncrementalHeartbeat = NRpc::TTypedServiceContext<
        NDataNodeTrackerClient::NProto::TReqIncrementalHeartbeat,
        NDataNodeTrackerClient::NProto::TRspIncrementalHeartbeat>;
    using TCtxIncrementalHeartbeatPtr = TIntrusivePtr<TCtxIncrementalHeartbeat>;
    virtual void ProcessIncrementalHeartbeat(TCtxIncrementalHeartbeatPtr context) = 0;

    // COMPAT(gritukan)
    virtual void ProcessIncrementalHeartbeat(
        NNodeTrackerServer::TNode* node,
        NDataNodeTrackerClient::NProto::TReqIncrementalHeartbeat* request,
        NDataNodeTrackerClient::NProto::TRspIncrementalHeartbeat* response) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDataNodeTracker)

////////////////////////////////////////////////////////////////////////////////

IDataNodeTrackerPtr CreateDataNodeTracker(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
