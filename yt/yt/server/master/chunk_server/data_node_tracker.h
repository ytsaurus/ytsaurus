#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/lib/hydra_common/entity_map.h>

#include <yt/yt/ytlib/data_node_tracker_client/proto/data_node_tracker_service.pb.h>

#include <yt/yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>

#include <yt/yt/core/actions/signal.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

using TChunkLocationUuidMap = THashMap<TChunkLocationUuid, TRealChunkLocation*>;

////////////////////////////////////////////////////////////////////////////////

struct IDataNodeTracker
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    using TCtxFullHeartbeat = NRpc::TTypedServiceContext<
        NDataNodeTrackerClient::NProto::TReqFullHeartbeat,
        NDataNodeTrackerClient::NProto::TRspFullHeartbeat>;
    using TCtxFullHeartbeatPtr = TIntrusivePtr<TCtxFullHeartbeat>;
    virtual void ProcessFullHeartbeat(TCtxFullHeartbeatPtr context) = 0;

    // COMPAT(gritukan)
    virtual void ProcessFullHeartbeat(
        NNodeTrackerServer::TNode* node,
        NDataNodeTrackerClient::NProto::TReqFullHeartbeat* request,
        NDataNodeTrackerClient::NProto::TRspFullHeartbeat* response) = 0;

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

    virtual void ValidateRegisterNode(
        const TString& address,
        NNodeTrackerClient::NProto::TReqRegisterNode* request) = 0;
    virtual void ProcessRegisterNode(
        NNodeTrackerServer::TNode* node,
        NNodeTrackerClient::NProto::TReqRegisterNode* request,
        NNodeTrackerClient::NProto::TRspRegisterNode* response) = 0;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(ChunkLocation, TRealChunkLocation);
    virtual TRealChunkLocation* FindChunkLocationByUuid(TChunkLocationUuid locationUuid) = 0;
    virtual TRealChunkLocation* GetChunkLocationByUuid(TChunkLocationUuid locationUuid) = 0;

    virtual const TChunkLocationUuidMap& ChunkLocationUuidMap() const = 0;
    virtual const TChunkLocationUuidMap& ChunkLocationUuidMapShard(int shardIndex) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IDataNodeTracker)

////////////////////////////////////////////////////////////////////////////////

IDataNodeTrackerPtr CreateDataNodeTracker(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
