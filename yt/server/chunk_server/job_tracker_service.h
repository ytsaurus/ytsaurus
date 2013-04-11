#pragma once

#include "public.h"

#include <ytlib/rpc/service.h>

#include <ytlib/job_tracker_client/job_tracker_service.pb.h>

#include <server/node_tracker_server/public.h>

#include <server/cell_master/meta_state_service.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TJobTrackerService
    : public NCellMaster::TMetaStateServiceBase
{
public:
    explicit TJobTrackerService(NCellMaster::TBootstrap* bootstrap);

private:
    typedef TJobTrackerService TThis;

    NNodeTrackerServer::TNode* GetNode(NNodeTrackerServer::TNodeId nodeId);

    DECLARE_RPC_SERVICE_METHOD(NJobTrackerClient::NProto, Heartbeat);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
