#pragma once

#include "public.h"

#include <ytlib/rpc/service.h>

#include <ytlib/node_tracker_client/node_tracker_service_proxy.h>
#include <ytlib/node_tracker_client/node_tracker_service.pb.h>

#include <server/cell_master/meta_state_service.h>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

class TNodeTrackerService
    : public NCellMaster::TMetaStateServiceBase
{
public:
    explicit TNodeTrackerService(
        TNodeTrackerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

private:
    typedef TNodeTrackerService TThis;

    TNodeTrackerConfigPtr Config;

    TRuntimeMethodInfoPtr FullHeartbeatMethodInfo;

    TNode* GetNode(TNodeId nodeId);

    void ValidateAuthorization(const Stroka& address);

    DECLARE_RPC_SERVICE_METHOD(NNodeTrackerClient::NProto, RegisterNode);
    DECLARE_RPC_SERVICE_METHOD(NNodeTrackerClient::NProto, FullHeartbeat);
    DECLARE_RPC_SERVICE_METHOD(NNodeTrackerClient::NProto, IncrementalHeartbeat);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
