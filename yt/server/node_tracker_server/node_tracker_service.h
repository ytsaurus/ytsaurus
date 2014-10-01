#pragma once

#include "public.h"

#include <core/rpc/service.h>

#include <ytlib/node_tracker_client/node_tracker_service.pb.h>

#include <server/cell_master/master_hydra_service.h>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

class TNodeTrackerService
    : public NCellMaster::TMasterHydraServiceBase
{
public:
    explicit TNodeTrackerService(
        TNodeTrackerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

private:
    typedef TNodeTrackerService TThis;

    TNodeTrackerConfigPtr Config;

    TRuntimeMethodInfoPtr FullHeartbeatMethodInfo;

    DECLARE_RPC_SERVICE_METHOD(NNodeTrackerClient::NProto, RegisterNode);
    DECLARE_RPC_SERVICE_METHOD(NNodeTrackerClient::NProto, FullHeartbeat);
    DECLARE_RPC_SERVICE_METHOD(NNodeTrackerClient::NProto, IncrementalHeartbeat);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
