#include "stdafx.h"
#include "node_tracker_service.h"
#include "node.h"
#include "node_tracker.h"
#include "private.h"
#include "config.h"

#include <ytlib/node_tracker_client/node_tracker_service_proxy.h>

#include <server/hydra/rpc_helpers.h>

#include <server/object_server/object_manager.h>

#include <server/chunk_server/chunk_manager.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/hydra_facade.h>
#include <server/cell_master/master_hydra_service.h>

namespace NYT {
namespace NNodeTrackerServer {

using namespace NHydra;
using namespace NCellMaster;
using namespace NNodeTrackerClient;
using namespace NChunkServer;

using NNodeTrackerClient::NProto::TChunkAddInfo;
using NNodeTrackerClient::NProto::TChunkRemoveInfo;

////////////////////////////////////////////////////////////////////////////////

class TNodeTrackerService
    : public NCellMaster::TMasterHydraServiceBase
{
public:
    explicit TNodeTrackerService(
        TNodeTrackerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TNodeTrackerServiceProxy::GetServiceName(),
            NodeTrackerServerLogger)
        , Config(config)
        {
            RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterNode));
            FullHeartbeatMethodInfo = RegisterMethod(RPC_SERVICE_METHOD_DESC(FullHeartbeat)
                .SetRequestHeavy(true)
                .SetInvoker(bootstrap->GetHydraFacade()->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::Heartbeat)));
            RegisterMethod(RPC_SERVICE_METHOD_DESC(IncrementalHeartbeat)
                .SetRequestHeavy(true));
        }


private:
    TNodeTrackerConfigPtr Config;

    TRuntimeMethodInfoPtr FullHeartbeatMethodInfo;


    DECLARE_RPC_SERVICE_METHOD(NNodeTrackerClient::NProto, RegisterNode)
    {
        UNUSED(response);

        ValidateActiveLeader();

        auto descriptor = FromProto<TNodeDescriptor>(request->node_descriptor());
        auto requestCellId = FromProto<TGuid>(request->cell_id());
        const auto& statistics = request->statistics();
        const auto& address = descriptor.GetDefaultAddress();

        context->SetRequestInfo("Address: %v, CellId: %v, %v",
            address,
            requestCellId,
            statistics);

        auto expectedCellId = Bootstrap->GetCellId();
        if (!requestCellId.IsEmpty() && requestCellId != expectedCellId) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::PoisonPill,
                "Wrong cell id reported by node %v: expected %v, received %v",
                address,
                expectedCellId,
                requestCellId);
        }

        auto nodeTracker = Bootstrap->GetNodeTracker();
        int fullHeartbeatQueueSize = FullHeartbeatMethodInfo->QueueSizeCounter.Current;
        int registeredNodeCount = nodeTracker->GetRegisteredNodeCount();
        if (fullHeartbeatQueueSize + registeredNodeCount > Config->MaxFullHeartbeatQueueSizeLimit) {
            context->Reply(TError(
                NRpc::EErrorCode::Unavailable,
                "Full heartbeat throttling is active")
                           << TErrorAttribute("queue_size", fullHeartbeatQueueSize)
                           << TErrorAttribute("registered_node_count", registeredNodeCount)
                           << TErrorAttribute("limit", Config->MaxFullHeartbeatQueueSizeLimit));
            return;
        }


        auto config = nodeTracker->FindNodeConfigByAddress(address);
        if (config && config->Banned) {
            THROW_ERROR_EXCEPTION("Node %v is banned", address);
        }

        nodeTracker
            ->CreateRegisterNodeMutation(*request)
            ->Commit()
            .Subscribe(CreateRpcResponseHandler(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NNodeTrackerClient::NProto, FullHeartbeat)
    {
        ValidateActiveLeader();

        auto nodeId = request->node_id();
        const auto& statistics = request->statistics();

        auto nodeTracker = Bootstrap->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        context->SetRequestInfo("NodeId: %v, Address: %v, %v",
            nodeId,
            node->GetAddress(),
            statistics);

        if (node->GetState() != ENodeState::Registered) {
            context->Reply(TError(
                NNodeTrackerClient::EErrorCode::InvalidState,
                "Cannot process a full heartbeat in %Qlv state",
                node->GetState()));
            return;
        }

        nodeTracker
            ->CreateFullHeartbeatMutation(context)
            ->Commit()
            .Subscribe(CreateRpcResponseHandler(context));
        ;
    }

    DECLARE_RPC_SERVICE_METHOD(NNodeTrackerClient::NProto, IncrementalHeartbeat)
    {
        ValidateActiveLeader();

        auto nodeId = request->node_id();
        const auto& statistics = request->statistics();

        auto nodeTracker = Bootstrap->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        context->SetRequestInfo("NodeId: %v, Address: %v, %v",
            nodeId,
            node->GetAddress(),
            statistics);

        if (node->GetState() != ENodeState::Online) {
            context->Reply(TError(
                NNodeTrackerClient::EErrorCode::InvalidState,
                "Cannot process an incremental heartbeat in %lv state",
                node->GetState()));
            return;
        }

        nodeTracker
            ->CreateIncrementalHeartbeatMutation(context)
            ->Commit()
            .Subscribe(CreateRpcResponseHandler(context));
    }

};

NRpc::IServicePtr CreateNodeTrackerService(
    TNodeTrackerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
{
    return New<TNodeTrackerService>(config, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
