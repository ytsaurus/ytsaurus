#include "stdafx.h"
#include "node_tracker_service.h"
#include "node.h"
#include "node_authority.h"
#include "node_tracker.h"
#include "private.h"
#include "config.h"

#include <ytlib/meta_state/rpc_helpers.h>

#include <ytlib/node_tracker_client/node_tracker_service_proxy.h>

#include <server/object_server/object_manager.h>

#include <server/chunk_server/chunk_manager.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>

namespace NYT {
namespace NNodeTrackerServer {

using namespace NMetaState;
using namespace NCellMaster;
using namespace NNodeTrackerClient;
using namespace NChunkServer;

using NNodeTrackerClient::NProto::TChunkAddInfo;
using NNodeTrackerClient::NProto::TChunkRemoveInfo;

////////////////////////////////////////////////////////////////////////////////

TNodeTrackerService::TNodeTrackerService(
    TNodeTrackerConfigPtr config,
    TBootstrap* bootstrap)
    : TMetaStateServiceBase(
        bootstrap,
        TNodeTrackerServiceProxy::GetServiceName(),
        NodeTrackerServerLogger.GetCategory())
    , Config(config)
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterNode));
    FullHeartbeatMethodInfo = RegisterMethod(
        RPC_SERVICE_METHOD_DESC(FullHeartbeat)
            .SetRequestHeavy(true)
            .SetInvoker(bootstrap->GetMetaStateFacade()->GetGuardedInvoker(EStateThreadQueue::Heartbeat)));
    RegisterMethod(
        RPC_SERVICE_METHOD_DESC(IncrementalHeartbeat)
            .SetRequestHeavy(true));
}

TNode* TNodeTrackerService::GetNode(TNodeId nodeId)
{
    auto nodeTracker = Bootstrap->GetNodeTracker();
    auto* node = nodeTracker->FindNode(nodeId);
    if (!node) {
        THROW_ERROR_EXCEPTION(
            NNodeTrackerClient::EErrorCode::NoSuchNode,
            "Invalid or expired node id: %d",
            nodeId);
    }
    return node;
}

void TNodeTrackerService::ValidateAuthorization(const Stroka& address)
{
    auto nodeAuthority = Bootstrap->GetNodeAuthority();
    if (!nodeAuthority->IsAuthorized(address)) {
        THROW_ERROR_EXCEPTION(
            NNodeTrackerClient::EErrorCode::NotAuthorized,
            "Node is not authorized: %s",
            ~address);
    }
}

DEFINE_RPC_SERVICE_METHOD(TNodeTrackerService, RegisterNode)
{
    UNUSED(response);

    ValidateActiveLeader();

    auto nodeTracker = Bootstrap->GetNodeTracker();
    auto objectManager = Bootstrap->GetObjectManager();

    auto descriptor = FromProto<TNodeDescriptor>(request->node_descriptor());
    auto requestCellGuid = FromProto<TGuid>(request->cell_guid());
    const auto& statistics = request->statistics();
    const auto& address = descriptor.Address;

    context->SetRequestInfo("Address: %s, CellGuid: %s, %s",
        ~address,
        ~ToString(requestCellGuid),
        ~ToString(statistics));

    auto expectedCellGuid = objectManager->GetCellGuid();
    if (!requestCellGuid.IsEmpty() && requestCellGuid != expectedCellGuid) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::PoisonPill,
            "Wrong cell guid reported by node %s: expected %s, received %s",
            ~address,
            ~ToString(expectedCellGuid),
            ~ToString(requestCellGuid));
    }

    ValidateAuthorization(address);

    int fullHeartbeatQueueSize = FullHeartbeatMethodInfo->QueueSizeCounter.Current;
    int registeredNodeCount = nodeTracker->GetRegisteredNodeCount();
    if (fullHeartbeatQueueSize + registeredNodeCount > Config->FullHeartbeatQueueSizeLimit) {
        context->Reply(TError(
            NRpc::EErrorCode::Unavailable,
            "Full heartbeat throttling is active")
            << TErrorAttribute("queue_size", fullHeartbeatQueueSize)
            << TErrorAttribute("registered_node_count", registeredNodeCount)
            << TErrorAttribute("limit", Config->FullHeartbeatQueueSizeLimit));
        return;
    }

    NProto::TMetaReqRegisterNode registerReq;
    ToProto(registerReq.mutable_node_descriptor(), descriptor);
    *registerReq.mutable_statistics() = statistics;
    nodeTracker
        ->CreateRegisterNodeMutation(registerReq)
        ->OnSuccess(BIND([=] (const NProto::TMetaRspRegisterNode& registerRsp) {
            auto nodeId = registerRsp.node_id();
            context->Response().set_node_id(nodeId);
            ToProto(response->mutable_cell_guid(), expectedCellGuid);
            context->SetResponseInfo("NodeId: %d", nodeId);
            context->Reply();
        }))
        ->OnError(CreateRpcErrorHandler(context))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TNodeTrackerService, FullHeartbeat)
{
    ValidateActiveLeader();

    auto nodeId = request->node_id();

    context->SetRequestInfo("NodeId: %d", nodeId);

    const auto* node = GetNode(nodeId);
    if (node->GetState() != ENodeState::Registered) {
        context->Reply(TError(
            NNodeTrackerClient::EErrorCode::InvalidState,
            "Cannot process a full heartbeat in %s state",
            ~FormatEnum(node->GetState()).Quote()));
        return;
    }
    ValidateAuthorization(node->GetAddress());

    auto nodeTracker = Bootstrap->GetNodeTracker();
    nodeTracker
        ->CreateFullHeartbeatMutation(context)
        ->OnSuccess(CreateRpcSuccessHandler(context))
        ->OnError(CreateRpcErrorHandler(context))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TNodeTrackerService, IncrementalHeartbeat)
{
    ValidateActiveLeader();

    auto nodeId = request->node_id();

    context->SetRequestInfo("NodeId: %d", nodeId);

    auto* node = GetNode(nodeId);
    if (node->GetState() != ENodeState::Online) {
        context->Reply(TError(
            NNodeTrackerClient::EErrorCode::InvalidState,
            "Cannot process an incremental heartbeat in %s state",
            ~FormatEnum(node->GetState())));
        return;
    }

    ValidateAuthorization(node->GetAddress());

    NProto::TMetaReqIncrementalHeartbeat heartbeatReq;
    heartbeatReq.set_node_id(nodeId);
    *heartbeatReq.mutable_statistics() = request->statistics();
    heartbeatReq.mutable_added_chunks()->MergeFrom(request->added_chunks());
    heartbeatReq.mutable_removed_chunks()->MergeFrom(request->removed_chunks());

    auto nodeTracker = Bootstrap->GetNodeTracker();
    nodeTracker
        ->CreateIncrementalHeartbeatMutation(heartbeatReq)
        ->OnSuccess(CreateRpcSuccessHandler(context))
        ->OnError(CreateRpcErrorHandler(context))
        ->Commit();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
