#include "stdafx.h"
#include "chunk_service.h"
#include "node_statistics.h"
#include "node.h"
#include "node_authority.h"
#include "private.h"
#include "config.h"

#include <ytlib/misc/string.h>

#include <ytlib/meta_state/rpc_helpers.h>

#include <ytlib/profiling/profiler.h>

#include <server/object_server/object_manager.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>

#include <server/transaction_server/transaction_manager.h>

#include <server/chunk_server/chunk_manager.h>

namespace NYT {
namespace NChunkServer {

using namespace NRpc;
using namespace NMetaState;
using namespace NChunkClient;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NChunkServer::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;
static NProfiling::TProfiler& Profiler = ChunkServerProfiler;

////////////////////////////////////////////////////////////////////////////////

TChunkService::TChunkService(
    TChunkManagerConfigPtr config,
    TBootstrap* bootstrap)
    : TMetaStateServiceBase(
        bootstrap,
        TChunkServiceProxy::GetServiceName(),
        ChunkServerLogger.GetCategory())
    , Config(config)
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterNode));
    FullHeartbeatMethodInfo = RegisterMethod(
        RPC_SERVICE_METHOD_DESC(FullHeartbeat)
            .SetRequestHeavy(true)
            .SetMaxQueueSize(Config->FullHeartbeatQueueHardLimit)
            .SetInvoker(bootstrap->GetMetaStateFacade()->GetGuardedInvoker(EStateThreadQueue::ChunkMaintenance)));
    RegisterMethod(
        RPC_SERVICE_METHOD_DESC(IncrementalHeartbeat)
            .SetRequestHeavy(true));
}

TDataNode* TChunkService::GetNode(TNodeId nodeId)
{
    auto* node = Bootstrap->GetChunkManager()->FindNode(nodeId);
    if (!node) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::NoSuchNode,
            "Invalid or expired node id: %d",
            nodeId);
    }
    return node;
}

void TChunkService::ValidateAuthorization(const Stroka& address)
{
    auto nodeAuthority = Bootstrap->GetNodeAuthority();
    if (!nodeAuthority->IsAuthorized(address)) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::NotAuthorized,
            "Node is not authorized: %s",
            ~address);
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TChunkService, RegisterNode)
{
    UNUSED(response);

    ValidateActiveLeader();

    auto chunkManager = Bootstrap->GetChunkManager();
    auto objectManager = Bootstrap->GetObjectManager();

    auto descriptor = FromProto<TNodeDescriptor>(request->node_descriptor());
    auto incarnationId = FromProto<TIncarnationId>(request->incarnation_id());
    auto requestCellGuid = FromProto<TGuid>(request->cell_guid());
    const auto& statistics = request->statistics();
    const auto& address = descriptor.Address;

    context->SetRequestInfo("Address: %s, IncarnationId: %s, CellGuid: %s, %s",
        ~address,
        ~ToString(incarnationId),
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
    int registeredNodeCount = chunkManager->GetRegisteredNodeCount();
    if (fullHeartbeatQueueSize + registeredNodeCount > Config->FullHeartbeatQueueSoftLimit) {
        context->Reply(TError(
            NRpc::EErrorCode::Unavailable,
            "Full heartbeat throttling is active")
            << TErrorAttribute("queue_size", fullHeartbeatQueueSize)
            << TErrorAttribute("registered_node_count", registeredNodeCount)
            << TErrorAttribute("limit", Config->FullHeartbeatQueueSoftLimit));
        return;
    }

    TMetaReqRegisterNode registerReq;
    ToProto(registerReq.mutable_node_descriptor(), descriptor);
    ToProto(registerReq.mutable_incarnation_id(), incarnationId);
    *registerReq.mutable_statistics() = statistics;
    chunkManager
        ->CreateRegisterNodeMutation(registerReq)
        ->OnSuccess(BIND([=] (const TMetaRspRegisterNode& registerRsp) {
            TNodeId nodeId = registerRsp.node_id();
            context->Response().set_node_id(nodeId);
            ToProto(response->mutable_cell_guid(), expectedCellGuid);
            context->SetResponseInfo("NodeId: %d", nodeId);
            context->Reply();
        }))
        ->OnError(CreateRpcErrorHandler(context))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TChunkService, FullHeartbeat)
{
    ValidateActiveLeader();

    auto chunkManager = Bootstrap->GetChunkManager();

    auto nodeId = request->node_id();

    context->SetRequestInfo("NodeId: %d", nodeId);

    const auto* node = GetNode(nodeId);
    if (node->GetState() != ENodeState::Registered) {
        context->Reply(TError(
            EErrorCode::InvalidState,
            Sprintf("Cannot process a full heartbeat in %s state", ~node->GetState().ToString())));
        return;
    }
    ValidateAuthorization(node->GetAddress());

    chunkManager
        ->CreateFullHeartbeatMutation(context)
        ->OnSuccess(CreateRpcSuccessHandler(context))
        ->OnError(CreateRpcErrorHandler(context))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TChunkService, IncrementalHeartbeat)
{
    ValidateActiveLeader();

    auto chunkManager = Bootstrap->GetChunkManager();

    auto nodeId = request->node_id();

    context->SetRequestInfo("NodeId: %d", nodeId);

    auto* node = GetNode(nodeId);
    if (node->GetState() != ENodeState::Online) {
        context->Reply(TError(
            EErrorCode::InvalidState,
            Sprintf("Cannot process an incremental heartbeat in %s state", ~node->GetState().ToString())));
        return;
    }
    ValidateAuthorization(node->GetAddress());

    TMetaReqIncrementalHeartbeat heartbeatReq;
    heartbeatReq.set_node_id(nodeId);
    *heartbeatReq.mutable_statistics() = request->statistics();
    heartbeatReq.mutable_added_chunks()->MergeFrom(request->added_chunks());
    heartbeatReq.mutable_removed_chunks()->MergeFrom(request->removed_chunks());

    chunkManager
        ->CreateIncrementalHeartbeatMutation(heartbeatReq)
        ->Commit();

    std::vector<TJobInfo> runningJobs(request->jobs().begin(), request->jobs().end());
    std::vector<TJobStartInfo> jobsToStart;
    std::vector<TJobStopInfo> jobsToStop;
    chunkManager->ScheduleJobs(
        node,
        runningJobs,
        &jobsToStart,
        &jobsToStop);

    TMetaReqUpdateJobs updateJobsReq;
    updateJobsReq.set_node_id(nodeId);

    FOREACH (const auto& jobInfo, jobsToStart) {
        *response->add_jobs_to_start() = jobInfo;
        *updateJobsReq.add_started_jobs() = jobInfo;
    }

    yhash_set<TJobId> runningJobIds;
    FOREACH (const auto& jobInfo, runningJobs) {
        runningJobIds.insert(FromProto<TJobId>(jobInfo.job_id()));
    }

    FOREACH (const auto& jobInfo, jobsToStop) {
        auto jobId = FromProto<TJobId>(jobInfo.job_id());
        if (runningJobIds.find(jobId) != runningJobIds.end()) {
            *response->add_jobs_to_stop() = jobInfo;
        }
        *updateJobsReq.add_stopped_jobs() = jobInfo;
    }

    chunkManager
        ->CreateUpdateJobsMutation(updateJobsReq)
        ->OnSuccess(BIND([=] () {
            context->SetResponseInfo("JobsToStart: %d, JobsToStop: %d",
                static_cast<int>(response->jobs_to_start_size()),
                static_cast<int>(response->jobs_to_stop_size()));
            context->Reply();
        }))
        ->OnError(CreateRpcErrorHandler(context))
        ->Commit();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
