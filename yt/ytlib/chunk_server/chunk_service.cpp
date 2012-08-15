#include "stdafx.h"
#include "chunk_service.h"
#include "node_statistics.h"
#include "node.h"
#include "node_authority.h"

#include <ytlib/misc/string.h>

#include <ytlib/actions/bind.h>

#include <ytlib/object_server/id.h>
#include <ytlib/object_server/object_manager.h>

#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/cell_master/meta_state_facade.h>

#include <ytlib/transaction_server/transaction_manager.h>

#include <ytlib/chunk_server/chunk_manager.h>

#include <ytlib/meta_state/rpc_helpers.h>

#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NChunkServer {

using namespace NRpc;
using namespace NMetaState;
using namespace NChunkHolder;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NChunkServer::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;
static NProfiling::TProfiler& Profiler = ChunkServerProfiler;

////////////////////////////////////////////////////////////////////////////////

TChunkService::TChunkService(TBootstrap* bootstrap)
    : TServiceBase(
        bootstrap->GetMetaStateFacade()->GetWrappedInvoker(),
        TChunkServiceProxy::GetServiceName(),
        ChunkServerLogger.GetCategory())
    , Bootstrap(bootstrap)
{
    YCHECK(bootstrap);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterNode));
    RegisterMethod(
        RPC_SERVICE_METHOD_DESC(FullHeartbeat)
            .SetHeavyRequest(true)
            .SetInvoker(bootstrap->GetMetaStateFacade()->GetWrappedInvoker(EStateThreadQueue::ChunkRefresh)));
    RegisterMethod(
        RPC_SERVICE_METHOD_DESC(IncrementalHeartbeat)
            .SetHeavyRequest(true));
}

void TChunkService::ValidateNodeId(TNodeId nodeId) const
{
    if (!Bootstrap->GetChunkManager()->FindNode(nodeId)) {
        ythrow TServiceException(EErrorCode::NoSuchNode) <<
            Sprintf("Invalid or expired node id %d", nodeId);
    }
}

void TChunkService::ValidateTransactionId(const TTransactionId& transactionId) const
{
    if (!Bootstrap->GetTransactionManager()->FindTransaction(transactionId)) {
        ythrow TServiceException(EErrorCode::NoSuchTransaction) << 
            Sprintf("No such transaction %s", ~transactionId.ToString());
    }
}

void TChunkService::CheckAuthorization(const Stroka& address) const
{
    auto nodeAuthority = Bootstrap->GetNodeAuthority();
    if (!nodeAuthority->IsAuthorized(address)) {
        ythrow TServiceException(TError(
            EErrorCode::NotAuthorized,
            Sprintf("Node %s is not authorized", ~address)));
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TChunkService, RegisterNode)
{
    UNUSED(response);

    auto metaStateFacade = Bootstrap->GetMetaStateFacade();
    auto chunkManager = Bootstrap->GetChunkManager();
    auto objectManager = Bootstrap->GetObjectManager();

    Stroka address = request->address();
    auto incarnationId = TIncarnationId::FromProto(request->incarnation_id());
    auto requestCellGuid = TGuid::FromProto(request->cell_guid());
    const auto& statistics = request->statistics();
    
    context->SetRequestInfo("Address: %s, IncarnationId: %s, CellGuid: %s, %s",
        ~address,
        ~incarnationId.ToString(),
        ~requestCellGuid.ToString(),
        ~ToString(statistics));

    if (!metaStateFacade->ValidateActiveLeader(context->GetUntypedContext()))
        return;

    auto expectedCellGuid = objectManager->GetCellGuid();
    if (!requestCellGuid.IsEmpty() && requestCellGuid != expectedCellGuid) {
        ythrow TServiceException(TError(
            NRpc::EErrorCode::PoisonPill,
            "Wrong cell guid reported by node %s: expected %s, received %s",
            ~address,
            ~expectedCellGuid.ToString(),
            ~requestCellGuid.ToString()));
    }

    CheckAuthorization(address);

    TMetaReqRegisterNode registerReq;
    registerReq.set_address(address);
    *registerReq.mutable_incarnation_id() = incarnationId.ToProto();
    *registerReq.mutable_statistics() = statistics;
    chunkManager
        ->CreateRegisterNodeMutation(registerReq)
        ->OnSuccess(BIND([=] (const TMetaRspRegisterNode& registerRsp) {
            TNodeId nodeId = registerRsp.node_id();
            context->Response().set_node_id(nodeId);
            *response->mutable_cell_guid() = expectedCellGuid.ToProto();
            context->SetResponseInfo("NodeId: %d", nodeId);
            context->Reply();
        }))
        ->OnError(CreateRpcErrorHandler(context->GetUntypedContext()))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TChunkService, FullHeartbeat)
{
    auto metaStateFacade = Bootstrap->GetMetaStateFacade();
    auto chunkManager = Bootstrap->GetChunkManager();

    auto nodeId = request->node_id();

    context->SetRequestInfo("NodeId: %d", nodeId);

    if (!metaStateFacade->ValidateActiveLeader(context->GetUntypedContext()))
        return;

    ValidateNodeId(nodeId);

    const auto* node = chunkManager->GetNode(nodeId);
    if (node->GetState() != ENodeState::Registered) {
        context->Reply(TError(
            EErrorCode::InvalidState,
            Sprintf("Cannot process a full heartbeat in %s state", ~node->GetState().ToString())));
        return;
    }
    CheckAuthorization(node->GetAddress());

    chunkManager
        ->CreateFullHeartbeatMutation(context)
        ->OnSuccess(CreateRpcSuccessHandler(context->GetUntypedContext()))
        ->OnError(CreateRpcErrorHandler(context->GetUntypedContext()))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TChunkService, IncrementalHeartbeat)
{
    auto metaStateFacade = Bootstrap->GetMetaStateFacade();
    auto chunkManager = Bootstrap->GetChunkManager();

    auto nodeId = request->node_id();

    context->SetRequestInfo("NodeId: %d");

    if (!metaStateFacade->ValidateActiveLeader(context->GetUntypedContext()))
        return;

    ValidateNodeId(nodeId);

    auto* node = chunkManager->GetNode(nodeId);
    if (node->GetState() != ENodeState::Online) {
        context->Reply(TError(
            EErrorCode::InvalidState,
            Sprintf("Cannot process an incremental heartbeat in %s state", ~node->GetState().ToString())));
        return;
    }
    CheckAuthorization(node->GetAddress());

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
        runningJobIds.insert(TJobId::FromProto(jobInfo.job_id()));
    }

    FOREACH (const auto& jobInfo, jobsToStop) {
        auto jobId = TJobId::FromProto(jobInfo.job_id());
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
        ->OnError(CreateRpcErrorHandler(context->GetUntypedContext()))
        ->Commit();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
