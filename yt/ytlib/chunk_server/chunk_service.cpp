#include "stdafx.h"
#include "chunk_service.h"
#include "holder_statistics.h"
#include "holder.h"
#include "holder_authority.h"

#include <ytlib/misc/string.h>

#include <ytlib/actions/bind.h>

#include <ytlib/object_server/id.h>
#include <ytlib/object_server/object_manager.h>

#include <ytlib/cell_master/bootstrap.h>

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

NLog::TLogger Logger("ChunkServer");
NProfiling::TProfiler Profiler("/chunk_server");

////////////////////////////////////////////////////////////////////////////////

TChunkService::TChunkService(TBootstrap* bootstrap)
    : TMetaStateServiceBase(
        bootstrap,
        TChunkServiceProxy::GetServiceName(),
        ChunkServerLogger.GetCategory())
{
    YASSERT(bootstrap);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterNode));
    RegisterMethod(
        RPC_SERVICE_METHOD_DESC(FullHeartbeat)
            .SetHeavyRequest(true),
        bootstrap->GetStateInvoker(EStateThreadQueue::ChunkRefresh));
    RegisterMethod(
        RPC_SERVICE_METHOD_DESC(IncrementalHeartbeat)
            .SetHeavyRequest(true));
}

 void TChunkService::ValidateNodeId(TNodeId nodeId)
{
    if (!Bootstrap->GetChunkManager()->FindNode(nodeId)) {
        ythrow TServiceException(EErrorCode::NoSuchHolder) <<
            Sprintf("Invalid or expired node id %d", nodeId);
    }
}

void TChunkService::ValidateTransactionId(const TTransactionId& transactionId)
{
    if (!Bootstrap->GetTransactionManager()->FindTransaction(transactionId)) {
        ythrow TServiceException(EErrorCode::NoSuchTransaction) << 
            Sprintf("No such transaction %s", ~transactionId.ToString());
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TChunkService, RegisterNode)
{
    UNUSED(response);

    Stroka address = request->address();
    auto incarnationId = TIncarnationId::FromProto(request->incarnation_id());
    auto requestCellGuid = TGuid::FromProto(request->cell_guid());
    const auto& statistics = request->statistics();
    
    context->SetRequestInfo("Address: %s, IncarnationId: %s, CellGuid: %s, %s",
        ~address,
        ~incarnationId.ToString(),
        ~requestCellGuid.ToString(),
        ~ToString(statistics));

    auto chunkManager = Bootstrap->GetChunkManager();
    auto objectManager = Bootstrap->GetObjectManager();

    auto expectedCellGuid = objectManager->GetCellGuid();
    if (!requestCellGuid.IsEmpty() && requestCellGuid != expectedCellGuid) {
        ythrow TServiceException(TError(
            NRpc::EErrorCode::PoisonPill,
            "Wrong cell guid reported by node %s: expected %s, received %s",
            ~address,
            ~expectedCellGuid.ToString(),
            ~requestCellGuid.ToString()));
    }

    CheckHolderAuthorization(address);

    TMetaReqRegisterNode message;
    message.set_address(address);
    *message.mutable_incarnation_id() = incarnationId.ToProto();
    *message.mutable_statistics() = statistics;
    chunkManager
        ->CreateRegisterNodeMutation(message)
        ->OnSuccess(BIND([=] (const TMetaRspRegisterNode& response) {
            TNodeId nodeId = response.node_id();
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
    auto nodeId = request->node_id();

    context->SetRequestInfo("NodeId: %d", nodeId);

    ValidateNodeId(nodeId);

    auto chunkManager = Bootstrap->GetChunkManager();
    const auto* holder = chunkManager->GetNode(nodeId);
    if (holder->GetState() != ENodeState::Registered) {
        context->Reply(TError(
            EErrorCode::InvalidState,
            Sprintf("Cannot process a full heartbeat in %s state", ~holder->GetState().ToString())));
        return;
    }
    CheckAuthorization(holder->GetAddress());

    chunkManager
        ->CreateFullHeartbeatMutation(context)
        ->OnSuccess(CreateRpcSuccessHandler(context->GetUntypedContext()))
        ->OnError(CreateRpcErrorHandler(context->GetUntypedContext()))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TChunkService, IncrementalHeartbeat)
{
    auto nodeId = request->node_id();

    context->SetRequestInfo("NodeId: %d");

    ValidateNodeId(nodeId);

    auto chunkManager = Bootstrap->GetChunkManager();
    auto* holder = chunkManager->GetNode(nodeId);
    if (holder->GetState() != ENodeState::Online) {
        context->Reply(TError(
            EErrorCode::InvalidState,
            Sprintf("Cannot process an incremental heartbeat in %s state", ~holder->GetState().ToString())));
        return;
    }
    CheckAuthorization(holder->GetAddress());

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
        holder,
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

void TChunkService::CheckAuthorization(const Stroka& address) const
{
    auto holderAuthority = Bootstrap->GetNodeAuthority();
    if (!holderAuthority->IsAuthorized(address)) {
        ythrow TServiceException(TError(
            EErrorCode::NotAuthorized,
            Sprintf("Node %s is not authorized", ~address)));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
