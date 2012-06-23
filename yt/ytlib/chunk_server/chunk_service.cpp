#include "stdafx.h"
#include "chunk_service.h"

#include "holder_statistics.h"
#include "holder.h"
#include "holder_authority.h"

#include <ytlib/misc/string.h>
#include <ytlib/actions/bind.h>
#include <ytlib/object_server/id.h>
#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/transaction_server/transaction_manager.h>
#include <ytlib/chunk_server/chunk_manager.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NChunkServer {

using namespace NRpc;
using namespace NMetaState;
using namespace NChunkHolder;
using namespace NProto;
using namespace NObjectServer;
using namespace NCellMaster;

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

    RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterHolder));
    RegisterMethod(
        RPC_SERVICE_METHOD_DESC(FullHeartbeat),
        ~bootstrap->GetStateInvoker(EStateThreadQueue::ChunkRefresh));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(IncrementalHeartbeat));
}

 void TChunkService::ValidateHolderId(THolderId holderId)
{
    if (!Bootstrap->GetChunkManager()->FindHolder(holderId)) {
        ythrow TServiceException(EErrorCode::NoSuchHolder) <<
            Sprintf("Invalid or expired holder id %d", holderId);
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

DEFINE_RPC_SERVICE_METHOD(TChunkService, RegisterHolder)
{
    UNUSED(response);

    Stroka address = request->address();
    auto incarnationId = TIncarnationId::FromProto(request->incarnation_id());
    const auto& statistics = request->statistics();
    
    context->SetRequestInfo("Address: %s, IncarnationId: %s, %s",
        ~address,
        ~incarnationId.ToString(),
        ~ToString(statistics));

    CheckHolderAuthorization(address);

    auto chunkManager = Bootstrap->GetChunkManager();

    TMsgRegisterHolder message;
    message.set_address(address);
    *message.mutable_incarnation_id() = incarnationId.ToProto();
    *message.mutable_statistics() = statistics;
    chunkManager
        ->InitiateRegisterHolder(message)
        ->OnSuccess(BIND([=] (THolderId id) {
            response->set_holder_id(id);
            context->SetResponseInfo("HolderId: %d", id);
            context->Reply();
        }))
        ->OnError(CreateErrorHandler(context))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TChunkService, FullHeartbeat)
{
    auto holderId = request->holder_id();

    context->SetRequestInfo("HolderId: %d", holderId);

    ValidateHolderId(holderId);

    auto chunkManager = Bootstrap->GetChunkManager();
    const auto* holder = chunkManager->GetHolder(holderId);
    if (holder->GetState() != EHolderState::Registered) {
        context->Reply(TError(
            EErrorCode::InvalidState,
            Sprintf("Cannot process a full heartbeat in %s state", ~holder->GetState().ToString())));
        return;
    }
    CheckHolderAuthorization(holder->GetAddress());

    chunkManager
        ->InitiateFullHeartbeat(context)
        ->OnSuccess(BIND([=] (TVoid) {
            context->Reply();
        }))
        ->OnError(CreateErrorHandler(context))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TChunkService, IncrementalHeartbeat)
{
    auto holderId = request->holder_id();

    context->SetRequestInfo("HolderId: %d");

    ValidateHolderId(holderId);

    auto chunkManager = Bootstrap->GetChunkManager();
    auto* holder = chunkManager->GetHolder(holderId);
    if (holder->GetState() != EHolderState::Online) {
        context->Reply(TError(
            EErrorCode::InvalidState,
            Sprintf("Cannot process an incremental heartbeat in %s state", ~holder->GetState().ToString())));
        return;
    }
    CheckHolderAuthorization(holder->GetAddress());

    TMsgIncrementalHeartbeat heartbeatMsg;
    heartbeatMsg.set_holder_id(holderId);
    *heartbeatMsg.mutable_statistics() = request->statistics();
    heartbeatMsg.mutable_added_chunks()->MergeFrom(request->added_chunks());
    heartbeatMsg.mutable_removed_chunks()->MergeFrom(request->removed_chunks());

    chunkManager
        ->InitiateIncrementalHeartbeat(heartbeatMsg)
        ->Commit();

    yvector<TJobInfo> runningJobs(request->jobs().begin(), request->jobs().end());
    yvector<TJobStartInfo> jobsToStart;
    yvector<TJobStopInfo> jobsToStop;
    chunkManager->ScheduleJobs(
        holder,
        runningJobs,
        &jobsToStart,
        &jobsToStop);

    TMsgUpdateJobs updateJobsMsg;
    updateJobsMsg.set_holder_id(holderId);

    FOREACH (const auto& jobInfo, jobsToStart) {
        *response->add_jobs_to_start() = jobInfo;
        *updateJobsMsg.add_started_jobs() = jobInfo;
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
        *updateJobsMsg.add_stopped_jobs() = jobInfo;
    }

    chunkManager
        ->InitiateUpdateJobs(updateJobsMsg)
        ->OnSuccess(BIND([=] (TVoid) {
            context->SetResponseInfo("JobsToStart: %d, JobsToStop: %d",
                static_cast<int>(response->jobs_to_start_size()),
                static_cast<int>(response->jobs_to_stop_size()));
            context->Reply();
        }))
        ->OnError(CreateErrorHandler(context))
        ->Commit();
}

void TChunkService::CheckHolderAuthorization(const Stroka &address) const
{
    auto holderAuthority = Bootstrap->GetHolderAuthority();
    if (!holderAuthority->IsHolderAuthorized(address)) {
        ythrow TServiceException(TError(
            EErrorCode::NotAuthorized,
            Sprintf("Holder %s is not authorized", ~address.Quote())));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
