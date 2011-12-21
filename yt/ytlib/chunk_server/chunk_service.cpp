#include "stdafx.h"
#include "chunk_service.h"

#include "../misc/string.h"

namespace NYT {
namespace NChunkServer {

using namespace NRpc;
using namespace NMetaState;
using namespace NChunkClient;
using namespace NChunkHolder;
using namespace NTransactionServer;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkService::TChunkService(
    NMetaState::IMetaStateManager* metaStateManager,
    TChunkManager* chunkManager,
    NTransactionServer::TTransactionManager* transactionManager,
    NRpc::IRpcServer* server)
    : TMetaStateServiceBase(
        metaStateManager,
        TChunkServiceProxy::GetServiceName(),
        ChunkServerLogger.GetCategory())
    , ChunkManager(chunkManager)
    , TransactionManager(transactionManager)
{
    YASSERT(chunkManager);
    YASSERT(server);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterHolder));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(HolderHeartbeat));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(AllocateChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(ConfirmChunks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(FindChunk));

    server->RegisterService(this);
}

 void TChunkService::ValidateHolderId(THolderId holderId)
{
    const auto* holder = ChunkManager->FindHolder(holderId);
    if (!holder) {
        ythrow TServiceException(EErrorCode::NoSuchHolder) <<
            Sprintf("Invalid or expired holder id (HolderId: %d)", holderId);
    }
}

void TChunkService::ValidateChunkId(const TChunkId& chunkId)
{
    const auto* chunk = ChunkManager->FindChunk(chunkId);
    if (!chunk) {
        ythrow TServiceException(EErrorCode::NoSuchChunk) <<
            Sprintf("No such chunk (ChunkId: %s)", ~chunkId.ToString());
    }
}

void TChunkService::ValidateTransactionId(const TTransactionId& transactionId)
{
    if (TransactionManager->FindTransaction(transactionId) == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchTransaction) << 
            Sprintf("No such transaction (TransactionId: %s)", ~transactionId.ToString());
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TChunkService, RegisterHolder)
{
    UNUSED(response);

    Stroka address = request->address();
    auto statistics = NChunkHolder::THolderStatistics::FromProto(request->statistics());
    
    context->SetRequestInfo("Address: %s, %s",
        ~address,
        ~statistics.ToString());

    ValidateLeader();

    ChunkManager
        ->InitiateRegisterHolder(address, statistics)
        ->OnSuccess(~FromFunctor([=] (THolderId id)
            {
                response->set_holderid(id);
                context->SetResponseInfo("HolderId: %d", id);
                context->Reply();
            }))
        ->OnError(~CreateErrorHandler(~context))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TChunkService, HolderHeartbeat)
{
    UNUSED(response);

    auto holderId = request->holderid();

    context->SetRequestInfo("HolderId: %d", holderId);

    ValidateLeader();
    ValidateHolderId(holderId);

    const auto& holder = ChunkManager->GetHolder(holderId);

    NProto::TMsgHeartbeatRequest requestMessage;
    requestMessage.set_holderid(holderId);
    *requestMessage.mutable_statistics() = request->statistics();
    requestMessage.mutable_addedchunks()->MergeFrom(request->addedchunks());
    requestMessage.mutable_removedchunks()->MergeFrom(request->removedchunks());

    ChunkManager
        ->InitiateHeartbeatRequest(requestMessage)
        ->Commit();

    yvector<NProto::TReqHolderHeartbeat::TJobInfo> runningJobs;
    runningJobs.reserve(request->jobs_size());
    FOREACH(const auto& jobInfo, request->jobs()) {
        auto jobId = TJobId::FromProto(jobInfo.jobid());
        const TJob* job = ChunkManager->FindJob(jobId);
        if (!job) {
            LOG_INFO("Stopping unknown or obsolete job (JobId: %s, Address: %s, HolderId: %d)",
                ~jobId.ToString(),
                ~holder.GetAddress(),
                holder.GetId());
            response->add_jobstostop(jobId.ToProto());
        } else {
            runningJobs.push_back(jobInfo);
        }
    }

    yvector<NProto::TRspHolderHeartbeat::TJobStartInfo> jobsToStart;
    yvector<TJobId> jobsToStop;
    ChunkManager->RunJobControl(
        holder,
        runningJobs,
        &jobsToStart,
        &jobsToStop);

    NProto::TMsgHeartbeatResponse responseMessage;
    responseMessage.set_holderid(holderId);

    FOREACH (const auto& jobInfo, jobsToStart) {
        *response->add_jobstostart() = jobInfo;
        *responseMessage.add_startedjobs() = jobInfo;
    }

    FOREACH (const auto& jobId, jobsToStop) {
        auto protoJobId = jobId.ToProto();
        response->add_jobstostop(protoJobId);
        responseMessage.add_stoppedjobs(protoJobId);
    }

    ChunkManager
        ->InitiateHeartbeatResponse(responseMessage)
        ->OnSuccess(~FromFunctor([=] (TVoid)
            {
                context->SetResponseInfo("JobsToStart: %d, JobsToStop: %d",
                    static_cast<int>(response->jobstostart_size()),
                    static_cast<int>(response->jobstostop_size()));

                context->Reply();
            }))
        ->OnError(~CreateErrorHandler(~context))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TChunkService, AllocateChunk)
{
    auto transactionId = TTransactionId::FromProto(request->transactionid());
    int replicaCount = request->replicacount();

    context->SetRequestInfo("TransactionId: %s, ReplicaCount: %d",
        ~transactionId.ToString(),
        replicaCount);

    ValidateLeader();
    ValidateTransactionId(transactionId);

    auto holderIds = ChunkManager->AllocateUploadTargets(replicaCount);
    if (holderIds.ysize() < replicaCount) {
        ythrow TServiceException(EErrorCode::NotEnoughHolders) << Sprintf("Not enough holders available (ReplicaCount: %d)",
            replicaCount);
    }

    FOREACH(auto holderId, holderIds) {
        const THolder& holder = ChunkManager->GetHolder(holderId);
        response->add_holderaddresses(holder.GetAddress());
    }

    auto chunkId = TChunkId::Create();

    ChunkManager
        ->InitiateAllocateChunk(transactionId)
        ->OnSuccess(~FromFunctor([=] (TChunkId id)
            {
                response->set_chunkid(id.ToProto());

                context->SetResponseInfo("ChunkId: %s, Addresses: [%s]",
                    ~id.ToString(),
                    ~JoinToString(response->holderaddresses(), ", "));

                context->Reply();
            }))
        ->OnError(~CreateErrorHandler(~context))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TChunkService, ConfirmChunks)
{
    auto transactionId = TTransactionId::FromProto(request->transactionid());

    context->SetRequestInfo("TransactionId: %s, ChunkCount: %d",
        ~transactionId.ToString(),
        request->chunks_size());

    ValidateLeader();
    ValidateTransactionId(transactionId);

    FOREACH (const auto& chunkInfo, request->chunks()) {
        auto chunkId = TChunkId::FromProto(chunkInfo.chunkid());
        auto* chunk = ChunkManager->FindChunk(chunkId);
        if (!chunk) {
            ythrow TServiceException(EErrorCode::NotEnoughHolders) <<
                Sprintf("No such chunk (ChunkId: %s)", ~chunkId.ToString());
        }
    }

    TMsgConfirmChunks message;
    message.set_transactionid(transactionId.ToProto());
    message.mutable_chunks()->MergeFrom(request->chunks());

    ChunkManager
        ->InitiateConfirmChunks(message)
        ->OnSuccess(~CreateSuccessHandler(~context))
        ->OnError(~CreateErrorHandler(~context))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TChunkService, FindChunk)
{
    auto chunkId = TChunkId::FromProto(request->chunkid());

    context->SetRequestInfo("ChunkId: %s", ~chunkId.ToString());

    ValidateLeader();
    ValidateChunkId(chunkId);

    const auto& chunk = ChunkManager->GetChunk(chunkId);
    ChunkManager->FillHolderAddresses(response->mutable_holderaddresses(), chunk);

    context->SetResponseInfo("HolderCount: %d", response->holderaddresses_size());

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
