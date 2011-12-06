#include "stdafx.h"
#include "chunk_service.h"

#include "../misc/string.h"

namespace NYT {
namespace NChunkServer {

using namespace NRpc;
using namespace NMetaState;
using NChunkClient::TChunkId;
using NChunkHolder::TJobId;

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
    YASSERT(chunkManager != NULL);
    YASSERT(server != NULL);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterHolder));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(HolderHeartbeat));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(FindChunk));
    server->RegisterService(this);
}

 void TChunkService::ValidateHolderId(THolderId holderId)
{
    const auto* holder = ChunkManager->FindHolder(holderId);
    if (holder == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchHolder) <<
            Sprintf("Invalid or expired holder id (HolderId: %d)", holderId);
    }
}

void TChunkService::ValidateChunkId(const TChunkId& chunkId)
{
    const auto* chunk = ChunkManager->FindChunk(chunkId);
    if (chunk == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchChunk) <<
            Sprintf("Invalid chunk id (ChunkId: %s)", ~chunkId.ToString());
    }
}

void TChunkService::ValidateTransactionId(const TTransactionId& transactionId)
{
    if (TransactionManager->FindTransaction(transactionId) == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchTransaction) << 
            Sprintf("Invalid transaction id (TransactionId: %s)", ~transactionId.ToString());
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

    FOREACH(const auto& chunkInfo, request->addedchunks()) {
        auto chunkId = TChunkId::FromProto(chunkInfo.id());
        if (holder.ChunkIds().find(chunkId) == holder.ChunkIds().end()) {
            *requestMessage.add_addedchunks() = chunkInfo;
        } else {
            LOG_WARNING("Chunk replica is already added (ChunkId: %s, Address: %s, HolderId: %d)",
                ~chunkId.ToString(),
                ~holder.GetAddress(),
                holder.GetId());
        }
    }

    FOREACH(const auto& protoChunkId, request->removedchunks()) {
        auto chunkId = TChunkId::FromProto(protoChunkId);
        if (holder.ChunkIds().find(chunkId) != holder.ChunkIds().end()) {
            requestMessage.add_removedchunks(chunkId.ToProto());
        } else if (ChunkManager->FindChunk(chunkId) != NULL) {
            LOG_WARNING("Chunk replica is already removed (ChunkId: %s, Address: %s, HolderId: %d)",
                ~chunkId.ToString(),
                ~holder.GetAddress(),
                holder.GetId());
        }
    }

    ChunkManager
        ->InitiateHeartbeatRequest(requestMessage)
        ->Commit();

    yvector<NProto::TJobInfo> runningJobs;
    runningJobs.reserve(request->jobs_size());
    FOREACH(const auto& jobInfo, request->jobs()) {
        auto jobId = TJobId::FromProto(jobInfo.jobid());
        const TJob* job = ChunkManager->FindJob(jobId);
        if (job == NULL) {
            LOG_INFO("Stopping unknown or obsolete job (JobId: %s, Address: %s, HolderId: %d)",
                ~jobId.ToString(),
                ~holder.GetAddress(),
                holder.GetId());
            response->add_jobstostop(jobId.ToProto());
        } else {
            runningJobs.push_back(jobInfo);
        }
    }

    yvector<NProto::TJobStartInfo> jobsToStart;
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

DEFINE_RPC_SERVICE_METHOD(TChunkService, CreateChunk)
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
        ->InitiateCreateChunk(transactionId)
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

DEFINE_RPC_SERVICE_METHOD(TChunkService, FindChunk)
{
    auto transactionId = TTransactionId::FromProto(request->transactionid());
    auto chunkId = TChunkId::FromProto(request->chunkid());

    context->SetRequestInfo("TransactionId: %s, ChunkId: %s",
        ~transactionId.ToString(),
        ~chunkId.ToString());

    ValidateLeader();
    ValidateTransactionId(transactionId);
    ValidateChunkId(chunkId);

    auto& chunk = ChunkManager->GetChunkForUpdate(chunkId);

    // TODO: sort w.r.t. proximity
    FOREACH(auto holderId, chunk.Locations()) {
        const THolder& holder = ChunkManager->GetHolder(holderId);
        response->add_holderaddresses(holder.GetAddress());
    }

    context->SetResponseInfo("HolderCount: %d",
        static_cast<int>(response->holderaddresses_size()));

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
