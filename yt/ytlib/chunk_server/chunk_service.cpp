#include "stdafx.h"
#include "chunk_service.h"

#include "../misc/string.h"

namespace NYT {
namespace NChunkServer {

using namespace NMetaState;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkService::TChunkService(
    TChunkManager::TPtr chunkManager,
    TTransactionManager::TPtr transactionManager,
    IInvoker::TPtr serviceInvoker,
    NRpc::TServer::TPtr server)
    : TMetaStateServiceBase(
        serviceInvoker,
        TChunkServiceProxy::GetServiceName(),
        ChunkServerLogger.GetCategory())
    , ChunkManager(chunkManager)
    , TransactionManager(transactionManager)
{
    YASSERT(~chunkManager != NULL);
    YASSERT(~server != NULL);

    RegisterMethods();
    server->RegisterService(this);
}

void TChunkService::RegisterMethods()
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterHolder));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(HolderHeartbeat));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(FindChunk));
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

RPC_SERVICE_METHOD_IMPL(TChunkService, RegisterHolder)
{
    UNUSED(response);

    Stroka address = request->GetAddress();
    auto statistics = THolderStatistics::FromProto(request->GetStatistics());
    
    context->SetRequestInfo("Address: %s, %s",
        ~address,
        ~statistics.ToString());

    ChunkManager
        ->InitiateRegisterHolder(address, statistics)
        ->OnSuccess(FromFunctor([=] (THolderId id)
            {
                response->SetHolderId(id);
                context->SetResponseInfo("HolderId: %d", id);
                context->Reply();
            }))
        ->OnError(CreateErrorHandler(context))
        ->Commit();
}

RPC_SERVICE_METHOD_IMPL(TChunkService, HolderHeartbeat)
{
    UNUSED(response);

    auto holderId = request->GetHolderId();

    context->SetRequestInfo("HolderId: %d", holderId);

    ValidateHolderId(holderId);

    const auto& holder = ChunkManager->GetHolder(holderId);

    context->SetRequestInfo("Address: %s, HolderId: %d",
        ~holder.GetAddress(),
        holderId);

    NProto::TMsgHeartbeatRequest requestMessage;
    requestMessage.SetHolderId(holderId);
    *requestMessage.MutableStatistics() = request->GetStatistics();

    FOREACH(const auto& chunkInfo, request->GetAddedChunks()) {
        auto chunkId = TChunkId::FromProto(chunkInfo.GetId());
        if (holder.Chunks().find(chunkId) == holder.Chunks().end()) {
            *requestMessage.AddAddedChunks() = chunkInfo;
        } else {
            LOG_WARNING("Chunk replica is already added (ChunkId: %s, Address: %s, HolderId: %d)",
                ~chunkId.ToString(),
                ~holder.GetAddress(),
                holder.GetId());
        }
    }

    FOREACH(const auto& protoChunkId, request->GetRemovedChunks()) {
        auto chunkId = TChunkId::FromProto(protoChunkId);
        if (holder.Chunks().find(chunkId) != holder.Chunks().end()) {
            requestMessage.AddRemovedChunks(chunkId.ToProto());
        } else {
            LOG_WARNING("Chunk replica does not exist or already removed (ChunkId: %s, Address: %s, HolderId: %d)",
                ~chunkId.ToString(),
                ~holder.GetAddress(),
                holder.GetId());
        }
    }

    ChunkManager
        ->InitiateHeartbeatRequest(requestMessage)
        ->Commit();

    yvector<NProto::TJobInfo> runningJobs;
    runningJobs.reserve(request->JobsSize());
    FOREACH(const auto& jobInfo, request->GetJobs()) {
        auto jobId = TJobId::FromProto(jobInfo.GetJobId());
        const TJob* job = ChunkManager->FindJob(jobId);
        if (job == NULL) {
            LOG_INFO("Stopping unknown or obsolete job (JobId: %s, Address: %s, HolderId: %d)",
                ~jobId.ToString(),
                ~holder.GetAddress(),
                holder.GetId());
            response->AddJobsToStop(jobId.ToProto());
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
    responseMessage.SetHolderId(holderId);

    FOREACH (const auto& jobInfo, jobsToStart) {
        *response->AddJobsToStart() = jobInfo;
        *responseMessage.AddStartedJobs() = jobInfo;
    }

    FOREACH (const auto& jobId, jobsToStop) {
        auto protoJobId = jobId.ToProto();
        response->AddJobsToStop(protoJobId);
        responseMessage.AddStoppedJobs(protoJobId);
    }

    ChunkManager
        ->InitiateHeartbeatResponse(responseMessage)
        ->OnSuccess(FromFunctor([=] (TVoid)
            {
                context->SetResponseInfo("JobsToStart: %d, JobsToStop: %d",
                    static_cast<int>(response->JobsToStartSize()),
                    static_cast<int>(response->JobsToStopSize()));

                context->Reply();
            }))
        ->OnError(CreateErrorHandler(context))
        ->Commit();
}

RPC_SERVICE_METHOD_IMPL(TChunkService, CreateChunk)
{
    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    int replicaCount = request->GetReplicaCount();

    context->SetRequestInfo("TransactionId: %s, ReplicaCount: %d",
        ~transactionId.ToString(),
        replicaCount);

    auto holderIds = ChunkManager->AllocateUploadTargets(replicaCount);
    FOREACH(auto holderId, holderIds) {
        const THolder& holder = ChunkManager->GetHolder(holderId);
        response->AddHolderAddresses(holder.GetAddress());
    }

    auto chunkId = TChunkId::Create();

    ChunkManager
        ->InitiateCreateChunk(transactionId)
        ->OnSuccess(FromFunctor([=] (TChunkId id)
            {
                response->SetChunkId(id.ToProto());

                context->SetResponseInfo("ChunkId: %s, Addresses: [%s]",
                    ~id.ToString(),
                    ~JoinToString(response->GetHolderAddresses(), ", "));

                context->Reply();
            }))
        ->OnError(CreateErrorHandler(context))
        ->Commit();
}

RPC_SERVICE_METHOD_IMPL(TChunkService, FindChunk)
{
    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    auto chunkId = TChunkId::FromProto(request->GetChunkId());

    context->SetRequestInfo("TransactionId: %s, ChunkId: %s",
        ~transactionId.ToString(),
        ~chunkId.ToString());

    ValidateTransactionId(transactionId);
    ValidateChunkId(chunkId);

    auto& chunk = ChunkManager->GetChunkForUpdate(chunkId);

    // TODO: sort w.r.t. proximity
    FOREACH(auto holderId, chunk.Locations()) {
        const THolder& holder = ChunkManager->GetHolder(holderId);
        response->AddHolderAddresses(holder.GetAddress());
    }

    context->SetResponseInfo("HolderCount: %d",
        static_cast<int>(response->HolderAddressesSize()));

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
