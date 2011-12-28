#include "stdafx.h"
#include "chunk_service.h"

#include "../misc/string.h"

namespace NYT {
namespace NChunkServer {

using namespace NRpc;
using namespace NMetaState;
using namespace NChunkHolder;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkService::TChunkService(
    NMetaState::IMetaStateManager* metaStateManager,
    TChunkManager* chunkManager,
    NTransactionServer::TTransactionManager* transactionManager)
    : TMetaStateServiceBase(
        metaStateManager,
        TChunkServiceProxy::GetServiceName(),
        ChunkServerLogger.GetCategory())
    , ChunkManager(chunkManager)
    , TransactionManager(transactionManager)
{
    YASSERT(chunkManager);
    YASSERT(transactionManager);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterHolder));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(HolderHeartbeat));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateChunks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(ConfirmChunks));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateChunkLists));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(AttachChunkTrees));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(DetachChunkTrees));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(LocateChunk));
}

 void TChunkService::ValidateHolderId(THolderId holderId)
{
    if (!ChunkManager->FindHolder(holderId)) {
        ythrow TServiceException(EErrorCode::NoSuchHolder) <<
            Sprintf("Invalid or expired holder id (HolderId: %d)", holderId);
    }
}

void TChunkService::ValidateChunkId(const TChunkId& chunkId)
{
    if (!ChunkManager->FindChunk(chunkId)) {
        ythrow TServiceException(EErrorCode::NoSuchChunk) <<
            Sprintf("No such chunk (ChunkId: %s)", ~chunkId.ToString());
    }
}

void TChunkService::ValidateChunkListId(const TChunkListId& chunkListId)
{
    if (!ChunkManager->FindChunkList(chunkListId)) {
        ythrow TServiceException(EErrorCode::NoSuchChunkList) <<
            Sprintf("No such chunk list (ChunkListId: %s)", ~chunkListId.ToString());
    }
}

void TChunkService::ValidateTransactionId(const TTransactionId& transactionId)
{
    if (!TransactionManager->FindTransaction(transactionId)) {
        ythrow TServiceException(EErrorCode::NoSuchTransaction) << 
            Sprintf("No such transaction (TransactionId: %s)", ~transactionId.ToString());
    }
}

void TChunkService::ValidateChunkTreeId(const TChunkTreeId& chunkTreeId)
{
    auto kind = GetChunkTreeKind(chunkTreeId);
    switch (kind) {
        case EChunkTreeKind::Chunk:
            ValidateChunkId(chunkTreeId);
            break;
        case EChunkTreeKind::ChunkList:
            ValidateChunkListId(chunkTreeId);
            break;
        default:
            ythrow TServiceException(EErrorCode::NoSuchChunkTree) << 
                Sprintf("No such chunk tree (ChunkTreeId: %s)", ~chunkTreeId.ToString());
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TChunkService, RegisterHolder)
{
    UNUSED(response);

    Stroka address = request->address();
    const auto& statistics = request->statistics();
    
    context->SetRequestInfo("Address: %s, %s",
        ~address,
        ~ToString(statistics));

    ValidateLeader();

    TMsgRegisterHolder message;
    message.set_address(address);
    message.mutable_statistics()->MergeFrom(statistics);
    ChunkManager
        ->InitiateRegisterHolder(message)
        ->OnSuccess(~FromFunctor([=] (THolderId id)
            {
                response->set_holder_id(id);
                context->SetResponseInfo("HolderId: %d", id);
                context->Reply();
            }))
        ->OnError(~CreateErrorHandler(~context))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TChunkService, HolderHeartbeat)
{
    UNUSED(response);

    auto holderId = request->holder_id();

    context->SetRequestInfo("HolderId: %d", holderId);

    ValidateLeader();
    ValidateHolderId(holderId);

    const auto& holder = ChunkManager->GetHolder(holderId);

    TMsgHeartbeatRequest requestMessage;
    requestMessage.set_holder_id(holderId);
    *requestMessage.mutable_statistics() = request->statistics();
    requestMessage.mutable_added_chunks()->MergeFrom(request->added_chunks());
    requestMessage.mutable_removed_chunks()->MergeFrom(request->removed_chunks());

    ChunkManager
        ->InitiateHeartbeatRequest(requestMessage)
        ->Commit();

    yvector<TReqHolderHeartbeat::TJobInfo> runningJobs;
    runningJobs.reserve(request->jobs_size());
    FOREACH(const auto& jobInfo, request->jobs()) {
        auto jobId = TJobId::FromProto(jobInfo.job_id());
        const TJob* job = ChunkManager->FindJob(jobId);
        if (!job) {
            LOG_INFO("Stopping unknown or obsolete job (JobId: %s, Address: %s, HolderId: %d)",
                ~jobId.ToString(),
                ~holder.GetAddress(),
                holder.GetId());
            response->add_jobs_to_stop(jobId.ToProto());
        } else {
            runningJobs.push_back(jobInfo);
        }
    }

    yvector<TRspHolderHeartbeat::TJobStartInfo> jobsToStart;
    yvector<TJobId> jobsToStop;
    ChunkManager->RunJobControl(
        holder,
        runningJobs,
        &jobsToStart,
        &jobsToStop);

    TMsgHeartbeatResponse responseMessage;
    responseMessage.set_holder_id(holderId);

    FOREACH (const auto& jobInfo, jobsToStart) {
        *response->add_jobs_to_start() = jobInfo;
        *responseMessage.add_started_jobs() = jobInfo;
    }

    FOREACH (const auto& jobId, jobsToStop) {
        auto protoJobId = jobId.ToProto();
        response->add_jobs_to_stop(protoJobId);
        responseMessage.add_stopped_jobs(protoJobId);
    }

    ChunkManager
        ->InitiateHeartbeatResponse(responseMessage)
        ->OnSuccess(~FromFunctor([=] (TVoid)
            {
                context->SetResponseInfo("JobsToStart: %d, JobsToStop: %d",
                    static_cast<int>(response->jobs_to_start_size()),
                    static_cast<int>(response->jobs_to_stop_size()));

                context->Reply();
            }))
        ->OnError(~CreateErrorHandler(~context))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TChunkService, CreateChunks)
{
    auto transactionId = TTransactionId::FromProto(request->transaction_id());
    int chunkCount = request->chunk_count();
    int uploadReplicaCount = request->upload_replica_count();

    context->SetRequestInfo("TransactionId: %s, ChunkCount: %d, UploadReplicaCount: %d",
        ~transactionId.ToString(),
        chunkCount,
        uploadReplicaCount);

    ValidateLeader();
    ValidateTransactionId(transactionId);

    for (int index = 0; index < chunkCount; ++index) {
        auto holderIds = ChunkManager->AllocateUploadTargets(uploadReplicaCount);
        if (holderIds.ysize() < uploadReplicaCount) {
            ythrow TServiceException(EErrorCode::NotEnoughHolders) <<
                Sprintf("Not enough holders available (ReplicaCount: %d)",
                uploadReplicaCount);
        }
        auto* chunkInfo = response->add_chunks();
        FOREACH(auto holderId, holderIds) {
            const THolder& holder = ChunkManager->GetHolder(holderId);
            chunkInfo->add_holder_addresses(holder.GetAddress());
        }
    }

    TMsgCreateChunks message;
    message.set_transaction_id(transactionId.ToProto());
    message.set_chunk_count(chunkCount);
    ChunkManager
        ->InitiateCreateChunks(message)
        ->OnSuccess(~FromFunctor([=] (yvector<TChunkId> chunkIds)
            {
                YASSERT(chunkIds.size() == chunkCount);
                for (int index = 0; index < chunkCount; ++index) {
                    response->mutable_chunks(index)->set_chunk_id(chunkIds[index].ToProto());
                }

                context->Reply();
            }))
        ->OnError(~CreateErrorHandler(~context))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TChunkService, ConfirmChunks)
{
    auto transactionId = TTransactionId::FromProto(request->transaction_id());

    context->SetRequestInfo("TransactionId: %s, ChunkCount: %d",
        ~transactionId.ToString(),
        request->chunks_size());

    ValidateLeader();
    ValidateTransactionId(transactionId);

    FOREACH (const auto& chunkInfo, request->chunks()) {
        auto chunkId = TChunkId::FromProto(chunkInfo.chunk_id());
        ValidateChunkId(chunkId);
    }

    TMsgConfirmChunks message;
    message.set_transaction_id(transactionId.ToProto());
    message.mutable_chunks()->MergeFrom(request->chunks());
    ChunkManager
        ->InitiateConfirmChunks(message)
        ->OnSuccess(~CreateSuccessHandler(~context))
        ->OnError(~CreateErrorHandler(~context))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TChunkService, CreateChunkLists)
{
    auto transactionId = TTransactionId::FromProto(request->transaction_id());
    int chunkListCount = request->chunk_list_count();

    context->SetRequestInfo("TransactionId: %s, ChunkListCount: %d",
        ~transactionId.ToString(),
        chunkListCount);

    ValidateLeader();
    ValidateTransactionId(transactionId);

    const auto& message = *request;
    ChunkManager
        ->InitiateCreateChunkLists(message)
        ->OnSuccess(~FromFunctor([=] (yvector<TChunkListId> chunkListIds)
            {
                YASSERT(chunkListIds.size() == chunkListCount);
                ToProto(*response->mutable_chunk_list_ids(), chunkListIds);

                context->Reply();
            }))
        ->OnError(~CreateErrorHandler(~context))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TChunkService, AttachChunkTrees)
{
    auto transactionId = TTransactionId::FromProto(request->transaction_id());
    auto chunkTreeIds = FromProto<TChunkTreeId>(request->chunk_tree_ids());
    auto parentId = TChunkListId::FromProto(request->parent_id());

    context->SetRequestInfo("TransactionId: %s, ChunkTreeIds: [%s], ParentId: %s",
        ~transactionId.ToString(),
        ~JoinToString(chunkTreeIds),
        ~parentId.ToString());

    ValidateLeader();
    ValidateTransactionId(transactionId);
    ValidateChunkListId(parentId);
    FOREACH (const auto& treeId, chunkTreeIds) {
        ValidateChunkTreeId(treeId);
    }

    const auto& message = *request;
    ChunkManager
        ->InitiateAttachChunkTrees(message)
        ->OnSuccess(~CreateSuccessHandler(~context))
        ->OnError(~CreateErrorHandler(~context))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TChunkService, DetachChunkTrees)
{
    auto transactionId = TTransactionId::FromProto(request->transaction_id());
    auto chunkTreeIds = FromProto<TChunkTreeId>(request->chunk_tree_ids());
    auto parentId = TChunkListId::FromProto(request->parent_id());

    context->SetRequestInfo("TransactionId: %s, ChunkTreeIds: [%s], ParentId: %s",
        ~transactionId.ToString(),
        ~JoinToString(chunkTreeIds),
        ~parentId.ToString());

    ValidateLeader();
    ValidateTransactionId(transactionId);
    if (parentId != NullChunkTreeId) {
        ValidateChunkListId(parentId);
    }
    FOREACH (const auto& treeId, chunkTreeIds) {
        ValidateChunkTreeId(treeId);
    }

    const auto& message = *request;
    ChunkManager
        ->InitiateDetachChunkTrees(message)
        ->OnSuccess(~CreateSuccessHandler(~context))
        ->OnError(~CreateErrorHandler(~context))
        ->Commit();
}

DEFINE_RPC_SERVICE_METHOD(TChunkService, LocateChunk)
{
    auto chunkId = TChunkId::FromProto(request->chunk_id());

    context->SetRequestInfo("ChunkId: %s", ~chunkId.ToString());

    ValidateLeader();
    ValidateChunkId(chunkId);

    const auto& chunk = ChunkManager->GetChunk(chunkId);
    ChunkManager->FillHolderAddresses(response->mutable_holder_addresses(), chunk);

    context->SetResponseInfo("HolderCount: %d", response->holder_addresses_size());

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
