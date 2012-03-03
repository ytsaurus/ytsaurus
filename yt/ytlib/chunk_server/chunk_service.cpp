#include "stdafx.h"
#include "chunk_service.h"
#include "holder_statistics.h"

#include <ytlib/misc/string.h>
#include <ytlib/object_server/id.h>

namespace NYT {
namespace NChunkServer {

using namespace NRpc;
using namespace NMetaState;
using namespace NChunkHolder;
using namespace NProto;
using namespace NObjectServer;

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

void TChunkService::ValidateChunkTreeId(const TChunkTreeId& treeId)
{
    auto type = TypeFromId(treeId);
    switch (type) {
        case EObjectType::Chunk:
            ValidateChunkId(treeId);
            break;
        case EObjectType::ChunkList:
            ValidateChunkListId(treeId);
            break;
        default:
            ythrow TServiceException(EErrorCode::NoSuchChunkTree) << 
                Sprintf("No such chunk tree (ChunkTreeId: %s)", ~treeId.ToString());
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

    TMsgRegisterHolder message;
    message.set_address(address);
    message.set_incarnation_id(incarnationId.ToProto());
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
    auto holderId = request->holder_id();

    context->SetRequestInfo("HolderId: %d", holderId);

    ValidateHolderId(holderId);

    const auto& holder = ChunkManager->GetHolder(holderId);

    TMsgHeartbeatRequest requestMessage;
    requestMessage.set_incremental(request->incremental());
    requestMessage.set_holder_id(holderId);
    *requestMessage.mutable_statistics() = request->statistics();
    requestMessage.mutable_added_chunks()->MergeFrom(request->added_chunks());
    requestMessage.mutable_removed_chunks()->MergeFrom(request->removed_chunks());

    ChunkManager
        ->InitiateHeartbeatRequest(requestMessage)
        ->Commit();

    yvector<TReqHolderHeartbeat::TJobInfo> runningJobs(request->jobs().begin(), request->jobs().end());
    
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

    yhash_set<TJobId> runningJobIds;
    FOREACH (const auto& jobInfo, runningJobs) {
        runningJobIds.insert(TJobId::FromProto(jobInfo.job_id()));
    }

    FOREACH (const auto& jobId, jobsToStop) {
        auto protoJobId = jobId.ToProto();
        if (runningJobIds.find(jobId) != runningJobIds.end()) {
            response->add_jobs_to_stop(protoJobId);
        }
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
