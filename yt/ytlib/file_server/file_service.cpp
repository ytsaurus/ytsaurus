#include "stdafx.h"
#include "file_service.h"

#include "../misc/string.h"

namespace NYT {
namespace NFileServer {

using namespace NChunkServer;
using namespace NMetaState;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = FileServerLogger;

////////////////////////////////////////////////////////////////////////////////

TFileService::TFileService(
    TMetaStateManager* metaStateManager,
    TChunkManager* chunkManager,
    TFileManager* fileManager,
    NRpc::IServer* server)
    : TMetaStateServiceBase(
        metaStateManager,
        TFileServiceProxy::GetServiceName(),
        FileServerLogger.GetCategory())
    , ChunkManager(chunkManager)
    , FileManager(fileManager)
{
    YASSERT(chunkManager != NULL);
    YASSERT(fileManager != NULL);
    YASSERT(server != NULL);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(SetFileChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetFileChunk));

    server->RegisterService(this);
}

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TFileService, SetFileChunk)
{
    UNUSED(response);

    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    auto nodeId = TTransactionId::FromProto(request->GetNodeId());
    auto chunkId = TTransactionId::FromProto(request->GetChunkId());

    context->SetRequestInfo("TransactionId: %s, NodeId: %s, ChunkId: %s",
        ~transactionId.ToString(),
        ~nodeId.ToString(),
        ~chunkId.ToString());

    ValidateLeader();

    FileManager
        ->InitiateSetFileChunk(nodeId, transactionId, chunkId)
        ->OnSuccess(~CreateSuccessHandler(~context))
        ->OnError(~CreateErrorHandler(~context))
        ->Commit();
}

RPC_SERVICE_METHOD_IMPL(TFileService, GetFileChunk)
{
    UNUSED(response);

    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    auto nodeId = TTransactionId::FromProto(request->GetNodeId());

    context->SetRequestInfo("TransactionId: %s, NodeId: %s",
        ~transactionId.ToString(),
        ~nodeId.ToString());

    auto chunkId = FileManager->GetFileChunk(nodeId, transactionId);
    response->SetChunkId(chunkId.ToProto());

    const auto& chunk = ChunkManager->GetChunk(chunkId);
    FOREACH (auto holderId, chunk.Locations()) {
        const auto& holder = ChunkManager->GetHolder(holderId);
        response->AddAddresses(holder.GetAddress());
    }

    context->SetResponseInfo("ChunkId: %s, Addresses: [%s]",
        ~chunkId.ToString(),
        ~JoinToString(response->GetAddresses()));

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT
