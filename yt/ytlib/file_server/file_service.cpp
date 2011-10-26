#include "stdafx.h"
#include "file_service.h"

#include "../misc/string.h"

namespace NYT {
namespace NFileServer {

using namespace NMetaState;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = FileServerLogger;

////////////////////////////////////////////////////////////////////////////////

TFileService::TFileService(
    TCypressManager::TPtr cypressManager,
    TTransactionManager::TPtr transactionManager,
    TChunkManager::TPtr chunkManager,
    TFileManager::TPtr fileManager,
    IInvoker::TPtr serviceInvoker,
    NRpc::TServer::TPtr server)
    : TMetaStateServiceBase(
        serviceInvoker,
        TFileServiceProxy::GetServiceName(),
        CypressLogger.GetCategory())
    , CypressManager(cypressManager)
    , TransactionManager(transactionManager)
    , ChunkManager(chunkManager)
    , FileManager(fileManager)
{
    YASSERT(~cypressManager != NULL);
    YASSERT(~serviceInvoker != NULL);
    YASSERT(~server!= NULL);

    RegisterMethods();

    server->RegisterService(this);
}

void TFileService::RegisterMethods()
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(SetFileChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetFileChunk));
}

void TFileService::ValidateTransactionId(const TTransactionId& transactionId)
{
    if (transactionId != NullTransactionId &&
        TransactionManager->FindTransaction(transactionId) == NULL)
    {
        ythrow TServiceException(EErrorCode::NoSuchTransaction) << 
            Sprintf("Invalid transaction id (TransactionId: %s)", ~transactionId.ToString());
    }
}

void TFileService::ValidateChunkId(const TChunkId& chunkId)
{
    if (ChunkManager->FindChunk(chunkId) == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchChunk) << 
            Sprintf("Invalid chunk id (ChunkId: %s)", ~chunkId.ToString());
    }
}

void TFileService::ValidateNodeId(const TNodeId& nodeId, const TTransactionId& transactionId)
{
    auto node = CypressManager->FindNode(nodeId, transactionId);
    if (~node == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchNode) << 
            Sprintf("Invalid node id (NodeId: %s, TransactionId: %s)",
                ~nodeId.ToString(),
                ~transactionId.ToString());
    }

    auto type = node->GetImpl().GetRuntimeType();
    if (type != ERuntimeNodeType::File) {
        ythrow TServiceException(EErrorCode::InvalidNodeType) << 
            Sprintf("Invalid node type (NodeId: %s, TransactionId: %s, Type: %s)",
                ~nodeId.ToString(),
                ~transactionId.ToString(),
                ~type.ToString());
    }
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

    ValidateTransactionId(transactionId);
    ValidateChunkId(chunkId);
    ValidateNodeId(nodeId, transactionId);

    FileManager
        ->InitiateSetFileChunk(transactionId, nodeId, chunkId)
        ->OnSuccess(CreateSuccessHandler(context))
        ->OnError(CreateErrorHandler(context))
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

    ValidateTransactionId(transactionId);
    ValidateNodeId(nodeId, transactionId);

    auto node = CypressManager->GetNode(nodeId, transactionId);
    YASSERT(~node != NULL);

    TFileNodeProxy::TPtr typedNode(dynamic_cast<TFileNodeProxy*>(~node));
    YASSERT(~typedNode != NULL);

    auto chunkId = typedNode->GetChunkId();
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
