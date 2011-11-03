#include "stdafx.h"
#include "table_service.h"

#include "../misc/string.h"

namespace NYT {
namespace NFileServer {

using namespace NMetaState;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = FileServerLogger;

////////////////////////////////////////////////////////////////////////////////

TFileService::TFileService(
    TMetaStateManager* metaStateManager,
    TChunkManager* chunkManager,
    TFileManager* tableManager,
    NRpc::TServer* server)
    : TMetaStateServiceBase(
        metaStateManager,
        TFileServiceProxy::GetServiceName(),
        CypressLogger.GetCategory())
    , ChunkManager(chunkManager)
    , FileManager(tableManager)
{
    YASSERT(chunkManager != NULL);
    YASSERT(tableManager != NULL);
    YASSERT(server != NULL);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(SetTableChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTableChunk));

    server->RegisterService(this);
}

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TTableService, SetTableChunk)
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

    TableManager
        ->InitiateSetTableChunk(nodeId, transactionId, chunkId)
        ->OnSuccess(CreateSuccessHandler(context))
        ->OnError(CreateErrorHandler(context))
        ->Commit();
}

RPC_SERVICE_METHOD_IMPL(TTableService, GetTableChunk)
{
    UNUSED(response);

    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    auto nodeId = TTransactionId::FromProto(request->GetNodeId());

    context->SetRequestInfo("TransactionId: %s, NodeId: %s",
        ~transactionId.ToString(),
        ~nodeId.ToString());

    auto chunkId = TableManager->GetTableChunk(nodeId, transactionId);
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

} // namespace NTableServer
} // namespace NYT
