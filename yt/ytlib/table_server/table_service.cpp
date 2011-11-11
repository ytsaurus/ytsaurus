#include "stdafx.h"
#include "table_service.h"

#include "../misc/string.h"

namespace NYT {
namespace NTableServer {

using namespace NMetaState;
using namespace NCypress;
using namespace NChunkServer;
using namespace NTransaction;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableServerLogger;

////////////////////////////////////////////////////////////////////////////////

TTableService::TTableService(
    TMetaStateManager* metaStateManager,
    TChunkManager* chunkManager,
    TTableManager* tableManager,
    NRpc::TServer* server)
    : TMetaStateServiceBase(
        metaStateManager,
        TTableServiceProxy::GetServiceName(),
        CypressLogger.GetCategory())
    , ChunkManager(chunkManager)
    , TableManager(tableManager)
{
    YASSERT(chunkManager != NULL);
    YASSERT(tableManager != NULL);
    YASSERT(server != NULL);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(AddTableChunks));

    server->RegisterService(this);
}

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TTableService, AddTableChunks)
{
    UNUSED(response);

    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    auto nodeId = TTransactionId::FromProto(request->GetNodeId());
    auto chunkIds = FromProto<TChunkId, Stroka>(request->GetChunkIds());

    context->SetRequestInfo("TransactionId: %s, NodeId: %s, ChunkIds: %s",
        ~transactionId.ToString(),
        ~nodeId.ToString(),
        ~JoinToString(chunkIds));

    ValidateLeader();

    TableManager
        ->InitiateAddTableChunks(nodeId, transactionId, chunkIds)
        ->OnSuccess(~CreateSuccessHandler(~context))
        ->OnError(~CreateErrorHandler(~context))
        ->Commit();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT
