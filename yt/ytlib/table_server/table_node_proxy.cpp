#include "stdafx.h"
#include "table_node_proxy.h"

#include "../misc/string.h"

namespace NYT {
namespace NTableServer {

using namespace NChunkServer;
using namespace NCypress;
using namespace NYTree;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TTableNodeProxy::TTableNodeProxy(
    INodeTypeHandler* typeHandler,
    TCypressManager* cypressManager,
    TChunkManager* chunkManager,
    const TTransactionId& transactionId,
    const TNodeId& nodeId)
    : TCypressNodeProxyBase<IEntityNode, TTableNode>(
        typeHandler,
        cypressManager,
        transactionId,
        nodeId)
    , ChunkManager(chunkManager)
{ }

bool TTableNodeProxy::IsLogged(IServiceContext* context) const
{
    Stroka verb = context->GetVerb();
    if (verb == "AddTableChunks") {
        return true;
    } else {
        return TBase::IsLogged(context);
    }
}

void TTableNodeProxy::DoInvoke(IServiceContext* context)
{
    Stroka verb = context->GetVerb();
    if (verb == "AddTableChunks") {
        AddTableChunksThunk(context);
    } else {
        TBase::DoInvoke(context);
    }
}

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TTableNodeProxy, AddTableChunks)
{
    UNUSED(response);

    auto chunkIds = FromProto<TChunkId>(request->GetChunkIds());

    context->SetRequestInfo("ChunkIds: [%s]", ~JoinToString(chunkIds));

    EnsureLocked();

    auto& impl = GetTypedImplForUpdate();
    YASSERT(impl.ChunkListIds().ysize() >= 1);
    const auto& appendChunkListId = impl.ChunkListIds().back();
    auto& appendChunkList = ChunkManager->GetChunkListForUpdate(appendChunkListId);

    FOREACH (const auto& chunkId, chunkIds) {
        auto& chunk = ChunkManager->GetChunkForUpdate(chunkId);
        ChunkManager->AddChunkToChunkList(chunk, appendChunkList);
    }

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

