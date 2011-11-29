#include "stdafx.h"
#include "table_node_proxy.h"

#include "../misc/string.h"

namespace NYT {
namespace NTableServer {

using namespace NChunkServer;
using namespace NCypress;
using namespace NYTree;
using namespace NRpc;
using NChunkClient::TChunkId;

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

DEFINE_RPC_SERVICE_METHOD_IMPL(TTableNodeProxy, AddTableChunks)
{
    UNUSED(response);

    auto chunkIds = FromProto<TChunkId>(request->GetChunkIds());

    context->SetRequestInfo("ChunkIds: [%s]", ~JoinToString(chunkIds));

    EnsureLocked();

    auto& impl = GetTypedImplForUpdate();

    // Check if the table has at least one chunk list.
    // If not, create not.
    TChunkList* chunkList;
    if (impl.ChunkListIds().empty()) {
        YASSERT(impl.GetState() != ENodeState::Branched);
        chunkList = &ChunkManager->CreateChunkList();
        impl.ChunkListIds().push_back(chunkList->GetId());
        ChunkManager->RefChunkList(*chunkList);
    } else {
        chunkList = &ChunkManager->GetChunkListForUpdate(impl.ChunkListIds().back());
    }

    FOREACH (const auto& chunkId, chunkIds) {
        auto& chunk = ChunkManager->GetChunkForUpdate(chunkId);
        ChunkManager->AddChunkToChunkList(chunk, *chunkList);
    }

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

