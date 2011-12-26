#include "stdafx.h"
#include "table_node_proxy.h"

#include "../misc/string.h"
#include "../misc/serialize.h"

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
    } else if (verb == "GetTableChunks") {
        GetTableChunksThunk(context);
    } else {
        TBase::DoInvoke(context);
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TTableNodeProxy, AddTableChunks)
{
    UNUSED(response);

    auto chunkIds = FromProto<TChunkId>(request->chunk_ids());

    context->SetRequestInfo("ChunkIds: [%s]", ~JoinToString(chunkIds));

    LockIfNeeded();

    auto& impl = GetTypedImplForUpdate();

    // Check if the table has at least one chunk list. Create one if needed.
    // Also create a new chunklist if the node is committed. The latter case
    // means that chunks are being appended without taking a lock.
    TChunkList* chunkList;
    if (impl.ChunkListIds().empty() || impl.GetState() == ENodeState::Committed) {
        // Branched node must already have a final chunk list ready for append.
        YASSERT(impl.GetState() != ENodeState::Branched);

        chunkList = &ChunkManager->CreateChunkList();
        impl.ChunkListIds().push_back(chunkList->GetId());
        ChunkManager->RefChunkList(*chunkList);
    } else {
        chunkList = &ChunkManager->GetChunkListForUpdate(impl.ChunkListIds().back());
    }
    YASSERT(chunkList->GetRefCounter() == 1);

    FOREACH (const auto& chunkId, chunkIds) {
        auto& chunk = ChunkManager->GetChunkForUpdate(chunkId);
        ChunkManager->AddChunkToChunkList(chunk, *chunkList);
    }

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TTableNodeProxy, GetTableChunks)
{
    UNUSED(request);

    yvector<TChunkId> chunkIds;

    const auto& impl = GetTypedImpl();

    FOREACH (const auto& chunkListId, impl.ChunkListIds()) {
        const auto& chunkList = ChunkManager->GetChunkList(chunkListId);
        chunkIds.insert(chunkIds.end(), chunkList.ChunkIds().begin(), chunkList.ChunkIds().end());
    }

    ToProto<TChunkId, Stroka>(*response->mutable_chunk_ids(), chunkIds);

    context->SetResponseInfo("ChunkCount: %d", chunkIds.ysize());
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

