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

void TTableNodeProxy::DoInvoke(IServiceContext* context)
{
    Stroka verb = context->GetVerb();
    if (verb == "GetChunkListId") {
        GetChunkListIdThunk(context);
    } else if (verb == "Fetch") {
        FetchThunk(context);
    } else {
        TBase::DoInvoke(context);
    }
}


bool TTableNodeProxy::IsLogged(IServiceContext* context) const
{
    Stroka verb = context->GetVerb();
    if (verb == "GetChunkListId") {
        return true;
    }
    return TBase::IsLogged(context);;
}

void TTableNodeProxy::TraverseChunkTree(
    yvector<NChunkServer::TChunkId>* chunkIds,
    const NChunkServer::TChunkTreeId& treeId)
{
    switch (GetChunkTreeKind(treeId)) {
        case EChunkTreeKind::Chunk: {
            chunkIds->push_back(treeId);
            break;
        }

        case EChunkTreeKind::ChunkList: {
            const auto& chunkList = ChunkManager->GetChunkList(treeId);
            FOREACH (const auto& childId, chunkList.ChildrenIds()) {
                TraverseChunkTree(chunkIds, childId);
            }
            break;
        }

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TTableNodeProxy, GetChunkListId)
{
    UNUSED(request);

    LockIfNeeded();
    auto& impl = GetTypedImplForUpdate();

    response->set_chunk_list_id(impl.GetChunkListId().ToProto());

    context->SetResponseInfo("ChunkListId: %s", ~impl->GetChunkListId().ToString());

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TTableNodeProxy, Fetch)
{
    const auto& impl = GetTypedImpl();

    yvector<TChunkId> chunkIds;
    TraverseChunkTree(&chunkIds, impl.GetChunkListId());

    FOREACH (const auto& chunkId, chunkIds) {
        auto* chunkInfo = response->add_chunks();
        chunkInfo->set_chunk_id(chunkId.ToProto());

        const auto& chunk = ChunkManager->GetChunk(chunkId);
        if (chunk.IsConfirmed()) {
            if (request->has_fetch_holder_addresses() && request->fetch_holder_addresses()) {
                ChunkManager->FillHolderAddresses(chunkInfo->mutable_holder_addresses(), chunk);
            }

            if (request->has_fetch_chunk_attributes() && request->fetch_chunk_attributes()) {
                auto attributes = chunk.GetAttributes();
                chunkInfo->mutable_attributes()->ParseFromArray(attributes.Begin(), attributes.Size());
            }
        }
    }

    context->SetResponseInfo("ChunkCount: %d", chunkIds.ysize());

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

