#include "stdafx.h"
#include "table_node_proxy.h"

#include <ytlib/misc/string.h>
#include <ytlib/misc/serialize.h>

namespace NYT {
namespace NTableServer {

using namespace NChunkServer;
using namespace NCypress;
using namespace NYTree;
using namespace NRpc;
using namespace NObjectServer;

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
    DISPATCH_YPATH_SERVICE_METHOD(GetChunkListForUpdate);
    DISPATCH_YPATH_SERVICE_METHOD(Fetch);
    TBase::DoInvoke(context);
}

bool TTableNodeProxy::IsWriteRequest(IServiceContext* context) const
{
    DECLARE_YPATH_SERVICE_WRITE_METHOD(GetChunkListForUpdate);
    return TBase::IsWriteRequest(context);
}

void TTableNodeProxy::TraverseChunkTree(
    yvector<NChunkServer::TChunkId>* chunkIds,
    const NChunkServer::TChunkTreeId& treeId)
{
    switch (TypeFromId(treeId)) {
        case EObjectType::Chunk: {
            chunkIds->push_back(treeId);
            break;
        }

        case EObjectType::ChunkList: {
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

void TTableNodeProxy::GetSystemAttributes(yvector<TAttributeInfo>* attributes)
{
    attributes->push_back("chunk_list_id");
    attributes->push_back("chunk_ids");
    TBase::GetSystemAttributes(attributes);
}

bool TTableNodeProxy::GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer)
{
    const auto& tableNode = GetTypedImpl();

    if (name == "chunk_list_id") {
        BuildYsonFluently(consumer)
            .Scalar(tableNode.GetChunkListId().ToString());
        return true;
    }

    if (name == "chunk_ids") {
        yvector<TChunkId> chunkIds;
        TraverseChunkTree(&chunkIds, tableNode.GetChunkListId());
        BuildYsonFluently(consumer)
            .DoListFor(chunkIds, [=] (TFluentList fluent, TChunkId chunkId)
                {
                    fluent.Item().Scalar(chunkId.ToString());
                });
        return true;
    }

    return TBase::GetSystemAttribute(name, consumer);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TTableNodeProxy, GetChunkListForUpdate)
{
    UNUSED(request);

    auto& impl = GetTypedImplForUpdate(ELockMode::Shared);

    response->set_chunk_list_id(impl.GetChunkListId().ToProto());

    context->SetResponseInfo("ChunkListId: %s", ~impl.GetChunkListId().ToString());

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

