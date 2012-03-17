#include "stdafx.h"
#include "table_node_proxy.h"

#include <ytlib/misc/string.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/ytree/yson_reader.h>
#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/chunk_server/chunk.h>
#include <ytlib/chunk_server/chunk_list.h>
#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/table_client/schema.h>

namespace NYT {
namespace NTableServer {

using namespace NChunkServer;
using namespace NCypress;
using namespace NYTree;
using namespace NRpc;
using namespace NObjectServer;
using namespace NTableClient;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TTableNodeProxy::TTableNodeProxy(
    INodeTypeHandler* typeHandler,
    TBootstrap* bootstrap,
    const TTransactionId& transactionId,
    const TNodeId& nodeId)
    : TCypressNodeProxyBase<IEntityNode, TTableNode>(
        typeHandler,
        bootstrap,
        transactionId,
        nodeId)
{ }

void TTableNodeProxy::DoInvoke(IServiceContext* context)
{
    DISPATCH_YPATH_SERVICE_METHOD(GetChunkListForUpdate);
    DISPATCH_YPATH_SERVICE_METHOD(Fetch);
    TBase::DoInvoke(context);
}

IYPathService::TResolveResult TTableNodeProxy::ResolveRecursive(const TYPath& path, const Stroka& verb)
{
    // Resolve to self to handle channels and ranges.
    if (verb == "Fetch") {
        return TResolveResult::Here(path);
    }
    return TBase::ResolveRecursive(path, verb);
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
            auto chunkManager = Bootstrap->GetChunkManager();
            const auto& chunkList = chunkManager->GetChunkList(treeId);
            FOREACH (const auto& childId, chunkList.ChildrenIds()) {
                TraverseChunkTree(chunkIds, childId);
            }
            break;
        }

        default:
            YUNREACHABLE();
    }
}

void TTableNodeProxy::GetSystemAttributes(std::vector<TAttributeInfo>* attributes)
{
    attributes->push_back("chunk_list_id");
    attributes->push_back(TAttributeInfo("chunk_ids", true, true));
    attributes->push_back("uncompressed_size");
    attributes->push_back("compressed_size");
    attributes->push_back("row_count");
    TBase::GetSystemAttributes(attributes);
}

bool TTableNodeProxy::GetSystemAttribute(const Stroka& name, IYsonConsumer* consumer)
{
    const auto& tableNode = GetTypedImpl();
    auto chunkManager = Bootstrap->GetChunkManager();
    const auto& chunkList = chunkManager->GetChunkList(tableNode.GetChunkListId());

    if (name == "chunk_list_id") {
        BuildYsonFluently(consumer)
            .Scalar(chunkList.GetId().ToString());
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

    if (name == "uncompressed_size") {
        BuildYsonFluently(consumer)
            .Scalar(chunkList.Statistics().UncompressedSize);
        return true;
    }

    if (name == "compressed_size") {
        BuildYsonFluently(consumer)
            .Scalar(chunkList.Statistics().CompressedSize);
        return true;
    }

    if (name == "row_count") {
        BuildYsonFluently(consumer)
            .Scalar(chunkList.Statistics().RowCount);
        return true;
    }

    return TBase::GetSystemAttribute(name, consumer);
}

void TTableNodeProxy::ParseYPath(
    const TYPath& path,
    NTableClient::TChannel* channel)
{
    // Set defaults.
    *channel = TChannel::Universal();
    
    // A simple shortcut.
    if (path.empty()) {
        return;
    }

    auto currentPath = path;
        
    // Parse channel.
    auto channelBuilder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    channelBuilder->BeginTree();
    TStringInput channelInput(currentPath);
    TYsonFragmentReader channelReader(~channelBuilder, &channelInput);
    if (channelReader.HasNext()) {
        channelReader.ReadNext();
        *channel = TChannel::FromNode(~channelBuilder->EndTree());
    }

    // TODO(babenko): parse range.
    // TODO(babenko): check for trailing garbage.
}

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

    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (const auto& chunkId, chunkIds) {
        auto* chunkInfo = response->add_chunks();
        auto* slice = chunkInfo->mutable_slice();

        slice->set_chunk_id(chunkId.ToProto());
        slice->mutable_start_limit();
        slice->mutable_end_limit();

        const auto& chunk = chunkManager->GetChunk(chunkId);
        if (chunk.IsConfirmed()) {
            if (request->has_fetch_holder_addresses() && request->fetch_holder_addresses()) {
                chunkManager->FillHolderAddresses(chunkInfo->mutable_holder_addresses(), chunk);
            }

            if (request->has_fetch_chunk_attributes() && request->fetch_chunk_attributes()) {
                const auto& attributes = chunk.GetAttributes();
                chunkInfo->set_attributes(attributes.Begin(), attributes.Size());
            }
        }   
    }

    auto channel = TChannel::Empty();
    ParseYPath(
        context->GetPath(),
        &channel);

    *response->mutable_channel() = channel.ToProto();

    context->SetResponseInfo("ChunkCount: %d", chunkIds.ysize());

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

