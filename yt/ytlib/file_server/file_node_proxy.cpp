#include "stdafx.h"
#include "file_node_proxy.h"
#include "file_chunk_meta.pb.h"

#include <ytlib/misc/string.h>
#include <ytlib/misc/codec.h>

namespace NYT {
namespace NFileServer {

using namespace NChunkServer;
using namespace NCypress;
using namespace NYTree;
using namespace NRpc;
using namespace NFileClient::NProto;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TFileNodeProxy::TFileNodeProxy(
    INodeTypeHandler* typeHandler,
    TCypressManager* cypressManager,
    TChunkManager* chunkManager,
    const TTransactionId& transactionId,
    const TNodeId& nodeId)
    : TCypressNodeProxyBase<IEntityNode, TFileNode>(
        typeHandler,
        cypressManager,
        transactionId,
        nodeId)
    , ChunkManager(chunkManager)
{ }

void TFileNodeProxy::DoInvoke(IServiceContext* context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Fetch);
    TBase::DoInvoke(context);
}

bool TFileNodeProxy::IsExecutable()
{
    // TODO(babenko): fetch this attribute
    return false;
}

Stroka TFileNodeProxy::GetFileName()
{
    // TODO(babenko): fetch this attribute

    auto parent = GetParent();
    YASSERT(parent);

    switch (parent->GetType()) {
        case ENodeType::Map:
            return parent->AsMap()->GetChildKey(this);

        case ENodeType::List:
            return ToString(parent->AsList()->GetChildIndex(this));

        default:
            YUNREACHABLE();
    }
}

void TFileNodeProxy::GetSystemAttributes(std::vector<TAttributeInfo>* attributes)
{
    attributes->push_back("size");
    attributes->push_back("compressed_size");
    attributes->push_back("codec_id");
    attributes->push_back("chunk_list_id");
    attributes->push_back("chunk_id");
    TBase::GetSystemAttributes(attributes);
}

bool TFileNodeProxy::GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer)
{
    const auto& fileNode = GetTypedImpl();
    const auto& chunkList = ChunkManager->GetChunkList(fileNode.GetChunkListId());
    YASSERT(chunkList.ChildrenIds().ysize() == 1);
    auto chunkId = chunkList.ChildrenIds()[0];
    const auto& chunk = ChunkManager->GetChunk(chunkId);
    auto attributes = chunk
        .DeserializeAttributes()
        .GetExtension(TFileChunkAttributes::file_attributes);

    if (name == "size") {
        BuildYsonFluently(consumer)
            .Scalar(chunkList.Statistics().UncompressedSize);
        return true;
    }

    if (name == "compressed_size") {
        BuildYsonFluently(consumer)
            .Scalar(chunkList.Statistics().CompressedSize);
        return true;
    }

    if (name == "codec_id") {
        auto codecId = ECodecId(attributes.codec_id());
        BuildYsonFluently(consumer)
            .Scalar(CamelCaseToUnderscoreCase(codecId.ToString()));
        return true;
    }

    if (name == "chunk_list_id") {
        BuildYsonFluently(consumer)
            .Scalar(chunkList.GetId().ToString());
        return true;
    }

    if (name == "chunk_id") {
        BuildYsonFluently(consumer)
            .Scalar(chunkId.ToString());
        return true;
    }

    return TBase::GetSystemAttribute(name, consumer);
}

DEFINE_RPC_SERVICE_METHOD(TFileNodeProxy, Fetch)
{
    UNUSED(request);

    const auto& impl = GetTypedImpl();
    
    auto chunkListId = impl.GetChunkListId();
    const auto& chunkList = ChunkManager->GetChunkList(chunkListId);
    YASSERT(chunkList.ChildrenIds().size() == 1);
    
    auto chunkId = chunkList.ChildrenIds()[0];
    YASSERT(TypeFromId(chunkId) == EObjectType::Chunk);

    const auto& chunk = ChunkManager->GetChunk(chunkId);

    response->set_chunk_id(chunkId.ToProto());
    ChunkManager->FillHolderAddresses(response->mutable_holder_addresses(), chunk);

    response->set_executable(IsExecutable());
    response->set_file_name(GetFileName());

    context->SetResponseInfo("ChunkId: %s, FileName: %s, Executable: %s, HolderAddresses: [%s]",
        ~chunkId.ToString(),
        ~response->file_name(),
        ~ToString(response->executable()),
        ~JoinToString(response->holder_addresses()));

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

