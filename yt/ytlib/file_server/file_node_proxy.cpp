#include "stdafx.h"
#include "file_node_proxy.h"
#include <ytlib/chunk_holder/chunk_meta_extensions.h>
#include <ytlib/chunk_holder/chunk.pb.h>

#include <ytlib/chunk_server/chunk.h>
#include <ytlib/chunk_server/chunk_list.h>
#include <ytlib/misc/string.h>
#include <ytlib/misc/codec.h>

namespace NYT {
namespace NFileServer {

using namespace NChunkServer;
using namespace NCypress;
using namespace NYTree;
using namespace NRpc;
using namespace NChunkHolder::NProto;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NTransactionServer;


////////////////////////////////////////////////////////////////////////////////

TFileNodeProxy::TFileNodeProxy(
    INodeTypeHandler* typeHandler,
    TBootstrap* bootstrap,
    TTransaction* transaction,
    const TNodeId& nodeId)
    : TCypressNodeProxyBase<IEntityNode, TFileNode>(
        typeHandler,
        bootstrap,
        transaction,
        nodeId)
{ }

void TFileNodeProxy::DoInvoke(IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Fetch);
    TBase::DoInvoke(context);
}

void TFileNodeProxy::OnUpdateAttribute(
    const Stroka& key,
    const TNullable<NYTree::TYson>& oldValue,
    const TNullable<NYTree::TYson>& newValue)
{
    if (key == "executable" && newValue) {
        DeserializeFromYson<bool>(*newValue);
    } else if (key == "file_name" && newValue) {
        // File name must be string.
        // ToDo(psushin): write more sophisticated validation.
        DeserializeFromYson<Stroka>(*newValue);
    }
}

bool TFileNodeProxy::IsExecutable()
{
    return Attributes().Get("executable", false);
}

Stroka TFileNodeProxy::GetFileName()
{
    auto fileName = Attributes().Find<Stroka>("file_name");
    if (fileName)
        return *fileName;

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
    attributes->push_back("compression_ratio");
    attributes->push_back("codec_id");
    attributes->push_back("chunk_list_id");
    attributes->push_back("chunk_id");
    TBase::GetSystemAttributes(attributes);
}

bool TFileNodeProxy::GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer)
{
    auto chunkManager = Bootstrap->GetChunkManager();
    const auto& fileNode = GetTypedImpl();
    const auto& chunkList = chunkManager->GetChunkList(fileNode.GetChunkListId());
    const auto& statistics = chunkList.Statistics();
    YASSERT(chunkList.Children().size() == 1);
    auto chunkRef = chunkList.Children()[0];
    const auto& chunk = *chunkRef.AsChunk();

    auto miscExt = GetProtoExtension<TMiscExt>(chunk.ChunkMeta().extensions());
    if (name == "size") {
        BuildYsonFluently(consumer)
            .Scalar(statistics.UncompressedSize);
        return true;
    }

    if (name == "compressed_size") {
        BuildYsonFluently(consumer)
            .Scalar(statistics.CompressedSize);
        return true;
    }

    if (name == "compression_ratio") {
        double ratio = statistics.UncompressedSize > 0 ?
            static_cast<double>(statistics.CompressedSize) / statistics.UncompressedSize : 0;
        BuildYsonFluently(consumer)
            .Scalar(ratio);
        return true;
    }

    if (name == "codec_id") {
        auto codecId = ECodecId(miscExt->codec_id());
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
            .Scalar(chunkRef.GetId().ToString());
        return true;
    }

    return TBase::GetSystemAttribute(name, consumer);
}

DEFINE_RPC_SERVICE_METHOD(TFileNodeProxy, Fetch)
{
    UNUSED(request);

    const auto& impl = GetTypedImpl();
    
    auto chunkListId = impl.GetChunkListId();
    auto chunkManager = Bootstrap->GetChunkManager();
    const auto& chunkList = chunkManager->GetChunkList(chunkListId);
    YASSERT(chunkList.Children().size() == 1);
    
    auto chunkRef = chunkList.Children()[0];
    YASSERT(chunkRef.GetType() == EObjectType::Chunk);

    auto chunkId = chunkRef.GetId();
    const auto& chunk = *chunkRef.AsChunk();

    *response->mutable_chunk_id() = chunkId.ToProto();
    chunkManager->FillNodeAddresses(response->mutable_node_addresses(), chunk);

    response->set_executable(IsExecutable());
    response->set_file_name(GetFileName());

    context->SetResponseInfo("ChunkId: %s, FileName: %s, Executable: %s, NodeAddresses: [%s]",
        ~chunkId.ToString(),
        ~response->file_name(),
        ~ToString(response->executable()),
        ~JoinToString(response->node_addresses()));

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

