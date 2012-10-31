#include "stdafx.h"
#include "file_node_proxy.h"
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/chunk.pb.h>

#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_list.h>
#include <ytlib/misc/string.h>
#include <ytlib/codecs/codec.h>

namespace NYT {
namespace NFileServer {

using namespace NChunkServer;
using namespace NCypressServer;
using namespace NYTree;
using namespace NRpc;
using namespace NChunkClient::NProto;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

TFileNodeProxy::TFileNodeProxy(
    INodeTypeHandlerPtr typeHandler,
    TBootstrap* bootstrap,
    TTransaction* transaction,
    ICypressNode* trunkNode)
    : TCypressNodeProxyBase<IEntityNode, TFileNode>(
        typeHandler,
        bootstrap,
        transaction,
        trunkNode)
{ }

void TFileNodeProxy::DoInvoke(IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(FetchFile);
    TBase::DoInvoke(context);
}

void TFileNodeProxy::ValidateUserAttributeUpdate(
    const Stroka& key,
    const TNullable<TYsonString>& oldValue,
    const TNullable<TYsonString>& newValue)
{
    UNUSED(oldValue);

    if (key == "executable" && newValue) {
        ConvertTo<bool>(*newValue);
        return;
    }
    
    if (key == "file_name" && newValue) {
        // File name must be string.
        // ToDo(psushin): write more sophisticated validation.
        ConvertTo<Stroka>(*newValue);
        return;
    }
}

bool TFileNodeProxy::SetSystemAttribute(const Stroka& key, const TYsonString& value)
{
    if (key == "replication_factor") {
        int replicationFactor = ConvertTo<int>(value);

        const int MinReplicationFactor = 1;
        const int MaxReplicationFactor = 10;
        if (replicationFactor < MinReplicationFactor || replicationFactor > MaxReplicationFactor) {
            THROW_ERROR_EXCEPTION("Value must be in range [%d,%d]",
                MinReplicationFactor,
                MaxReplicationFactor);
        }

        if (Transaction) {
            THROW_ERROR_EXCEPTION("Value cannot be altered inside transaction");
        }

        auto* impl = GetThisTypedMutableImpl();
        YCHECK(impl->GetTrunkNode() == impl);
        impl->SetReplicationFactor(replicationFactor);

        return true;
    }

    return TBase::SetSystemAttribute(key, value);
}

bool TFileNodeProxy::IsExecutable()
{
    return Attributes().Get("executable", false);
}

Stroka TFileNodeProxy::GetFileName()
{
    auto fileName = Attributes().Find<Stroka>("file_name");
    if (fileName) {
        return *fileName;
    }

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

TClusterResources TFileNodeProxy::GetResourceUsage() const 
{
    const auto* impl = GetThisTypedImpl();
    const auto* chunkList = impl->GetChunkList();
    return TClusterResources(chunkList->Statistics().DiskSpace);
}

void TFileNodeProxy::ListSystemAttributes(std::vector<TAttributeInfo>* attributes) const
{
    attributes->push_back("size");
    attributes->push_back("compressed_size");
    attributes->push_back("compression_ratio");
    attributes->push_back("codec_id");
    attributes->push_back("chunk_list_id");
    attributes->push_back("chunk_id");
    attributes->push_back("replication_factor");
    TBase::ListSystemAttributes(attributes);
}

bool TFileNodeProxy::GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer) const
{
    const auto* impl = GetThisTypedImpl();
    const auto* chunkList = impl->GetChunkList();
    const auto& statistics = chunkList->Statistics();
    YASSERT(chunkList->Children().size() == 1);
    auto chunkRef = chunkList->Children()[0];
    const auto* chunk = chunkRef.AsChunk();

    auto miscExt = GetProtoExtension<TMiscExt>(chunk->ChunkMeta().extensions());
    if (key == "size") {
        BuildYsonFluently(consumer)
            .Scalar(statistics.UncompressedSize);
        return true;
    }

    if (key == "compressed_size") {
        BuildYsonFluently(consumer)
            .Scalar(statistics.CompressedSize);
        return true;
    }

    if (key == "compression_ratio") {
        double ratio = statistics.UncompressedSize > 0 ?
            static_cast<double>(statistics.CompressedSize) / statistics.UncompressedSize : 0;
        BuildYsonFluently(consumer)
            .Scalar(ratio);
        return true;
    }

    if (key == "codec_id") {
        auto codecId = ECodecId(miscExt.codec_id());
        BuildYsonFluently(consumer)
            .Scalar(CamelCaseToUnderscoreCase(codecId.ToString()));
        return true;
    }

    if (key == "chunk_list_id") {
        BuildYsonFluently(consumer)
            .Scalar(chunkList->GetId().ToString());
        return true;
    }

    if (key == "chunk_id") {
        BuildYsonFluently(consumer)
            .Scalar(chunkRef.GetId().ToString());
        return true;
    }

    if (key == "replication_factor") {
        BuildYsonFluently(consumer)
            .Scalar(impl->GetOwningReplicationFactor());
        return true;
    }

    return TBase::GetSystemAttribute(key, consumer);
}

DEFINE_RPC_SERVICE_METHOD(TFileNodeProxy, FetchFile)
{
    UNUSED(request);

    auto chunkManager = Bootstrap->GetChunkManager();

    const auto* impl = GetThisTypedImpl();
    
    const auto* chunkList = impl->GetChunkList();
    YASSERT(chunkList->Children().size() == 1);
    
    auto chunkRef = chunkList->Children()[0];
    YASSERT(chunkRef.GetType() == EObjectType::Chunk);

    auto chunkId = chunkRef.GetId();
    const auto* chunk = chunkRef.AsChunk();

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

