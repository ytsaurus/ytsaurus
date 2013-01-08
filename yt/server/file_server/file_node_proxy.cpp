#include "stdafx.h"
#include "file_node_proxy.h"

#include <ytlib/misc/string.h>

#include <ytlib/codecs/codec.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/chunk.pb.h>

#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_list.h>

namespace NYT {
namespace NFileServer {

using namespace NChunkServer;
using namespace NCypressServer;
using namespace NYTree;
using namespace NYson;
using namespace NRpc;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TFileNodeProxy::TFileNodeProxy(
    INodeTypeHandlerPtr typeHandler,
    TBootstrap* bootstrap,
    TTransaction* transaction,
    TFileNode* trunkNode)
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

bool TFileNodeProxy::IsExecutable()
{
    return Attributes().Get("executable", false);
}

Stroka TFileNodeProxy::GetFileName()
{
    // TODO(ignat): Remake wrapper and than delete this option
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
    const auto* node = GetThisTypedImpl();
    const auto* chunkList = node->GetChunkList();
    i64 diskSpace = chunkList->Statistics().DiskSpace * node->GetReplicationFactor();
    return TClusterResources::FromDiskSpace(diskSpace);
}

void TFileNodeProxy::ListSystemAttributes(std::vector<TAttributeInfo>* attributes) const
{
    attributes->push_back("size");
    attributes->push_back("compressed_size");
    attributes->push_back("compression_ratio");
    attributes->push_back("codec");
    attributes->push_back("chunk_list_id");
    attributes->push_back("chunk_id");
    attributes->push_back("replication_factor");
    TBase::ListSystemAttributes(attributes);
}

bool TFileNodeProxy::GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer) const
{
    const auto* node = GetThisTypedImpl();
    const auto* chunkList = node->GetChunkList();
    const auto& statistics = chunkList->Statistics();
    YASSERT(chunkList->Children().size() == 1);
    auto chunkRef = chunkList->Children()[0];
    const auto* chunk = chunkRef.AsChunk();

    auto miscExt = GetProtoExtension<TMiscExt>(chunk->ChunkMeta().extensions());
    if (key == "size") {
        BuildYsonFluently(consumer)
            .Value(statistics.UncompressedDataSize);
        return true;
    }

    if (key == "compressed_size") {
        BuildYsonFluently(consumer)
            .Value(statistics.CompressedDataSize);
        return true;
    }

    if (key == "compression_ratio") {
        double ratio = statistics.UncompressedDataSize > 0 ?
            static_cast<double>(statistics.CompressedDataSize) / statistics.UncompressedDataSize : 0;
        BuildYsonFluently(consumer)
            .Value(ratio);
        return true;
    }

    if (key == "codec") {
        auto codecId = ECodec(miscExt.codec());
        BuildYsonFluently(consumer)
            .Value(CamelCaseToUnderscoreCase(codecId.ToString()));
        return true;
    }

    if (key == "chunk_list_id") {
        BuildYsonFluently(consumer)
            .Value(chunkList->GetId().ToString());
        return true;
    }

    if (key == "chunk_id") {
        BuildYsonFluently(consumer)
            .Value(chunkRef.GetId().ToString());
        return true;
    }

    if (key == "replication_factor") {
        BuildYsonFluently(consumer)
            .Value(node->GetReplicationFactor());
        return true;
    }

    return TBase::GetSystemAttribute(key, consumer);
}

bool TFileNodeProxy::SetSystemAttribute(const Stroka& key, const TYsonString& value)
{
    auto chunkManager = Bootstrap->GetChunkManager();

    if (key == "replication_factor") {
        if (Transaction) {
            THROW_ERROR_EXCEPTION("Attribute cannot be altered inside transaction");
        }

        int replicationFactor = ConvertTo<int>(value);
        const int MinReplicationFactor = 1;
        const int MaxReplicationFactor = 10;
        if (replicationFactor < MinReplicationFactor || replicationFactor > MaxReplicationFactor) {
            THROW_ERROR_EXCEPTION("Value must be in range [%d,%d]",
                MinReplicationFactor,
                MaxReplicationFactor);
        }

        auto* node = GetThisTypedMutableImpl();
        YCHECK(node->IsTrunk());

        if (node->GetReplicationFactor() != replicationFactor) {
            node->SetReplicationFactor(replicationFactor);

            auto securityManager = Bootstrap->GetSecurityManager();
            securityManager->UpdateAccountNodeUsage(node);

            if (IsLeader()) {
                chunkManager->ScheduleRFUpdate(node->GetChunkList());
            }
        }

        return true;
    }

    return TBase::SetSystemAttribute(key, value);
}

DEFINE_RPC_SERVICE_METHOD(TFileNodeProxy, FetchFile)
{
    UNUSED(request);

    auto chunkManager = Bootstrap->GetChunkManager();

    const auto* node = GetThisTypedImpl();
    
    const auto* chunkList = node->GetChunkList();
    YASSERT(chunkList->Children().size() == 1);
    
    auto chunkRef = chunkList->Children()[0];
    YASSERT(chunkRef.GetType() == EObjectType::Chunk);

    auto chunkId = chunkRef.GetId();
    const auto* chunk = chunkRef.AsChunk();

    *response->mutable_chunk_id() = chunkId.ToProto();
    auto addresses = chunkManager->GetChunkAddresses(chunk);
    FOREACH (const auto& address, addresses) {
        response->add_node_addresses(address);
    }

    response->set_executable(IsExecutable());
    response->set_file_name(GetFileName());

    context->SetResponseInfo("ChunkId: %s, FileName: %s, Executable: %s, Addresses: [%s]",
        ~chunkId.ToString(),
        ~response->file_name(),
        ~ToString(response->executable()),
        ~JoinToString(addresses));

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

