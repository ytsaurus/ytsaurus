#include "stdafx.h"
#include "table_node_proxy.h"

#include <ytlib/misc/string.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/yson_parser.h>
#include <ytlib/ytree/tokenizer.h>
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
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

TTableNodeProxy::TTableNodeProxy(
    INodeTypeHandler* typeHandler,
    TBootstrap* bootstrap,
    TTransaction* transaction,
    const TNodeId& nodeId)
    : TCypressNodeProxyBase<IEntityNode, TTableNode>(
        typeHandler,
        bootstrap,
        transaction,
        nodeId)
{ }

void TTableNodeProxy::DoInvoke(IServiceContext* context)
{
    DISPATCH_YPATH_SERVICE_METHOD(GetChunkListForUpdate);
    DISPATCH_YPATH_SERVICE_METHOD(Fetch);
    DISPATCH_YPATH_SERVICE_METHOD(SetSorted);
    TBase::DoInvoke(context);
}

IYPathService::TResolveResult TTableNodeProxy::Resolve(const TYPath& path, const Stroka& verb)
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
    DECLARE_YPATH_SERVICE_WRITE_METHOD(SetSorted);
    return TBase::IsWriteRequest(context);
}

void TTableNodeProxy::TraverseChunkTree(
    yvector<NChunkServer::TChunkId>* chunkIds,
    const NChunkServer::TChunkList* chunkList)
{
    FOREACH (const auto& child, chunkList->Children()) {
        switch (child.GetType()) {
            case EObjectType::Chunk: {
                chunkIds->push_back(child.GetId());
                break;
            }
            case EObjectType::ChunkList: {
                TraverseChunkTree(chunkIds, child.AsChunkList());
                break;
            }
            default:
                YUNREACHABLE();
        }
    }
}

void TTableNodeProxy::GetSystemAttributes(std::vector<TAttributeInfo>* attributes)
{
    attributes->push_back("chunk_list_id");
    attributes->push_back(TAttributeInfo("chunk_ids", true, true));
    attributes->push_back("chunk_count");
    attributes->push_back("uncompressed_size");
    attributes->push_back("compressed_size");
    attributes->push_back("compression_ratio");
    attributes->push_back("row_count");
    attributes->push_back("sorted");
    TBase::GetSystemAttributes(attributes);
}

bool TTableNodeProxy::GetSystemAttribute(const Stroka& name, IYsonConsumer* consumer)
{
    const auto& tableNode = GetTypedImpl();
    const auto& chunkList = *tableNode.GetChunkList();
    const auto& statistics = chunkList.Statistics();

    if (name == "chunk_list_id") {
        BuildYsonFluently(consumer)
            .Scalar(chunkList.GetId().ToString());
        return true;
    }

    if (name == "chunk_ids") {
        yvector<TChunkId> chunkIds;

        TraverseChunkTree(&chunkIds, tableNode.GetChunkList());
        BuildYsonFluently(consumer)
            .DoListFor(chunkIds, [=] (TFluentList fluent, TChunkId chunkId) {
                fluent.Item().Scalar(chunkId.ToString());
            });
        return true;
    }

    if (name == "chunk_count") {
        BuildYsonFluently(consumer)
            .Scalar(statistics.ChunkCount);
        return true;
    }

    if (name == "uncompressed_size") {
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
        double ratio = statistics.CompressedSize > 0?
            static_cast<double>(statistics.UncompressedSize) / statistics.CompressedSize :
            1.0;
        BuildYsonFluently(consumer)
            .Scalar(ratio);
        return true;
    }

    if (name == "row_count") {
        BuildYsonFluently(consumer)
            .Scalar(chunkList.Statistics().RowCount);
        return true;
    }

    if (name == "sorted") {
        BuildYsonFluently(consumer)
            .Scalar(chunkList.GetSorted());
        return true;
    }

    return TBase::GetSystemAttribute(name, consumer);
}

void TTableNodeProxy::ParseYPath(
    const TYPath& path,
    TChannel* channel)
{
    TTokenizer tokens(path);
    int index = 0;

    if (tokens[index].GetType() == ETokenType::LeftBrace) {
        *channel = TChannel::CreateEmpty();
        ++index;
        while (tokens[index].GetType() != ETokenType::RightBrace) {
            TColumn begin;
            bool isRange = false;
            switch (tokens[index].GetType()) {
                case ETokenType::String:
                    begin.assign(tokens[index].GetStringValue());
                    ++index;
                    if (tokens[index].GetType() == ETokenType::Colon) {
                        isRange = true;
                        ++index;
                    }
                    break;
                case ETokenType::Colon:
                    isRange = true;
                    ++index;
                    break;
                default:
                    ThrowUnexpectedToken(tokens[index]);
                    YUNREACHABLE();
            }
            if (isRange) {
                switch (tokens[index].GetType()) {
                    case ETokenType::String: {
                        TColumn end(tokens[index].GetStringValue());
                        channel->AddRange(begin, end);
                        ++index;
                        break;
                    }
                    case ETokenType::Comma:
                        channel->AddRange(TRange(begin));
                        break;
                    default:
                        ThrowUnexpectedToken(tokens[index]);
                        YUNREACHABLE();
                }
            } else {
                channel->AddColumn(begin);
            }
            switch (tokens[index].GetType()) {
                case ETokenType::Comma:
                    ++index;
                    break;
                case ETokenType::RightBrace:
                    break;
                default:
                    ThrowUnexpectedToken(tokens[index]);
                    YUNREACHABLE();
            }
        }
        ++index;
    } else {
        *channel = TChannel::CreateUniversal();
    }

    tokens[index].CheckType(ETokenType::None);
}

DEFINE_RPC_SERVICE_METHOD(TTableNodeProxy, GetChunkListForUpdate)
{
    UNUSED(request);

    auto& impl = GetTypedImplForUpdate(ELockMode::Shared);

    const auto& chunkListId = impl.GetChunkList()->GetId();
    *response->mutable_chunk_list_id() = chunkListId.ToProto();

    context->SetResponseInfo("ChunkListId: %s", ~chunkListId.ToString());

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TTableNodeProxy, Fetch)
{
    const auto& impl = GetTypedImpl();

    yvector<TChunkId> chunkIds;
    TraverseChunkTree(&chunkIds, impl.GetChunkList());

    auto channel = TChannel::CreateEmpty();
    ParseYPath(context->GetPath(), &channel);

    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (const auto& chunkId, chunkIds) {
        auto* inputChunk = response->add_chunks();
        auto* slice = inputChunk->mutable_slice();

        *slice->mutable_chunk_id() = chunkId.ToProto();
        slice->mutable_start_limit();
        slice->mutable_end_limit();

        *inputChunk->mutable_channel() = channel.ToProto();

        const auto& chunk = chunkManager->GetChunk(chunkId);
        if (!chunk.IsConfirmed()) {
            ythrow yexception() << Sprintf("Attempt to fetch a table containing an unconfirmed chunk %s",
                ~chunkId.ToString());
        }

        const auto& attributesBlob = chunk.GetAttributes();
        NChunkHolder::NProto::TChunkAttributes attributes;
        YVERIFY(DeserializeFromProto(&attributes, attributesBlob));
        const auto& tableAttributes = attributes.GetExtension(NTableClient::NProto::TTableChunkAttributes::table_attributes);

        inputChunk->set_approximate_row_count(tableAttributes.row_count());
        inputChunk->set_approximate_data_size(chunk.GetSize());

        if (request->has_fetch_holder_addresses() && request->fetch_holder_addresses()) {
            chunkManager->FillHolderAddresses(inputChunk->mutable_holder_addresses(), chunk);
        }

        if (request->has_fetch_chunk_attributes() && request->fetch_chunk_attributes()) {
            inputChunk->set_chunk_attributes(attributesBlob.Begin(), attributesBlob.Size());
        }
    }

    context->SetResponseInfo("ChunkCount: %d", static_cast<int>(chunkIds.size()));

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TTableNodeProxy, SetSorted)
{
    const auto& impl = GetTypedImplForUpdate();

    auto* rootChunkList = impl.GetChunkList();
    YASSERT(rootChunkList->Parents().empty());
    rootChunkList->SetSorted(true);

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

