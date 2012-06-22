#include "stdafx.h"
#include "table_node_proxy.h"

#include <ytlib/misc/string.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/yson_parser.h>
#include <ytlib/ytree/tokenizer.h>
#include <ytlib/ytree/ypath_format.h>
#include <ytlib/chunk_server/chunk.h>
#include <ytlib/chunk_server/chunk_list.h>
#include <ytlib/chunk_server/chunk_manager.h>
#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/table_client/schema.h>
#include <ytlib/table_client/key.h>
#include <ytlib/chunk_holder/chunk_meta_extensions.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

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

using NTableClient::NProto::TReadLimit;
using NTableClient::NProto::TKey;

////////////////////////////////////////////////////////////////////////////////

namespace {

void GetBoundaryKeys(TChunkTreeRef ref, TKey* minKey, TKey* maxKey)
{
    switch (ref.GetType()) {
        case EObjectType::Chunk: {
            auto boundaryKeysExt = GetProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(
                ref.AsChunk()->ChunkMeta().extensions());
            if (minKey) {
                *minKey = boundaryKeysExt->start();
            }
            if (maxKey) {
                *maxKey = boundaryKeysExt->end();
            }
            break;
        }
        case EObjectType::ChunkList: {
            const auto& children = ref.AsChunkList()->Children();
            YASSERT(!children.empty());
            if (children.size() == 1) {
                GetBoundaryKeys(children.front(), minKey, maxKey);
            } else {
                if (minKey) {
                    GetBoundaryKeys(ref.AsChunkList()->Children().front(), minKey, NULL);
                }
                if (maxKey) {
                    GetBoundaryKeys(ref.AsChunkList()->Children().back(), NULL, maxKey);
                }
            }
            break;
        }
        default:
            YUNREACHABLE();
    }
}

bool LessComparer(TChunkTreeRef ref, const TKey& key)
{
    TKey minKey;
    GetBoundaryKeys(ref, &minKey, NULL);
    return minKey < key;
}

bool IsEmpty(TChunkTreeRef ref)
{
    switch (ref.GetType()) {
    case EObjectType::Chunk:
        return false;
    case EObjectType::ChunkList:
        return ref.AsChunkList()->Children().empty();
    default:
        YUNREACHABLE();
    }
}

// Adopted from http://www.cplusplus.com/reference/algorithm/lower_bound/
template <class ForwardIterator>
ForwardIterator LowerBound(ForwardIterator first, ForwardIterator last, const TKey& value)
{
    ForwardIterator it;
    typename std::iterator_traits<ForwardIterator>::difference_type count, step;
    count = std::distance(first, last);
    while (count > 0) {
        it = first;
        step = count / 2;
        std::advance(it, step);
        while (it != last && IsEmpty(*it)) {
            ++it;
        }
        if (it != last && LessComparer(*it, value)) {
            count -= std::distance(first, it) + 1;
            first = ++it;
        } else {
            count = step;
        }
    }
    return first;
}

void ParseChannel(TTokenizer& tokenizer, TChannel* channel)
{
    if (tokenizer.GetCurrentType() == BeginColumnSelectorToken) {
        tokenizer.ParseNext();
        *channel = TChannel::CreateEmpty();
        while (tokenizer.GetCurrentType() != EndColumnSelectorToken) {
            Stroka begin;
            bool isRange = false;
            switch (tokenizer.GetCurrentType()) {
                case ETokenType::String:
                    begin.assign(tokenizer.CurrentToken().GetStringValue());
                    tokenizer.ParseNext();
                    if (tokenizer.GetCurrentType() == RangeToken) {
                        isRange = true;
                        tokenizer.ParseNext();
                    }
                    break;
                case RangeToken:
                    isRange = true;
                    tokenizer.ParseNext();
                    break;
                default:
                    ThrowUnexpectedToken(tokenizer.CurrentToken());
                    YUNREACHABLE();
            }
            if (isRange) {
                switch (tokenizer.GetCurrentType()) {
                    case ETokenType::String: {
                        Stroka end(tokenizer.CurrentToken().GetStringValue());
                        channel->AddRange(begin, end);
                        tokenizer.ParseNext();
                        break;
                    }
                    case ColumnSeparatorToken:
                    case EndColumnSelectorToken:
                        channel->AddRange(TRange(begin));
                        break;
                    default:
                        ThrowUnexpectedToken(tokenizer.CurrentToken());
                        YUNREACHABLE();
                }
            } else {
                channel->AddColumn(begin);
            }
            switch (tokenizer.GetCurrentType()) {
                case ColumnSeparatorToken:
                    tokenizer.ParseNext();
                    break;
                case EndColumnSelectorToken:
                    break;
                default:
                    ThrowUnexpectedToken(tokenizer.CurrentToken());
                    YUNREACHABLE();
            }
        }
        tokenizer.ParseNext();
    } else {
        *channel = TChannel::CreateUniversal();
    }
}

void ParseKeyPart(
    TTokenizer& tokenizer,
    TKey* key)
{
    auto *keyPart = key->add_parts();

    switch (tokenizer.GetCurrentType()) {
        case ETokenType::String: {
            auto value = tokenizer.CurrentToken().GetStringValue();
            keyPart->set_str_value(value.begin(), value.size());
            keyPart->set_type(EKeyPartType::String);
            break;
        }

        case ETokenType::Integer: {
            auto value = tokenizer.CurrentToken().GetIntegerValue();
            keyPart->set_int_value(value);
            keyPart->set_type(EKeyPartType::Integer);
            break;
        }

        case ETokenType::Double: {
            auto value = tokenizer.CurrentToken().GetDoubleValue();
            keyPart->set_double_value(value);
            keyPart->set_type(EKeyPartType::Double);
            break;
        }

        default:
            ThrowUnexpectedToken(tokenizer.CurrentToken());
            break;
    }
    tokenizer.ParseNext();
}

void ParseRowLimit(
    TTokenizer& tokenizer,
    ETokenType separator,
    TReadLimit* limit)
{
    if (tokenizer.GetCurrentType() == separator) {
        tokenizer.ParseNext();
        return;
    }

    switch (tokenizer.GetCurrentType()) {
        case RowIndexMarkerToken:
            tokenizer.ParseNext();
            limit->set_row_index(tokenizer.CurrentToken().GetIntegerValue());
            tokenizer.ParseNext();
            break;

        case BeginTupleToken:
            tokenizer.ParseNext();
            limit->mutable_key();
            while (tokenizer.GetCurrentType() != EndTupleToken) {
                ParseKeyPart(tokenizer, limit->mutable_key());
                switch (tokenizer.GetCurrentType()) {
                    case KeySeparatorToken:
                        tokenizer.ParseNext();
                        break;
                    case EndTupleToken:
                        break;
                    default:
                        ThrowUnexpectedToken(tokenizer.CurrentToken());
                        YUNREACHABLE();
                }
            }
            tokenizer.ParseNext();
            break;

        default:
            ParseKeyPart(tokenizer, limit->mutable_key());
            break;
    }

    tokenizer.CurrentToken().CheckType(separator);
    tokenizer.ParseNext();
}

void ParseRowLimits(
    TTokenizer& tokenizer,
    TReadLimit* lowerLimit,
    TReadLimit* upperLimit)
{
    *lowerLimit = TReadLimit();
    *upperLimit = TReadLimit();
    if (tokenizer.GetCurrentType() == BeginRowSelectorToken) {
        tokenizer.ParseNext();
        ParseRowLimit(tokenizer, RangeToken, lowerLimit);
        ParseRowLimit(tokenizer, EndRowSelectorToken, upperLimit);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TTableNodeProxy::TTableNodeProxy(
    INodeTypeHandlerPtr typeHandler,
    TBootstrap* bootstrap,
    TTransaction* transaction,
    const TNodeId& nodeId)
    : TCypressNodeProxyBase<IEntityNode, TTableNode>(
        typeHandler,
        bootstrap,
        transaction,
        nodeId)
{ }

void TTableNodeProxy::DoInvoke(IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(GetChunkListForUpdate);
    DISPATCH_YPATH_SERVICE_METHOD(Fetch);
    DISPATCH_YPATH_SERVICE_METHOD(SetSorted);
    DISPATCH_YPATH_SERVICE_METHOD(Clear);
    TBase::DoInvoke(context);
}

IYPathService::TResolveResult TTableNodeProxy::Resolve(const TYPath& path, const Stroka& verb)
{
    // |Fetch| and |GetId| can actually handle path suffix while others can't.
    // NB: |GetId| "handles" suffixes by ignoring them
    // (provided |allow_nonempty_path_suffix| is True).
    if (verb == "GetId" || verb == "Fetch") {
        return TResolveResult::Here(path);
    }
    return TCypressNodeProxyBase::Resolve(path, verb);
}

bool TTableNodeProxy::IsWriteRequest(IServiceContextPtr context) const
{
    DECLARE_YPATH_SERVICE_WRITE_METHOD(GetChunkListForUpdate);
    DECLARE_YPATH_SERVICE_WRITE_METHOD(SetSorted);
    DECLARE_YPATH_SERVICE_WRITE_METHOD(Clear);
    return TBase::IsWriteRequest(context);
}

void TTableNodeProxy::TraverseChunkTree(std::vector<TChunkId>* chunkIds, const TChunkList* chunkList)
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

void TTableNodeProxy::TraverseChunkTree(
    const TChunkList* chunkList,
    i64 lowerBound,
    TNullable<i64> upperBound,
    NProto::TRspFetch* response)
{
    if (chunkList->Children().empty() || 
        lowerBound >= chunkList->Statistics().RowCount ||
        upperBound && (*upperBound <= 0 || *upperBound <= lowerBound))
    {
        return;
    }

    YASSERT(chunkList->Children().size() == chunkList->RowCountSums().size() + 1);

    int index =
        std::upper_bound(
            chunkList->RowCountSums().begin(),
            chunkList->RowCountSums().end(),
            lowerBound) -
        chunkList->RowCountSums().begin();
    YASSERT(index < chunkList->Children().size());

    i64 firstRowIndex = index == 0 ? 0 : chunkList->RowCountSums()[index - 1];
    YASSERT(firstRowIndex <= lowerBound || lowerBound < 0 && firstRowIndex == 0);

    while (index < chunkList->Children().size()) {
        if (upperBound && firstRowIndex >= *upperBound) {
            break;
        }
        const auto& child = chunkList->Children()[index];
        switch (child.GetType()) {
            case EObjectType::Chunk: {
                i64 rowCount = child.AsChunk()->GetStatistics().RowCount;
                i64 lastRowIndex = firstRowIndex + rowCount; // exclusive
                YASSERT(lowerBound < lastRowIndex);

                auto* inputChunk = response->add_chunks();
                auto* slice = inputChunk->mutable_slice();
                *slice->mutable_chunk_id() = child.GetId().ToProto();

                slice->mutable_start_limit();
                if (lowerBound > firstRowIndex) {
                    slice->mutable_start_limit()->set_row_index(lowerBound - firstRowIndex);
                }

                slice->mutable_end_limit();
                if (upperBound && *upperBound < lastRowIndex) {
                    slice->mutable_end_limit()->set_row_index(*upperBound - firstRowIndex);
                }

                firstRowIndex += rowCount;
                break;
            }
            case EObjectType::ChunkList:
                TraverseChunkTree(
                    child.AsChunkList(),
                    lowerBound - firstRowIndex,
                    upperBound ? MakeNullable(*upperBound - firstRowIndex) : Null,
                    response);
                firstRowIndex += child.AsChunkList()->Statistics().RowCount;
                break;
            default:
                YUNREACHABLE();
        }
        ++index;
    }
}

void TTableNodeProxy::TraverseChunkTree(
    const TChunkList* chunkList,
    const TKey& lowerBound,
    const TKey* upperBound,
    NProto::TRspFetch* response)
{
    if (chunkList->Children().empty()) {
        return;
    }

    if (upperBound && *upperBound <= lowerBound) {
        return;
    }

    auto it = LowerBound(
        chunkList->Children().begin(),
        chunkList->Children().end(),
        lowerBound);
    if (it != chunkList->Children().begin()) {
        --it;
    }

    for (; it != chunkList->Children().end(); ++it) {
        const auto& child = *it;
        if (IsEmpty(child)) {
            continue;
        }

        TKey minKey, maxKey;
        GetBoundaryKeys(child, &minKey, &maxKey);

        if (lowerBound > maxKey) {
            continue; // possible for the first chunk tree considered
        }
        if (upperBound && minKey >= *upperBound) {
            break;
        }

        switch (child.GetType()) {
            case EObjectType::Chunk: {
                auto* inputChunk = response->add_chunks();
                auto* slice = inputChunk->mutable_slice();
                *slice->mutable_chunk_id() = child.GetId().ToProto();

                slice->mutable_start_limit();
                if (lowerBound > minKey) {
                    *slice->mutable_start_limit()->mutable_key() = lowerBound;
                }

                slice->mutable_end_limit();
                if (upperBound && *upperBound <= maxKey) {
                    *slice->mutable_end_limit()->mutable_key() = *upperBound;
                }

                break;
            }
            case EObjectType::ChunkList:
                TraverseChunkTree(
                    child.AsChunkList(),
                    lowerBound,
                    upperBound,
                    response);
                break;
            default:
                YUNREACHABLE();
        }
    }
}

void TTableNodeProxy::GetSystemAttributes(std::vector<TAttributeInfo>* attributes)
{
    const auto* tableNode = GetTypedImpl();
    const auto* chunkList = tableNode->GetChunkList();

    attributes->push_back("chunk_list_id");
    attributes->push_back(TAttributeInfo("chunk_ids", true, true));
    attributes->push_back("chunk_count");
    attributes->push_back("uncompressed_data_size");
    attributes->push_back("compressed_size");
    attributes->push_back("compression_ratio");
    attributes->push_back("row_count");
    attributes->push_back("sorted");
    attributes->push_back("update_mode");
    attributes->push_back(TAttributeInfo("key_columns", chunkList->GetSorted()));
    TBase::GetSystemAttributes(attributes);
}

bool TTableNodeProxy::GetSystemAttribute(const Stroka& name, IYsonConsumer* consumer)
{
    const auto* tableNode = GetTypedImpl();
    const auto* chunkList = tableNode->GetChunkList();
    const auto& statistics = chunkList->Statistics();

    if (name == "chunk_list_id") {
        BuildYsonFluently(consumer)
            .Scalar(chunkList->GetId().ToString());
        return true;
    }

    if (name == "chunk_ids") {
        std::vector<TChunkId> chunkIds;
        TraverseChunkTree(&chunkIds, tableNode->GetChunkList());
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

    if (name == "uncompressed_data_size") {
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

    if (name == "row_count") {
        BuildYsonFluently(consumer)
            .Scalar(chunkList->Statistics().RowCount);
        return true;
    }

    if (name == "sorted") {
        BuildYsonFluently(consumer)
            .Scalar(chunkList->GetSorted());
        return true;
    }

    if (name == "update_mode") {
        BuildYsonFluently(consumer)
            .Scalar(FormatEnum(tableNode->GetUpdateMode()));
        return true;
    }

    if (chunkList->GetSorted()) {
        if (name == "key_columns") {
            BuildYsonFluently(consumer)
                .List(chunkList->KeyColumns());
            return true;
        }
    }

    return TBase::GetSystemAttribute(name, consumer);
}

void TTableNodeProxy::ParseYPath(
    const TYPath& path,
    TChannel* channel,
    TReadLimit* lowerBound,
    TReadLimit* upperBound)
{
    TTokenizer tokenizer(path);
    tokenizer.ParseNext();
    ParseChannel(tokenizer, channel);
    ParseRowLimits(tokenizer, lowerBound, upperBound);
    tokenizer.CurrentToken().CheckType(ETokenType::EndOfStream);
}

DEFINE_RPC_SERVICE_METHOD(TTableNodeProxy, GetChunkListForUpdate)
{
    UNUSED(request);

    context->SetRequestInfo("");

    auto* tableNode = GetTypedImplForUpdate(ELockMode::Shared);

    const auto& chunkListId = tableNode->GetChunkList()->GetId();
    *response->mutable_chunk_list_id() = chunkListId.ToProto();

    context->SetResponseInfo("ChunkListId: %s", ~chunkListId.ToString());

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TTableNodeProxy, Fetch)
{
    context->SetRequestInfo("");

    const auto& tableNode = GetTypedImpl();

    auto channel = TChannel::CreateEmpty();
    TReadLimit lowerLimit, upperLimit;
    ParseYPath(context->GetPath(), &channel, &lowerLimit, &upperLimit);
    auto* chunkList = tableNode->GetChunkList();

    if (lowerLimit.has_key() || upperLimit.has_key()) {
        if (lowerLimit.has_row_index() || upperLimit.has_row_index()) {
            ythrow yexception() << Sprintf("Row limits must have the same type");
        }
        if (!chunkList->GetSorted()) {
            ythrow yexception() << Sprintf("Table is not sorted");
        }
        const auto& lowerBound = lowerLimit.key();
        const auto* upperBound = upperLimit.has_key() ? &upperLimit.key() : NULL;
        if (!upperBound || *upperBound > lowerBound) {
            if (request->negate()) {
                TraverseChunkTree(chunkList, TKey(), &lowerBound, response);
                if (upperBound) {
                    TraverseChunkTree(chunkList, *upperBound, NULL, response);
                }
            } else {
                TraverseChunkTree(chunkList, lowerBound, upperBound, response);
            }
        }
    } else {
        i64 lowerBound = lowerLimit.has_row_index() ? lowerLimit.row_index() : 0;
        auto upperBound = upperLimit.has_row_index() ? MakeNullable(upperLimit.row_index()) : Null;
        if (!upperBound || *upperBound > lowerBound) {
            if (request->negate()) {
                TraverseChunkTree(chunkList, 0, lowerBound, response);
                if (upperBound) {
                    TraverseChunkTree(chunkList, *upperBound, Null, response);
                }
            } else {
                TraverseChunkTree(chunkList, lowerBound, upperBound, response);
            }
        }
    }

    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (auto& inputChunk, *response->mutable_chunks()) {
        auto chunkId = TChunkId::FromProto(inputChunk.slice().chunk_id());
        const auto& chunk = chunkManager->GetChunk(chunkId);
        if (!chunk.IsConfirmed()) {
            ythrow yexception() << Sprintf("Cannot fetch a table containing an unconfirmed chunk %s",
                ~chunkId.ToString());
        }

        *inputChunk.mutable_channel() = channel.ToProto();

        if (request->fetch_node_addresses()) {
            chunkManager->FillNodeAddresses(inputChunk.mutable_node_addresses(), chunk);
        }

        if (request->fetch_all_meta_extensions()) {
            *inputChunk.mutable_extensions() = chunk.ChunkMeta().extensions();
        } else {
            yhash_set<int> tags(
                request->extension_tags().begin(),
                request->extension_tags().end());
            FilterProtoExtensions(
                inputChunk.mutable_extensions(),
                chunk.ChunkMeta().extensions(),
                tags);
        }
    }

    context->SetResponseInfo("ChunkCount: %d", response->chunks_size());

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TTableNodeProxy, SetSorted)
{
    auto keyColumns = FromProto<Stroka>(request->key_columns());

    context->SetRequestInfo("KeyColumns: %s", ~SerializeToYson(keyColumns, EYsonFormat::Text));

    // This takes an exclusive lock.
    auto* tableNode = GetTypedImplForUpdate();

    auto* rootChunkList = tableNode->GetChunkList();
    YCHECK(rootChunkList->Parents().empty());
    rootChunkList->SetSorted(true);
    rootChunkList->KeyColumns() = keyColumns;

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TTableNodeProxy, Clear)
{
    context->SetRequestInfo("");

    // This takes an exclusive lock.
    auto* tableNode = GetTypedImplForUpdate();

    auto chunkManager = Bootstrap->GetChunkManager();
    auto* chunkList = tableNode->GetChunkList();
    chunkManager->ClearChunkList(chunkList);
    chunkList->SetBranchedRoot(false);
    tableNode->SetUpdateMode(ETableUpdateMode::Overwrite);

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

