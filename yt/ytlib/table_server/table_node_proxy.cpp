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

using NTableClient::NProto::TReadLimit;
using NTableClient::NProto::TKey;
using NTableServer::NProto::TRspFetch;

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
    return TBase::Resolve(path, verb);
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

void TTableNodeProxy::TraverseChunkTree(
    const TChunkList* chunkList,
    i64 lowerBound,
    TNullable<i64> upperBound,
    NProto::TRspFetch* response)
{
    if (lowerBound >= chunkList->Statistics().RowCount ||
        upperBound && (*upperBound <= 0 || *upperBound <= lowerBound))
    {
        return;
    }

    int index;
    if (lowerBound == 0) {
        index = 0;
    } else {
        auto it = std::upper_bound(
            chunkList->RowCountSums().begin(),
            chunkList->RowCountSums().end(),
            lowerBound);
        index = it - chunkList->RowCountSums().begin();
    }

    YASSERT(index < chunkList->Children().size());
    i64 firstRowIndex = index == 0 ? 0 : chunkList->RowCountSums()[index - 1];

    while (index < chunkList->Children().size()) {
        if (upperBound && firstRowIndex >= *upperBound) {
            break;
        }
        const auto& child = chunkList->Children()[index];
        switch (child.GetType()) {
            case EObjectType::Chunk: {
                i64 rowCount = child.AsChunk()->GetStatistics().RowCount;

                auto* inputChunk = response->add_chunks();
                auto* slice = inputChunk->mutable_slice();
                *slice->mutable_chunk_id() = child.GetId().ToProto();

                slice->mutable_start_limit();
                if (lowerBound > firstRowIndex) {
                    YASSERT(lowerBound - firstRowIndex < rowCount);
                    slice->mutable_start_limit()->set_row_index(lowerBound - firstRowIndex);
                }

                slice->mutable_end_limit();
                if (upperBound && *upperBound < firstRowIndex + rowCount) {
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

namespace {
NTableClient::NProto::TKey GetMinKey(const TChunkTreeRef& ref)
{
    switch (ref.GetType()) {
        case EObjectType::Chunk: {
            auto attributes = ref.AsChunk()->DeserializeAttributes();
            const auto& tableAttributes = attributes.GetExtension(
                NTableClient::NProto::TTableChunkAttributes::table_attributes);
            return tableAttributes.key_samples(0).key();
        }
        case EObjectType::ChunkList:
            YASSERT(!ref.AsChunkList()->Children().empty());
            return GetMinKey(ref.AsChunkList()->Children()[0]);
        default:
            YUNREACHABLE();
    }
}

NTableClient::NProto::TKey GetMaxKey(const TChunkTreeRef& ref)
{
    switch (ref.GetType()) {
        case EObjectType::Chunk: {
            auto attributes = ref.AsChunk()->DeserializeAttributes();
            const auto& tableAttributes = attributes.GetExtension(
                NTableClient::NProto::TTableChunkAttributes::table_attributes);
            return tableAttributes.key_samples(tableAttributes.key_samples_size() - 1).key();
        }
        case EObjectType::ChunkList:
            YASSERT(!ref.AsChunkList()->Children().empty());
            return GetMaxKey(ref.AsChunkList()->Children().back());
        default:
            YUNREACHABLE();
    }
}

bool LessComparer(const TChunkTreeRef& ref, const NTableClient::NProto::TKey& key)
{
    return GetMinKey(ref) < key;
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

}

void TTableNodeProxy::TraverseChunkTree(
    const TChunkList* chunkList,
    const NTableClient::NProto::TKey& lowerBound,
    const NTableClient::NProto::TKey* upperBound,
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
        auto minKey = GetMinKey(child);
        auto maxKey = GetMaxKey(child);
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

namespace {

void ParseChannel(TTokenizer& tokenizer, TChannel* channel)
{
    if (tokenizer.GetCurrentType() == ETokenType::LeftBrace) {
        tokenizer.ParseNext();
        *channel = TChannel::CreateEmpty();
        while (tokenizer.GetCurrentType() != ETokenType::RightBrace) {
            TColumn begin;
            bool isRange = false;
            switch (tokenizer.GetCurrentType()) {
                case ETokenType::String:
                    begin.assign(tokenizer.Current().GetStringValue());
                    tokenizer.ParseNext();
                    if (tokenizer.GetCurrentType() == ETokenType::Colon) {
                        isRange = true;
                        tokenizer.ParseNext();
                    }
                    break;
                case ETokenType::Colon:
                    isRange = true;
                    tokenizer.ParseNext();
                    break;
                default:
                    ThrowUnexpectedToken(tokenizer.Current());
                    YUNREACHABLE();
            }
            if (isRange) {
                switch (tokenizer.GetCurrentType()) {
                    case ETokenType::String: {
                        TColumn end(tokenizer.Current().GetStringValue());
                        channel->AddRange(begin, end);
                        tokenizer.ParseNext();
                        break;
                    }
                    case ETokenType::Comma:
                        channel->AddRange(TRange(begin));
                        break;
                    default:
                        ThrowUnexpectedToken(tokenizer.Current());
                        YUNREACHABLE();
                }
            } else {
                channel->AddColumn(begin);
            }
            switch (tokenizer.GetCurrentType()) {
                case ETokenType::Comma:
                    tokenizer.ParseNext();
                    break;
                case ETokenType::RightBrace:
                    break;
                default:
                    ThrowUnexpectedToken(tokenizer.Current());
                    YUNREACHABLE();
            }
        }
        tokenizer.ParseNext();
    } else {
        *channel = TChannel::CreateUniversal();
    }
}

void ParseRowLimit(
    TTokenizer& tokenizer,
    ETokenType separator,
    TReadLimit* limit)
{
    switch (tokenizer.GetCurrentType()) {
        case ETokenType::String:
            limit->mutable_key()->add_values(Stroka(tokenizer.Current().GetStringValue()));
            tokenizer.ParseNext();
            break;
        case ETokenType::Hash:
            tokenizer.ParseNext();
            limit->set_row_index(tokenizer.Current().GetIntegerValue());
            tokenizer.ParseNext();
            break;
        case ETokenType::LeftParenthesis:
            tokenizer.ParseNext();
            limit->mutable_key();
            while (tokenizer.GetCurrentType() != ETokenType::RightParenthesis) {
                limit->mutable_key()->add_values(Stroka(tokenizer.Current().GetStringValue()));
                tokenizer.ParseNext();
                switch (tokenizer.GetCurrentType()) {
                    case ETokenType::Comma:
                        tokenizer.ParseNext();
                        break;
                    case ETokenType::RightParenthesis:
                        break;
                    default:
                        ThrowUnexpectedToken(tokenizer.Current());
                        YUNREACHABLE();
                }
            }
            tokenizer.ParseNext();
            break;
        default:
            if (tokenizer.GetCurrentType() != separator) {
                ThrowUnexpectedToken(tokenizer.Current());
            }
            break;
    }

    tokenizer.Current().CheckType(separator);
    tokenizer.ParseNext();
}

void ParseRowLimits(
    TTokenizer& tokenizer,
    TReadLimit* lowerLimit,
    TReadLimit* upperLimit)
{
    *lowerLimit = TReadLimit();
    *upperLimit = TReadLimit();
    if (tokenizer.GetCurrentType() == ETokenType::LeftBracket) {
        tokenizer.ParseNext();
        ParseRowLimit(tokenizer, ETokenType::Colon, lowerLimit);
        ParseRowLimit(tokenizer, ETokenType::RightBracket, upperLimit);
    }
}

} // namespace <anonymous>

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
    tokenizer.Current().CheckType(ETokenType::EndOfStream);
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

    auto channel = TChannel::CreateEmpty();
    TReadLimit lowerLimit, upperLimit;
    ParseYPath(context->GetPath(), &channel, &lowerLimit, &upperLimit);
    auto* chunkList = impl.GetChunkList();

    if (lowerLimit.has_key() || upperLimit.has_key()) {
        if (lowerLimit.has_row_index() || upperLimit.has_row_index()) {
            ythrow yexception() << Sprintf("Row limits must have the same type");
        }
        if (!chunkList->GetSorted()) {
            ythrow yexception() << Sprintf("Table is not sorted");
        }
        const auto& lowerBound = lowerLimit.key();
        const auto* upperBound = upperLimit.has_key() ? &upperLimit.key() : NULL;
        //if (!upperBound || *upperBound > lowerBound) {
            if (request->has_negate() && request->negate()) {
                TraverseChunkTree(chunkList, NTableClient::NProto::TKey(), &lowerBound, response);
                if (upperBound) {
                    TraverseChunkTree(chunkList, *upperBound, NULL, response);
                }
            } else {
                TraverseChunkTree(chunkList, lowerBound, upperBound, response);
            }
        //}
    } else {
        i64 lowerBound = lowerLimit.has_row_index() ? lowerLimit.row_index() : 0;
        auto upperBound = upperLimit.has_row_index()
            ? MakeNullable(upperLimit.row_index())
            : Null;
        //if (!upperBound || *upperBound > lowerBound) {
            if (request->has_negate() && request->negate()) {
                TraverseChunkTree(chunkList, 0, lowerBound, response);
                if (upperBound) {
                    TraverseChunkTree(chunkList, *upperBound, Null, response);
                }
            } else {
                TraverseChunkTree(chunkList, lowerBound, upperBound, response);
            }
        //}
    }

    auto chunkManager = Bootstrap->GetChunkManager();
    for (int i = 0; i < response->chunks_size(); ++i) {
        auto* inputChunk = response->mutable_chunks(i);

        *inputChunk->mutable_channel() = channel.ToProto();

        auto chunkId = TChunkId::FromProto(inputChunk->slice().chunk_id());
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

    context->SetResponseInfo("ChunkCount: %d", response->chunks_size());

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

