#include "stdafx.h"
#include "table_node_proxy.h"

#include <ytlib/misc/string.h>
#include <ytlib/misc/serialize.h>

#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/ephemeral_node_factory.h>
#include <ytlib/ytree/yson_parser.h>
#include <ytlib/ytree/tokenizer.h>
#include <ytlib/ytree/ypath_format.h>

#include <ytlib/ypath/token.h>

#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_list.h>
#include <server/chunk_server/chunk_manager.h>
#include <server/chunk_server/chunk_tree_traversing.h>

#include <server/cell_master/bootstrap.h>

#include <ytlib/table_client/schema.h>
#include <ytlib/table_client/key.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

namespace NYT {
namespace NTableServer {

using namespace NChunkServer;
using namespace NCypressServer;
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

void ThrowUnexpectedToken(const TToken& token)
{
    THROW_ERROR_EXCEPTION("Token is unexpected");
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

class TTableNodeProxy::TFetchChunkVisitor
    : public IChunkVisitor
{
public:
    TFetchChunkVisitor(
        NCellMaster::TBootstrap* bootstrap,
        const TChunkList* chunkList,
        TCtxFetchPtr context, 
        const TChannel& channel)
        : Bootstrap(bootstrap)
        , ChunkList(chunkList)
        , Context(context)
        , Channel(channel)
        , SessionCount(0)
        , Completed(false)
        , Finished(false)
    { }

    void StartSession(const TReadLimit& lowerBound, const TReadLimit& upperBound)
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        ++SessionCount;

        TraverseChunkTree(
            Bootstrap,
            this,
            ChunkList,
            lowerBound,
            upperBound);
    }

    void Complete() 
    {
        VERIFY_THREAD_AFFINITY(StateThread);
        YCHECK(!Completed);

        Completed = true;
        if (SessionCount == 0 && !Finished) {
            Reply();
        }
    }

private:
    NCellMaster::TBootstrap* Bootstrap;
    const TChunkList* ChunkList;
    TCtxFetchPtr Context;
    TChannel Channel;

    int SessionCount;
    bool Finished;
    bool Completed;

    void Reply()
    {
        Context->SetResponseInfo("ChunkCount: %d", Context->Response().chunks_size());
        Context->Reply();
        Finished = true;
    }

    virtual void OnChunk(
        const TChunk* chunk, 
        const TReadLimit& startLimit,
        const TReadLimit& endLimit) override
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        auto chunkManager = Bootstrap->GetChunkManager();

        if (!chunk->IsConfirmed()) {
            ReplyError(TError("Cannot fetch a table containing an unconfirmed chunk %s",
                ~chunk->GetId().ToString()));
            return;
        }

        auto* inputChunk = Context->Response().add_chunks();
        *inputChunk->mutable_channel() = Channel.ToProto();

        if (Context->Request().fetch_node_addresses()) {
            chunkManager->FillNodeAddresses(inputChunk->mutable_node_addresses(), chunk);
        }

        if (Context->Request().fetch_all_meta_extensions()) {
            *inputChunk->mutable_extensions() = chunk->ChunkMeta().extensions();
        } else {
            yhash_set<int> tags(
                Context->Request().extension_tags().begin(),
                Context->Request().extension_tags().end());
            FilterProtoExtensions(
                inputChunk->mutable_extensions(),
                chunk->ChunkMeta().extensions(),
                tags);
        }

        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(chunk->ChunkMeta().extensions());
        inputChunk->set_uncompressed_data_size(miscExt.uncompressed_data_size());
        inputChunk->set_row_count(miscExt.row_count());

        auto* slice = inputChunk->mutable_slice();
        *slice->mutable_chunk_id() = chunk->GetId().ToProto();

        *slice->mutable_start_limit() = startLimit;
        *slice->mutable_end_limit() = endLimit;
    }

    virtual void OnError(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        --SessionCount;
        YCHECK(SessionCount >= 0);

        ReplyError(error);
    }

    virtual void OnFinish() override
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        --SessionCount;
        YCHECK(SessionCount >= 0);

        if (Completed && !Finished && SessionCount == 0) {
            Reply();
        }
    }


    void ReplyError(const TError& error)
    {
        if (Finished)
            return;

        auto wrappedError = TError(
            error.GetCode() == ETraversingError::Retriable
            ? NRpc::EErrorCode(NRpc::EErrorCode::Unavailable)
            : NRpc::EErrorCode(TError::Fail),
            "Failed to fetch table")
            << error;
        Context->Reply(wrappedError);

        Finished = true;
    }

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

class TChunkVisitorBase
    : public IChunkVisitor
{
public:
    TAsyncError Run()
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        TraverseChunkTree(
            Bootstrap,
            this,
            ChunkList);

        return Promise;
    }

protected:
    NCellMaster::TBootstrap* Bootstrap;
    IYsonConsumer* Consumer;
    const TChunkList* ChunkList;
    TPromise<TError> Promise;

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

    TChunkVisitorBase(
        NCellMaster::TBootstrap* bootstrap,
        const TChunkList* chunkList,
        IYsonConsumer* consumer)
        : Bootstrap(bootstrap)
        , Consumer(consumer)
        , ChunkList(chunkList)
        , Promise(NewPromise<TError>())
    {
        VERIFY_THREAD_AFFINITY(StateThread);
    }

    virtual void OnError(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        Promise.Set(TError("Error traversing chunk tree") << error);
    }
};

class TChunkIdsAttributeVisitor
    : public TChunkVisitorBase
{
public:
    TChunkIdsAttributeVisitor(
        NCellMaster::TBootstrap* bootstrap,
        const TChunkList* chunkList,
        IYsonConsumer* consumer)
        : TChunkVisitorBase(bootstrap, chunkList, consumer)
    {
        Consumer->OnBeginList();
    }

    virtual void OnChunk(
        const TChunk* chunk, 
        const NTableClient::NProto::TReadLimit& startLimit,
        const NTableClient::NProto::TReadLimit& endLimit) override
    {
        VERIFY_THREAD_AFFINITY(StateThread);
        UNUSED(startLimit);
        UNUSED(endLimit);
        
        Consumer->OnListItem();
        Consumer->OnStringScalar(chunk->GetId().ToString());
    }

    virtual void OnFinish() override
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        Consumer->OnEndList();
        Promise.Set(TError());
    }
};

class TCodecStatisticsAttributeVisitor
    : public TChunkVisitorBase
{
public:
    TCodecStatisticsAttributeVisitor(
        TBootstrap* bootstrap,
        const TChunkList* chunkList,
        IYsonConsumer* consumer)
        : TChunkVisitorBase(bootstrap, chunkList, consumer)
    { }

    virtual void OnChunk(
        const TChunk* chunk,
        const TReadLimit& startLimit,
        const TReadLimit& endLimit) override
    {
        VERIFY_THREAD_AFFINITY(StateThread);
        UNUSED(startLimit);
        UNUSED(endLimit);

        const auto& chunkMeta = chunk->ChunkMeta();
        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(chunkMeta.extensions());

        CodecInfo[ECodecId(miscExt.codec_id())].Accumulate(chunk->GetStatistics());
    }

    virtual void OnFinish() override
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        BuildYsonFluently(Consumer)
            .DoMapFor(CodecInfo, [=] (TFluentMap fluent, const TCodecInfoMap::value_type& pair) {
                const auto& statistics = pair.second;
                // TODO(panin): maybe use here the same method as in attributes
                fluent
                    .Item(FormatEnum(pair.first))
                    .BeginMap()
                        .Item("chunk_count").Scalar(statistics.ChunkCount)
                        .Item("uncompressed_data_size").Scalar(statistics.UncompressedSize)
                        .Item("compressed_size").Scalar(statistics.CompressedSize)
                    .EndMap();
            });
        Promise.Set(TError());
    }

private:
    typedef yhash_map<ECodecId, TChunkTreeStatistics> TCodecInfoMap;
    TCodecInfoMap CodecInfo;

};

////////////////////////////////////////////////////////////////////////////////

TTableNodeProxy::TTableNodeProxy(
    INodeTypeHandlerPtr typeHandler,
    TBootstrap* bootstrap,
    TTransaction* transaction,
    ICypressNode* trunkNode)
    : TCypressNodeProxyBase<IEntityNode, TTableNode>(
        typeHandler,
        bootstrap,
        transaction,
        trunkNode)
{ }

void TTableNodeProxy::DoInvoke(IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(GetChunkListForUpdate);
    DISPATCH_YPATH_HEAVY_SERVICE_METHOD(Fetch);
    DISPATCH_YPATH_SERVICE_METHOD(SetSorted);
    DISPATCH_YPATH_SERVICE_METHOD(Clear);
    TBase::DoInvoke(context);
}

IYPathService::TResolveResult TTableNodeProxy::Resolve(
    const TYPath& path,
    IServiceContextPtr context)
{
    // |Fetch| and |GetId| can actually handle path suffix while others can't.
    // NB: |GetId| "handles" suffixes by ignoring them
    // (provided |allow_nonempty_path_suffix| is True).
    const auto& verb = context->GetVerb();
    if (verb == "GetId" || verb == "Fetch") {
        return TResolveResult::Here(path);
    }
    return TCypressNodeProxyBase::Resolve(path, context);
}

bool TTableNodeProxy::IsWriteRequest(IServiceContextPtr context) const
{
    DECLARE_YPATH_SERVICE_WRITE_METHOD(GetChunkListForUpdate);
    DECLARE_YPATH_SERVICE_WRITE_METHOD(SetSorted);
    DECLARE_YPATH_SERVICE_WRITE_METHOD(Clear);
    return TBase::IsWriteRequest(context);
}

TClusterResources TTableNodeProxy::GetResourceUsage() const 
{
    const auto* impl = GetThisTypedImpl();
    const auto* chunkList = impl->GetChunkList();
    // TODO(babenko): this is wrong
    return TClusterResources(chunkList->Statistics().CompressedSize * 3);
}

void TTableNodeProxy::ListSystemAttributes(std::vector<TAttributeInfo>* attributes) const
{
    const auto* impl = GetThisTypedImpl();
    const auto* chunkList = impl->GetChunkList();

    attributes->push_back("chunk_list_id");
    attributes->push_back(TAttributeInfo("chunk_ids", true, true));
    attributes->push_back(TAttributeInfo("codec_statistics", true, true));
    attributes->push_back("chunk_count");
    attributes->push_back("uncompressed_data_size");
    attributes->push_back("compressed_size");
    attributes->push_back("compression_ratio");
    attributes->push_back("row_count");
    attributes->push_back("sorted");
    attributes->push_back("update_mode");
    attributes->push_back(TAttributeInfo("sorted_by", !chunkList->SortedBy().empty()));
    attributes->push_back("replication_factor");
    TBase::ListSystemAttributes(attributes);
}

bool TTableNodeProxy::GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer) const
{
    const auto* impl = GetThisTypedImpl();
    const auto* chunkList = impl->GetChunkList();
    const auto& statistics = chunkList->Statistics();

    if (key == "chunk_list_id") {
        BuildYsonFluently(consumer)
            .Scalar(chunkList->GetId().ToString());
        return true;
    }

    if (key == "chunk_count") {
        BuildYsonFluently(consumer)
            .Scalar(statistics.ChunkCount);
        return true;
    }

    if (key == "uncompressed_data_size") {
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

    if (key == "row_count") {
        BuildYsonFluently(consumer)
            .Scalar(chunkList->Statistics().RowCount);
        return true;
    }

    if (key == "sorted") {
        BuildYsonFluently(consumer)
            .Scalar(!chunkList->SortedBy().empty());
        return true;
    }

    if (key == "update_mode") {
        BuildYsonFluently(consumer)
            .Scalar(FormatEnum(impl->GetUpdateMode()));
        return true;
    }

    if (!chunkList->SortedBy().empty()) {
        if (key == "sorted_by") {
            BuildYsonFluently(consumer)
                .List(chunkList->SortedBy());
            return true;
        }
    }

    if (key == "replication_factor") {
        BuildYsonFluently(consumer)
            .Scalar(impl->GetOwningReplicationFactor());
        return true;
    }

    return TBase::GetSystemAttribute(key, consumer);
}

TAsyncError TTableNodeProxy::GetSystemAttributeAsync(const Stroka& key, IYsonConsumer* consumer) const 
{
    const auto* impl = GetThisTypedImpl();
    const auto* chunkList = impl->GetChunkList();

    if (key == "chunk_ids") {
        auto visitor = New<TChunkIdsAttributeVisitor>(
            Bootstrap,
            chunkList,
            consumer);
        return visitor->Run();
    }

    if (key == "codec_statistics") {
        auto visitor = New<TCodecStatisticsAttributeVisitor>(
            Bootstrap,
            chunkList,
            consumer);
        return visitor->Run();
    }

    return TBase::GetSystemAttributeAsync(key, consumer);
}

void TTableNodeProxy::ValidateUserAttributeUpdate(
    const Stroka& key,
    const TNullable<TYsonString>& oldValue,
    const TNullable<TYsonString>& newValue)
{
    UNUSED(oldValue);

    if (key == "channels") {
        if (!newValue) {
            ThrowCannotRemoveAttribute();
        }
        ChannelsFromYson(newValue.Get());
        return;
    }
}

void TTableNodeProxy::SetSystemAttribute(const Stroka& key, const TYsonString& value)
{
    if (key == "replication_factor") {
        int value = ConvertTo<int>(value);
        
        const int MinReplicationFactor = 1;
        const int MaxReplicationFactor = 10;
        if (value < MinReplicationFactor || value > MaxReplicationFactor) {
            THROW_ERROR_EXCEPTION("Value must be in range [%d,%d]",
                MinReplicationFactor,
                MaxReplicationFactor);
        }
        
        if (Transaction) {
            THROW_ERROR_EXCEPTION("Value cannot be altered inside transaction");
        }

        auto* impl = GetThisTypedMutableImpl();
        YCHECK(impl->GetTrunkNode() == impl);
        impl->SetReplicationFactor(value);

        return;
    }

    TBase::SetSystemAttribute(key, value);
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

TChunkList* TTableNodeProxy::EnsureNodeMutable(TTableNode* node)
{
    switch (node->GetUpdateMode()) {
        case ETableUpdateMode::Append:
            YCHECK(node->GetChunkList()->Children().size() == 2);
            return node->GetChunkList()->Children()[1].AsChunkList();

        case ETableUpdateMode::Overwrite:
            return node->GetChunkList();

        case ETableUpdateMode::None: {
            auto chunkManager = Bootstrap->GetChunkManager();
            auto objectManager = Bootstrap->GetObjectManager();

            auto* snapshotChunkList = node->GetChunkList();

            auto* newChunkList = chunkManager->CreateChunkList();
            YCHECK(newChunkList->OwningNodes().insert(node).second);
            newChunkList->SetRigid(true);

            YCHECK(snapshotChunkList->OwningNodes().erase(node) == 1);
            node->SetChunkList(newChunkList);
            objectManager->RefObject(newChunkList);

            newChunkList->SortedBy() = snapshotChunkList->SortedBy();
            chunkManager->AttachToChunkList(newChunkList, snapshotChunkList);

            auto* deltaChunkList = chunkManager->CreateChunkList();
            chunkManager->AttachToChunkList(newChunkList, deltaChunkList);

            node->SetUpdateMode(ETableUpdateMode::Append);

            objectManager->UnrefObject(snapshotChunkList);

            LOG_DEBUG_UNLESS(IsRecovery(), "Table node is switched to append mode (NodeId: %s, NewChunkListId: %s, SnapshotChunkListId: %s, DeltaChunkListId: %s)",
                ~node->GetId().ToString(),
                ~newChunkList->GetId().ToString(),
                ~snapshotChunkList->GetId().ToString(),
                ~deltaChunkList->GetId().ToString());
            return deltaChunkList;
        }

        default:
            YUNREACHABLE();
    }
}

void TTableNodeProxy::ClearNode(TTableNode* node)
{
    auto chunkManager = Bootstrap->GetChunkManager();
    auto objectManager = Bootstrap->GetObjectManager();

    auto* oldChunkList = node->GetChunkList();
    YCHECK(oldChunkList->OwningNodes().erase(node) == 1);
    objectManager->UnrefObject(oldChunkList);

    auto* newChunkList = chunkManager->CreateChunkList();
    YCHECK(newChunkList->OwningNodes().insert(node).second);
    node->SetChunkList(newChunkList);
    objectManager->RefObject(newChunkList);

    node->SetUpdateMode(ETableUpdateMode::Overwrite);

    LOG_DEBUG_UNLESS(IsRecovery(), "Table node is cleared and switched to overwrite mode (NodeId: %s, NewChunkListId: %s)",
        ~node->GetId().ToString(),
        ~newChunkList->GetId().ToString());
}

DEFINE_RPC_SERVICE_METHOD(TTableNodeProxy, GetChunkListForUpdate)
{
    UNUSED(request);
    context->SetRequestInfo("");

    if (!Transaction) {
        THROW_ERROR_EXCEPTION("Transaction required");
    }

    auto* impl = LockThisTypedImpl(ELockMode::Shared);
    const auto* chunkList = EnsureNodeMutable(impl);

    *response->mutable_chunk_list_id() = chunkList->GetId().ToProto();
    context->SetResponseInfo("ChunkListId: %s", ~chunkList->GetId().ToString());

    SetModified();

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TTableNodeProxy, Fetch)
{
    context->SetRequestInfo("");

    const auto* impl = GetThisTypedImpl();

    auto channel = TChannel::CreateEmpty();
    TReadLimit lowerLimit, upperLimit;
    ParseYPath(context->GetPath(), &channel, &lowerLimit, &upperLimit);
    auto* chunkList = impl->GetChunkList();

    auto visitor = New<TFetchChunkVisitor>(
        Bootstrap,
        chunkList,
        context, 
        channel);

    if (request->negate()) {
        if (lowerLimit.has_row_index() || lowerLimit.has_key()) {
            visitor->StartSession(TReadLimit(), lowerLimit);
        }

        if (upperLimit.has_row_index() || upperLimit.has_key()) {
            visitor->StartSession(upperLimit, TReadLimit());
        }
    } else {
        visitor->StartSession(lowerLimit, upperLimit);
    }

    visitor->Complete();
}

DEFINE_RPC_SERVICE_METHOD(TTableNodeProxy, SetSorted)
{
    auto keyColumns = FromProto<Stroka>(request->key_columns());
    context->SetRequestInfo("KeyColumns: %s", ~ConvertToYsonString(keyColumns, EYsonFormat::Text).Data());

    auto* impl = LockThisTypedImpl();

    if (impl->GetUpdateMode() != ETableUpdateMode::Overwrite) {
        THROW_ERROR_EXCEPTION("Table node must be in overwrite mode");
    }

    impl->GetChunkList()->SortedBy() = keyColumns;

    SetModified();

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TTableNodeProxy, Clear)
{
    context->SetRequestInfo("");

    auto* impl = LockThisTypedImpl();

    ClearNode(impl);

    SetModified();

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

