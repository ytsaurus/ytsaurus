#include "stdafx.h"
#include "table_node_proxy.h"
#include "table_node.h"
#include "private.h"

#include <ytlib/misc/string.h>
#include <ytlib/misc/serialize.h>

#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/ephemeral_node_factory.h>
#include <ytlib/yson/yson_parser.h>
#include <ytlib/yson/tokenizer.h>
#include <ytlib/ypath/token.h>

#include <ytlib/table_client/table_ypath_proxy.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <ytlib/chunk_client/schema.h>
#include <ytlib/chunk_client/key.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_list.h>
#include <server/chunk_server/chunk_manager.h>
#include <server/chunk_server/chunk_tree_traversing.h>
#include <server/chunk_server/node_directory_builder.h>

#include <server/cypress_server/node_proxy_detail.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NTableServer {

using namespace NChunkServer;
using namespace NChunkClient;
using namespace NCypressServer;
using namespace NYTree;
using namespace NYson;
using namespace NRpc;
using namespace NObjectServer;
using namespace NTableClient;
using namespace NCellMaster;
using namespace NTransactionServer;
using namespace NSecurityServer;

using NChunkClient::NProto::TReadLimit;
using NChunkClient::NProto::TKey;

////////////////////////////////////////////////////////////////////////////////

class TTableNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TTableNode>
{
public:
    TTableNodeProxy(
        NCypressServer::INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        TTableNode* trunkNode);

    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const override;

    virtual NSecurityServer::TClusterResources GetResourceUsage() const override;

private:
    typedef TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TTableNode> TBase;

    class TFetchChunkVisitor;

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override;
    virtual bool GetSystemAttribute(const Stroka& key, NYson::IYsonConsumer* consumer) override;
    virtual TAsyncError GetSystemAttributeAsync(const Stroka& key, NYson::IYsonConsumer* consumer) override;
    virtual void ValidateUserAttributeUpdate(
        const Stroka& key,
        const TNullable<NYTree::TYsonString>& oldValue,
        const TNullable<NYTree::TYsonString>& newValue) override;
    virtual bool SetSystemAttribute(const Stroka& key, const NYTree::TYsonString& value) override;

    virtual bool DoInvoke(NRpc::IServiceContextPtr context) override;

    void ParseYPath(
        const NYPath::TYPath& path,
        NChunkClient::TChannel* channel,
        NChunkClient::NProto::TReadLimit* lowerBound,
        NChunkClient::NProto::TReadLimit* upperBound);

    DECLARE_RPC_SERVICE_METHOD(NTableClient::NProto, PrepareForUpdate);
    DECLARE_RPC_SERVICE_METHOD(NTableClient::NProto, Fetch);
    DECLARE_RPC_SERVICE_METHOD(NTableClient::NProto, SetSorted);

};

////////////////////////////////////////////////////////////////////////////////

class TTableNodeProxy::TFetchChunkVisitor
    : public IChunkVisitor
{
public:
    TFetchChunkVisitor(
        NCellMaster::TBootstrap* bootstrap,
        TChunkList* chunkList,
        TCtxFetchPtr context,
        const TChannel& channel)
        : Bootstrap(bootstrap)
        , ChunkList(chunkList)
        , Context(context)
        , Channel(channel)
        , NodeDirectoryBuilder(context->Response().mutable_node_directory())
        , SessionCount(0)
        , Completed(false)
        , Finished(false)
    {
        if (!Context->Request().fetch_all_meta_extensions()) {
            FOREACH (int tag, Context->Request().extension_tags()) {
                ExtensionTags.insert(tag);
            }
        }
    }

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
    TChunkList* ChunkList;
    TCtxFetchPtr Context;
    TChannel Channel;

    TNodeDirectoryBuilder NodeDirectoryBuilder;
    yhash_set<int> ExtensionTags;
    int SessionCount;
    bool Completed;
    bool Finished;


    void Reply()
    {
        Context->SetResponseInfo("ChunkCount: %d", Context->Response().chunks_size());
        Context->Reply();
        Finished = true;
    }

    virtual bool OnChunk(
        TChunk* chunk,
        const TReadLimit& startLimit,
        const TReadLimit& endLimit) override
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        auto chunkManager = Bootstrap->GetChunkManager();

        if (!chunk->IsConfirmed()) {
            ReplyError(TError("Cannot fetch a table containing an unconfirmed chunk %s",
                ~ToString(chunk->GetId())));
            return false;
        }

        auto replicas = chunkManager->GetChunkReplicas(chunk);
        if (replicas.empty()) {
            // NB: make the check before calling add_chunks, otherwise response can be malformed.
            if (Context->Request().ignore_lost_chunks()) {
                // Just ignore this chunk.
                return true;
            } else {
                ReplyError(TError("Chunk is lost %s",
                    ~ToString(chunk->GetId())));
                return false;
            }
        }

        auto* inputChunk = Context->Response().add_chunks();
        if (!Channel.IsUniversal()) {
            *inputChunk->mutable_channel() = Channel.ToProto();
        }

        NodeDirectoryBuilder.Add(replicas);
        ToProto(inputChunk->mutable_replicas(), replicas);

        if (Context->Request().fetch_all_meta_extensions()) {
            *inputChunk->mutable_extensions() = chunk->ChunkMeta().extensions();
        } else {
            FilterProtoExtensions(
                inputChunk->mutable_extensions(),
                chunk->ChunkMeta().extensions(),
                ExtensionTags);
        }

        ToProto(inputChunk->mutable_chunk_id(), chunk->GetId());
        inputChunk->set_erasure_codec(chunk->GetErasureCodec());
        *inputChunk->mutable_start_limit() = startLimit;
        *inputChunk->mutable_end_limit() = endLimit;

        return true;
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

        Context->Reply(error);
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
    TChunkList* ChunkList;
    TPromise<TError> Promise;

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

    TChunkVisitorBase(
        NCellMaster::TBootstrap* bootstrap,
        TChunkList* chunkList,
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
        TChunkList* chunkList,
        IYsonConsumer* consumer)
        : TChunkVisitorBase(bootstrap, chunkList, consumer)
    {
        Consumer->OnBeginList();
    }

    virtual bool OnChunk(
        TChunk* chunk,
        const NChunkClient::NProto::TReadLimit& startLimit,
        const NChunkClient::NProto::TReadLimit& endLimit) override
    {
        VERIFY_THREAD_AFFINITY(StateThread);
        UNUSED(startLimit);
        UNUSED(endLimit);

        Consumer->OnListItem();
        Consumer->OnStringScalar(ToString(chunk->GetId()));

        return true;
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
        TChunkList* chunkList,
        IYsonConsumer* consumer)
        : TChunkVisitorBase(bootstrap, chunkList, consumer)
    { }

    virtual bool OnChunk(
        TChunk* chunk,
        const TReadLimit& startLimit,
        const TReadLimit& endLimit) override
    {
        VERIFY_THREAD_AFFINITY(StateThread);
        UNUSED(startLimit);
        UNUSED(endLimit);

        const auto& chunkMeta = chunk->ChunkMeta();
        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(chunkMeta.extensions());

        CodecInfo[NCompression::ECodec(miscExt.compression_codec())].Accumulate(chunk->GetStatistics());
        return true;
    }

    virtual void OnFinish() override
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        BuildYsonFluently(Consumer)
            .DoMapFor(CodecInfo, [=] (TFluentMap fluent, const TCodecInfoMap::value_type& pair) {
                const auto& statistics = pair.second;
                // TODO(panin): maybe use here the same method as in attributes
                fluent
                    .Item(FormatEnum(pair.first)).BeginMap()
                        .Item("chunk_count").Value(statistics.ChunkCount)
                        .Item("uncompressed_data_size").Value(statistics.UncompressedDataSize)
                        .Item("compressed_data_size").Value(statistics.CompressedDataSize)
                    .EndMap();
            });
        Promise.Set(TError());
    }

private:
    typedef yhash_map<NCompression::ECodec, TChunkTreeStatistics> TCodecInfoMap;
    TCodecInfoMap CodecInfo;

};

////////////////////////////////////////////////////////////////////////////////

TTableNodeProxy::TTableNodeProxy(
    INodeTypeHandlerPtr typeHandler,
    TBootstrap* bootstrap,
    TTransaction* transaction,
    TTableNode* trunkNode)
    : TBase(
        typeHandler,
        bootstrap,
        transaction,
        trunkNode)
{ }

bool TTableNodeProxy::DoInvoke(IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(PrepareForUpdate);
    DISPATCH_YPATH_HEAVY_SERVICE_METHOD(Fetch);
    DISPATCH_YPATH_SERVICE_METHOD(SetSorted);
    return TBase::DoInvoke(context);
}

bool TTableNodeProxy::IsWriteRequest(IServiceContextPtr context) const
{
    DECLARE_YPATH_SERVICE_WRITE_METHOD(PrepareForUpdate);
    DECLARE_YPATH_SERVICE_WRITE_METHOD(SetSorted);
    return TBase::IsWriteRequest(context);
}

TClusterResources TTableNodeProxy::GetResourceUsage() const
{
    const auto* node = GetThisTypedImpl();
    const auto* chunkList = node->GetChunkList();
    i64 diskSpace = chunkList->Statistics().DiskSpace * node->GetReplicationFactor();
    return TClusterResources(diskSpace, 1);
}

void TTableNodeProxy::ListSystemAttributes(std::vector<TAttributeInfo>* attributes)
{
    const auto* node = GetThisTypedImpl();
    const auto* chunkList = node->GetChunkList();

    attributes->push_back("chunk_list_id");
    attributes->push_back(TAttributeInfo("chunk_ids", true, true));
    attributes->push_back(TAttributeInfo("compression_statistics", true, true));
    attributes->push_back("compression_codec");
    attributes->push_back("chunk_count");
    attributes->push_back("uncompressed_data_size");
    attributes->push_back("compressed_data_size");
    attributes->push_back("compression_ratio");
    attributes->push_back("row_count");
    attributes->push_back("sorted");
    attributes->push_back("update_mode");
    attributes->push_back(TAttributeInfo("sorted_by", !chunkList->SortedBy().empty()));
    attributes->push_back("replication_factor");
    TBase::ListSystemAttributes(attributes);
}

bool TTableNodeProxy::GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer)
{
    const auto* node = GetThisTypedImpl();
    const auto* chunkList = node->GetChunkList();
    const auto& statistics = chunkList->Statistics();

    if (key == "chunk_list_id") {
        BuildYsonFluently(consumer)
            .Value(ToString(chunkList->GetId()));
        return true;
    }

    if (key == "chunk_count") {
        BuildYsonFluently(consumer)
            .Value(statistics.ChunkCount);
        return true;
    }

    if (key == "uncompressed_data_size") {
        BuildYsonFluently(consumer)
            .Value(statistics.UncompressedDataSize);
        return true;
    }

    if (key == "compressed_data_size") {
        BuildYsonFluently(consumer)
            .Value(statistics.CompressedDataSize);
        return true;
    }

    if (key == "compression_ratio") {
        double ratio =
            statistics.UncompressedDataSize > 0
            ? static_cast<double>(statistics.CompressedDataSize) / statistics.UncompressedDataSize
            : 0;
        BuildYsonFluently(consumer)
            .Value(ratio);
        return true;
    }

    if (key == "row_count") {
        BuildYsonFluently(consumer)
            .Value(statistics.RowCount);
        return true;
    }

    if (key == "sorted") {
        BuildYsonFluently(consumer)
            .Value(!chunkList->SortedBy().empty());
        return true;
    }

    if (key == "update_mode") {
        BuildYsonFluently(consumer)
            .Value(FormatEnum(node->GetUpdateMode()));
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
            .Value(node->GetReplicationFactor());
        return true;
    }

    if (key == "compression_codec") {
        BuildYsonFluently(consumer)
            .Value(FormatEnum(node->GetTrunkNode()->GetCodec()));
        return true;
    }

    return TBase::GetSystemAttribute(key, consumer);
}

TAsyncError TTableNodeProxy::GetSystemAttributeAsync(const Stroka& key, IYsonConsumer* consumer)
{
    const auto* node = GetThisTypedImpl();
    const auto* chunkList = node->GetChunkList();

    if (key == "chunk_ids") {
        auto visitor = New<TChunkIdsAttributeVisitor>(
            Bootstrap,
            const_cast<TChunkList*>(chunkList),
            consumer);
        return visitor->Run();
    }

    if (key == "compression_statistics") {
        auto visitor = New<TCodecStatisticsAttributeVisitor>(
            Bootstrap,
            const_cast<TChunkList*>(chunkList),
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
            ThrowCannotRemoveAttribute(key);
        }
        ConvertTo<TChannels>(newValue.Get());
        return;
    }
}

bool TTableNodeProxy::SetSystemAttribute(const Stroka& key, const TYsonString& value)
{
    auto chunkManager = Bootstrap->GetChunkManager();

    if (key == "replication_factor") {
        ValidateNoTransaction();
        int replicationFactor = ConvertTo<int>(value);
        const int MinReplicationFactor = 1;
        const int MaxReplicationFactor = 10;
        if (replicationFactor < MinReplicationFactor || replicationFactor > MaxReplicationFactor) {
            THROW_ERROR_EXCEPTION("Value must be in range [%d,%d]",
                MinReplicationFactor,
                MaxReplicationFactor);
        }

        auto* node = GetThisTypedImpl();
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

    if (key == "compression_codec") {
        ValidateNoTransaction();

        auto* node = GetThisTypedImpl();
        YCHECK(node->IsTrunk());
        auto codecName = ConvertTo<Stroka>(value);
        node->SetCodec(ParseEnum<NCompression::ECodec>(codecName));
        return true;
    }

    return TBase::SetSystemAttribute(key, value);
}

DEFINE_RPC_SERVICE_METHOD(TTableNodeProxy, PrepareForUpdate)
{
    auto mode = ETableUpdateMode(request->mode());
    YCHECK(mode == ETableUpdateMode::Append || mode == ETableUpdateMode::Overwrite);

    context->SetRequestInfo("Mode: %s", ~mode.ToString());

    ValidateTransaction();
    ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

    auto* node = LockThisTypedImpl(mode == ETableUpdateMode::Append ? ELockMode::Shared : ELockMode::Exclusive);

    if (node->GetUpdateMode() != ETableUpdateMode::None) {
        THROW_ERROR_EXCEPTION("Node is already in %s mode",
            ~FormatEnum(node->GetUpdateMode()).Quote());
    }

    auto chunkManager = Bootstrap->GetChunkManager();
    auto objectManager = Bootstrap->GetObjectManager();

    TChunkList* resultChunkList;
    switch (mode) {
        case ETableUpdateMode::Append: {
            auto* snapshotChunkList = node->GetChunkList();

            auto* newChunkList = chunkManager->CreateChunkList();
            YCHECK(newChunkList->OwningNodes().insert(node).second);

            YCHECK(snapshotChunkList->OwningNodes().erase(node) == 1);
            node->SetChunkList(newChunkList);
            objectManager->RefObject(newChunkList);

            newChunkList->SortedBy() = snapshotChunkList->SortedBy();
            chunkManager->AttachToChunkList(newChunkList, snapshotChunkList);

            auto* deltaChunkList = chunkManager->CreateChunkList();
            chunkManager->AttachToChunkList(newChunkList, deltaChunkList);

            objectManager->UnrefObject(snapshotChunkList);

            resultChunkList = deltaChunkList;

            LOG_DEBUG_UNLESS(IsRecovery(), "Table node is switched to \"append\" mode (NodeId: %s, NewChunkListId: %s, SnapshotChunkListId: %s, DeltaChunkListId: %s)",
                ~ToString(node->GetId()),
                ~ToString(newChunkList->GetId()),
                ~ToString(snapshotChunkList->GetId()),
                ~ToString(deltaChunkList->GetId()));
            break;
        }

        case ETableUpdateMode::Overwrite: {
            auto* oldChunkList = node->GetChunkList();
            YCHECK(oldChunkList->OwningNodes().erase(node) == 1);
            objectManager->UnrefObject(oldChunkList);

            auto* newChunkList = chunkManager->CreateChunkList();
            YCHECK(newChunkList->OwningNodes().insert(node).second);
            node->SetChunkList(newChunkList);
            objectManager->RefObject(newChunkList);

            resultChunkList = newChunkList;

            LOG_DEBUG_UNLESS(IsRecovery(), "Table node is switched to \"overwrite\" mode (NodeId: %s, NewChunkListId: %s)",
                ~ToString(node->GetId()),
                ~ToString(newChunkList->GetId()));
            break;
        }

        default:
            YUNREACHABLE();
    }

    node->SetUpdateMode(mode);

    SetModified();

    ToProto(response->mutable_chunk_list_id(), resultChunkList->GetId());
    context->SetResponseInfo("ChunkListId: %s", ~ToString(resultChunkList->GetId()));

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TTableNodeProxy, Fetch)
{
    context->SetRequestInfo("");

    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    const auto* node = GetThisTypedImpl();

    auto attributes = ConvertToAttributes(TYsonString(request->Attributes().GetYson("path_attributes")));
    auto channel = attributes->Get("channel", TChannel::Universal());
    auto lowerLimit = attributes->Get("lower_limit", TReadLimit());
    auto upperLimit = attributes->Get("upper_limit", TReadLimit());
    bool complement = attributes->Get("complement", false);

    auto* chunkList = node->GetChunkList();

    auto visitor = New<TFetchChunkVisitor>(
        Bootstrap,
        chunkList,
        context,
        channel);

    if (complement) {
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

    ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

    auto* node = LockThisTypedImpl();

    if (node->GetUpdateMode() != ETableUpdateMode::Overwrite) {
        THROW_ERROR_EXCEPTION("Table node must be in overwrite mode");
    }

    node->GetChunkList()->SortedBy() = keyColumns;

    SetModified();

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateTableNodeProxy(
    NCypressServer::INodeTypeHandlerPtr typeHandler,
    NCellMaster::TBootstrap* bootstrap,
    NTransactionServer::TTransaction* transaction,
    TTableNode* trunkNode)
{
    return New<TTableNodeProxy>(
        typeHandler,
        bootstrap,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

