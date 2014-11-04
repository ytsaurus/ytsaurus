#include "stdafx.h"
#include "private.h"
#include "chunk_owner_node_proxy.h"
#include "chunk.h"
#include "chunk_list.h"
#include "chunk_manager.h"
#include "chunk_tree_traversing.h"
#include "node_directory_builder.h"

#include <core/ytree/node.h>
#include <core/ytree/fluent.h>
#include <core/ytree/system_attribute_provider.h>
#include <core/ytree/attribute_helpers.h>

#include <core/erasure/codec.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/chunk_spec.h>

#include <server/cell_master/config.h>

namespace NYT {
namespace NChunkServer {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NCypressServer;
using namespace NTransactionServer;
using namespace NYson;
using namespace NYTree;

using NChunkClient::TChannel;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TFetchChunkVisitor
    : public IChunkVisitor
{
public:
    typedef NYT::NRpc::TTypedServiceContext<TReqFetch, TRspFetch> TCtxFetch;

    typedef TIntrusivePtr<TCtxFetch> TCtxFetchPtr;

    TFetchChunkVisitor(
        NCellMaster::TBootstrap* bootstrap,
        TChunkList* chunkList,
        TCtxFetchPtr context,
        const TChannel& channel,
        bool fetchParityReplicas);

    void StartSession(
        const TReadLimit& lowerBound,
        const TReadLimit& upperBound);

    void Complete();

private:
    NCellMaster::TBootstrap* Bootstrap;
    TChunkList* ChunkList;
    TCtxFetchPtr Context;
    TChannel Channel;
    bool FetchParityReplicas;

    yhash_set<int> ExtensionTags;
    TNodeDirectoryBuilder NodeDirectoryBuilder;
    int SessionCount;
    bool Completed;
    bool Finished;

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

    void Reply();
    void ReplyError(const TError& error);

    virtual bool OnChunk(
        TChunk* chunk,
        i64 rowIndex,
        const TReadLimit& startLimit,
        const TReadLimit& endLimit) override;

    virtual void OnError(const TError& error) override;
    virtual void OnFinish() override;

};

typedef TIntrusivePtr<TFetchChunkVisitor> TFetchChunkVisitorPtr;

////////////////////////////////////////////////////////////////////////////////

TFetchChunkVisitor::TFetchChunkVisitor(
    NCellMaster::TBootstrap* bootstrap,
    TChunkList* chunkList,
    TCtxFetchPtr context,
    const TChannel& channel,
    bool fetchParityReplicas)
    : Bootstrap(bootstrap)
    , ChunkList(chunkList)
    , Context(context)
    , Channel(channel)
    , FetchParityReplicas(fetchParityReplicas)
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

void TFetchChunkVisitor::StartSession(
    const TReadLimit& lowerBound,
    const TReadLimit& upperBound)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    ++SessionCount;

    TraverseChunkTree(
        CreateTraverserCallbacks(Bootstrap),
        this,
        ChunkList,
        lowerBound,
        upperBound);
}

void TFetchChunkVisitor::Complete()
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YCHECK(!Completed);

    Completed = true;
    if (SessionCount == 0 && !Finished) {
        Reply();
    }
}

void TFetchChunkVisitor::Reply()
{
    Context->SetResponseInfo("ChunkCount: %d", Context->Response().chunks_size());
    Context->Reply();
    Finished = true;
}

bool TFetchChunkVisitor::OnChunk(
    TChunk* chunk,
    i64 rowIndex,
    const TReadLimit& startLimit,
    const TReadLimit& endLimit)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto chunkManager = Bootstrap->GetChunkManager();
    const auto& config = Bootstrap->GetConfig()->ChunkManager;

    if (!chunk->IsConfirmed()) {
        ReplyError(TError("Cannot fetch a table containing an unconfirmed chunk %s",
            ~ToString(chunk->GetId())));
        return false;
    }

    auto* chunkSpec = Context->Response().add_chunks();

    chunkSpec->set_table_row_index(rowIndex);

    if (!Channel.IsUniversal()) {
        *chunkSpec->mutable_channel() = Channel.ToProto();
    }

    // Default value for non-erasure chunks.
    int firstSkippedPartIndex = 1;
    auto erasureCodecId = chunk->GetErasureCodec();
    if (erasureCodecId != NErasure::ECodec::None) {
        auto erasureCodec = NErasure::GetCodec(erasureCodecId);
        firstSkippedPartIndex = FetchParityReplicas
            ? erasureCodec->GetTotalPartCount()
            : erasureCodec->GetDataPartCount();
    }

    TSmallVector<TNodePtrWithIndex, TypicalReplicaCount> replicas;
    auto addReplica = [&] (TNodePtrWithIndex replica) -> bool {
        if (replica.GetIndex() < firstSkippedPartIndex) {
            replicas.push_back(replica);
            return true;
        } else {
            return false;
        }
    };

    for (auto replica : chunk->StoredReplicas()) {
        addReplica(replica);
    }

    if (chunk->CachedReplicas()) {
        int cachedReplicaCount = 0;
        for (auto replica : *chunk->CachedReplicas()) {
            if (cachedReplicaCount >= config->MaxCachedReplicasPerFetch)
                break;
            if (addReplica(replica)) {
                ++cachedReplicaCount;
            }
        }
    }

    for (auto replica : replicas) {
        NodeDirectoryBuilder.Add(replica);
        chunkSpec->add_replicas(NYT::ToProto<ui32>(replica));
    }

    ToProto(chunkSpec->mutable_chunk_id(), chunk->GetId());
    chunkSpec->set_erasure_codec(erasureCodecId);

    if (Context->Request().fetch_all_meta_extensions()) {
        *chunkSpec->mutable_extensions() = chunk->ChunkMeta().extensions();
    } else {
        FilterProtoExtensions(
            chunkSpec->mutable_extensions(),
            chunk->ChunkMeta().extensions(),
            ExtensionTags);
    }

    // Try to keep responses small -- avoid producing redundant limits.
    if (IsNontrivial(startLimit)) {
        *chunkSpec->mutable_start_limit() = startLimit;
    }
    if (IsNontrivial(endLimit)) {
        *chunkSpec->mutable_end_limit() = endLimit;
    }

    return true;
}

void TFetchChunkVisitor::OnError(const TError& error)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    --SessionCount;
    YCHECK(SessionCount >= 0);

    ReplyError(error);
}

void TFetchChunkVisitor::OnFinish()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    --SessionCount;
    YCHECK(SessionCount >= 0);

    if (Completed && !Finished && SessionCount == 0) {
        Reply();
    }
}

void TFetchChunkVisitor::ReplyError(const TError& error)
{
    if (Finished)
        return;

    Context->Reply(error);
    Finished = true;
}

////////////////////////////////////////////////////////////////////////////////

class TChunkVisitorBase
    : public IChunkVisitor
{
public:
    TAsyncError Run()
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        TraverseChunkTree(
            CreateTraverserCallbacks(Bootstrap),
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

////////////////////////////////////////////////////////////////////////////////

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
        i64 rowIndex,
        const TReadLimit& startLimit,
        const TReadLimit& endLimit) override
    {
        VERIFY_THREAD_AFFINITY(StateThread);
        UNUSED(rowIndex);
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

TAsyncError GetChunkIdsAttribute(
    NCellMaster::TBootstrap* bootstrap,
    TChunkList* chunkList,
    IYsonConsumer* consumer)
{
    auto visitor = New<TChunkIdsAttributeVisitor>(
        bootstrap,
        const_cast<TChunkList*>(chunkList),
        consumer);
    return visitor->Run();
}

////////////////////////////////////////////////////////////////////////////////

template <class TCodecExtractor>
class TCodecStatisticsVisitor
    : public TChunkVisitorBase
{
public:
    TCodecStatisticsVisitor(
        NCellMaster::TBootstrap* bootstrap,
        TChunkList* chunkList,
        IYsonConsumer* consumer)
        : TChunkVisitorBase(bootstrap, chunkList, consumer)
        , CodecExtractor_()
    { }

    virtual bool OnChunk(
        TChunk* chunk,
        i64 rowIndex,
        const TReadLimit& startLimit,
        const TReadLimit& endLimit) override
    {
        VERIFY_THREAD_AFFINITY(StateThread);
        UNUSED(rowIndex);
        UNUSED(startLimit);
        UNUSED(endLimit);

        CodecInfo[CodecExtractor_(chunk)].Accumulate(chunk->GetStatistics());
        return true;
    }

    virtual void OnFinish() override
    {
        VERIFY_THREAD_AFFINITY(StateThread);

        BuildYsonFluently(Consumer)
            .DoMapFor(CodecInfo, [=] (TFluentMap fluent, const typename TCodecInfoMap::value_type& pair) {
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
    typedef yhash_map<typename TCodecExtractor::TValue, TChunkTreeStatistics> TCodecInfoMap;
    TCodecInfoMap CodecInfo;

    TCodecExtractor CodecExtractor_;
};

template <class TVisitor>
TAsyncError ComputeCodecStatistics(
    NCellMaster::TBootstrap* bootstrap,
    TChunkList* chunkList,
    IYsonConsumer* consumer)
{
    auto visitor = New<TVisitor>(
        bootstrap,
        const_cast<TChunkList*>(chunkList),
        consumer);
    return visitor->Run();
}

////////////////////////////////////////////////////////////////////////////////

TChunkOwnerNodeProxy::TChunkOwnerNodeProxy(
    INodeTypeHandlerPtr typeHandler,
    NCellMaster::TBootstrap* bootstrap,
    TTransaction* transaction,
    TChunkOwnerBase* trunkNode)
    : TNontemplateCypressNodeProxyBase(
        typeHandler,
        bootstrap,
        transaction,
        trunkNode)
{ }

bool TChunkOwnerNodeProxy::DoInvoke(NRpc::IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(PrepareForUpdate);
    DISPATCH_YPATH_HEAVY_SERVICE_METHOD(Fetch);
    return TNontemplateCypressNodeProxyBase::DoInvoke(context);
}

bool TChunkOwnerNodeProxy::IsWriteRequest(NRpc::IServiceContextPtr context) const
{
    DECLARE_YPATH_SERVICE_WRITE_METHOD(PrepareForUpdate);
    return TNontemplateCypressNodeProxyBase::IsWriteRequest(context);
}

NSecurityServer::TClusterResources TChunkOwnerNodeProxy::GetResourceUsage() const
{
    const auto* node = GetThisTypedImpl<TChunkOwnerBase>();
    const auto* chunkList = node->GetChunkList();
    i64 diskSpace = chunkList->Statistics().RegularDiskSpace * node->GetReplicationFactor() +
        chunkList->Statistics().ErasureDiskSpace;
    return NSecurityServer::TClusterResources(diskSpace, 1);
}

void TChunkOwnerNodeProxy::ListSystemAttributes(std::vector<TAttributeInfo>* attributes)
{
    attributes->push_back("chunk_list_id");
    attributes->push_back(TAttributeInfo("chunk_ids", true, true));
    attributes->push_back(TAttributeInfo("compression_statistics", true, true));
    attributes->push_back(TAttributeInfo("erasure_statistics", true, true));
    attributes->push_back("chunk_count");
    attributes->push_back("uncompressed_data_size");
    attributes->push_back("compressed_data_size");
    attributes->push_back("compression_ratio");
    attributes->push_back(TAttributeInfo("compression_codec", true, false, true));
    attributes->push_back(TAttributeInfo("erasure_codec", true, false, true));
    attributes->push_back("update_mode");
    attributes->push_back("replication_factor");
    attributes->push_back("vital");
    TNontemplateCypressNodeProxyBase::ListSystemAttributes(attributes);
}

bool TChunkOwnerNodeProxy::GetBuiltinAttribute(
    const Stroka& key,
    NYson::IYsonConsumer* consumer)
{
    const auto* node = GetThisTypedImpl<TChunkOwnerBase>();
    const auto* chunkList = node->GetChunkList();
    const auto& statistics = chunkList->Statistics();

    if (key == "chunk_list_id") {
        NYTree::BuildYsonFluently(consumer)
            .Value(ToString(chunkList->GetId()));
        return true;
    }

    if (key == "chunk_count") {
        NYTree::BuildYsonFluently(consumer)
            .Value(statistics.ChunkCount);
        return true;
    }

    if (key == "uncompressed_data_size") {
        NYTree::BuildYsonFluently(consumer)
            .Value(statistics.UncompressedDataSize);
        return true;
    }

    if (key == "compressed_data_size") {
        NYTree::BuildYsonFluently(consumer)
            .Value(statistics.CompressedDataSize);
        return true;
    }

    if (key == "compression_ratio") {
        double ratio =
            statistics.UncompressedDataSize > 0
            ? static_cast<double>(statistics.CompressedDataSize) / statistics.UncompressedDataSize
            : 0;
        NYTree::BuildYsonFluently(consumer)
            .Value(ratio);
        return true;
    }

    if (key == "update_mode") {
        NYTree::BuildYsonFluently(consumer)
            .Value(FormatEnum(node->GetUpdateMode()));
        return true;
    }

    if (key == "replication_factor") {
        NYTree::BuildYsonFluently(consumer)
            .Value(node->GetReplicationFactor());
        return true;
    }

    if (key == "vital") {
        NYTree::BuildYsonFluently(consumer)
            .Value(node->GetVital());
        return true;
    }

    return TNontemplateCypressNodeProxyBase::GetBuiltinAttribute(key, consumer);
}

TAsyncError TChunkOwnerNodeProxy::GetBuiltinAttributeAsync(
    const Stroka& key,
    NYson::IYsonConsumer* consumer)
{
    const auto* node = GetThisTypedImpl<TChunkOwnerBase>();
    const auto* chunkList = node->GetChunkList();

    if (key == "chunk_ids") {
        return GetChunkIdsAttribute(
            Bootstrap,
            const_cast<TChunkList*>(chunkList),
            consumer);
    }

    if (key == "compression_statistics") {
        struct TExtractCompressionCodec
        {
            typedef NCompression::ECodec TValue;
            TValue operator() (const TChunk* chunk) {
                const auto& chunkMeta = chunk->ChunkMeta();
                auto miscExt = GetProtoExtension<TMiscExt>(chunkMeta.extensions());
                return TValue(miscExt.compression_codec());
            }
        };
        typedef TCodecStatisticsVisitor<TExtractCompressionCodec> TCompressionStatisticsVisitor;

        return ComputeCodecStatistics<TCompressionStatisticsVisitor>(
            Bootstrap,
            const_cast<TChunkList*>(chunkList),
            consumer);
    }

    if (key == "erasure_statistics") {
        struct TExtractErasureCodec
        {
            typedef NErasure::ECodec TValue;
            TValue operator() (const TChunk* chunk) {
                return chunk->GetErasureCodec();
            }
        };
        typedef TCodecStatisticsVisitor<TExtractErasureCodec> TErasureStatisticsVisitor;

        return ComputeCodecStatistics<TErasureStatisticsVisitor>(
            Bootstrap,
            const_cast<TChunkList*>(chunkList),
            consumer);
    }

    return TNontemplateCypressNodeProxyBase::GetBuiltinAttributeAsync(key, consumer);
}

void TChunkOwnerNodeProxy::ValidateCustomAttributeUpdate(
    const Stroka& key,
    const TNullable<NYTree::TYsonString>& oldValue,
    const TNullable<NYTree::TYsonString>& newValue)
{
    UNUSED(oldValue);

    if (key == "compression_codec") {
        if (!newValue) {
            NYTree::ThrowCannotRemoveAttribute(key);
        }
        ParseEnum<NCompression::ECodec>(ConvertTo<Stroka>(newValue.Get()));
        return;
    }

    if (key == "erasure_codec") {
        if (!newValue) {
            NYTree::ThrowCannotRemoveAttribute(key);
        }
        ParseEnum<NErasure::ECodec>(ConvertTo<Stroka>(newValue.Get()));
        return;
    }
}

bool TChunkOwnerNodeProxy::SetBuiltinAttribute(
    const Stroka& key,
    const NYTree::TYsonString& value)
{
    auto chunkManager = Bootstrap->GetChunkManager();

    if (key == "replication_factor") {
        ValidateNoTransaction();
        int replicationFactor = NYTree::ConvertTo<int>(value);
        const int MinReplicationFactor = 1;
        const int MaxReplicationFactor = 10;
        if (replicationFactor < MinReplicationFactor || replicationFactor > MaxReplicationFactor) {
            THROW_ERROR_EXCEPTION("Value must be in range [%d,%d]",
                MinReplicationFactor,
                MaxReplicationFactor);
        }

        auto* node = GetThisTypedImpl<TChunkOwnerBase>();
        YCHECK(node->IsTrunk());

        if (node->GetReplicationFactor() != replicationFactor) {
            node->SetReplicationFactor(replicationFactor);

            auto securityManager = Bootstrap->GetSecurityManager();
            securityManager->UpdateAccountNodeUsage(node);

            if (IsLeader()) {
                chunkManager->SchedulePropertiesUpdate(node->GetChunkList());
            }
        }
        return true;
    }

    if (key == "vital") {
        ValidateNoTransaction();
        bool vital = NYTree::ConvertTo<bool>(value);

        auto* node = GetThisTypedImpl<TChunkOwnerBase>();
        YCHECK(node->IsTrunk());

        if (node->GetVital() != vital) {
            node->SetVital(vital);

            if (IsLeader()) {
                chunkManager->SchedulePropertiesUpdate(node->GetChunkList());
            }
        }

        return true;
    }

    return TNontemplateCypressNodeProxyBase::SetBuiltinAttribute(key, value);
}

void TChunkOwnerNodeProxy::ValidatePathAttributes(
    const TNullable<TChannel>& channel,
    const TReadLimit& upperLimit,
    const TReadLimit& lowerLimit)
{
    UNUSED(channel);
    UNUSED(upperLimit);
    UNUSED(lowerLimit);
}

DEFINE_RPC_SERVICE_METHOD(TChunkOwnerNodeProxy, PrepareForUpdate)
{
    auto mode = EUpdateMode(request->mode());
    YCHECK(mode == EUpdateMode::Append || mode == EUpdateMode::Overwrite);

    context->SetRequestInfo("Mode: %s", ~FormatEnum(mode));

    ValidateTransaction();
    ValidatePermission(
        NYTree::EPermissionCheckScope::This,
        NSecurityServer::EPermission::Write);

    auto* node = LockThisTypedImpl<TChunkOwnerBase>(GetLockMode(mode));

    if (node->GetUpdateMode() != EUpdateMode::None) {
        THROW_ERROR_EXCEPTION("Node is already in %s mode",
            ~FormatEnum(node->GetUpdateMode()).Quote());
    }

    auto chunkManager = Bootstrap->GetChunkManager();
    auto objectManager = Bootstrap->GetObjectManager();

    TChunkList* resultChunkList;
    switch (mode) {
        case EUpdateMode::Append: {
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

            LOG_DEBUG_UNLESS(
                IsRecovery(),
                "Node is switched to \"append\" mode (NodeId: %s, NewChunkListId: %s, SnapshotChunkListId: %s, DeltaChunkListId: %s)",
                ~ToString(node->GetId()),
                ~ToString(newChunkList->GetId()),
                ~ToString(snapshotChunkList->GetId()),
                ~ToString(deltaChunkList->GetId()));

            break;
        }

        case EUpdateMode::Overwrite: {
            auto* oldChunkList = node->GetChunkList();
            YCHECK(oldChunkList->OwningNodes().erase(node) == 1);
            objectManager->UnrefObject(oldChunkList);

            auto* newChunkList = chunkManager->CreateChunkList();
            YCHECK(newChunkList->OwningNodes().insert(node).second);
            node->SetChunkList(newChunkList);
            objectManager->RefObject(newChunkList);

            resultChunkList = newChunkList;

            LOG_DEBUG_UNLESS(
                IsRecovery(),
                "Node is switched to \"overwrite\" mode (NodeId: %s, NewChunkListId: %s)",
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

DEFINE_RPC_SERVICE_METHOD(TChunkOwnerNodeProxy, Fetch)
{
    context->SetRequestInfo("");

    ValidatePermission(
        NYTree::EPermissionCheckScope::This,
        NSecurityServer::EPermission::Read);

    const auto* node = GetThisTypedImpl<TChunkOwnerBase>();

    auto attributes = NYTree::FromProto(request->attributes());
    auto channelAttribute = attributes->Find<TChannel>("channel");
    auto lowerLimit = attributes->Get("lower_limit", TReadLimit());
    auto upperLimit = attributes->Get("upper_limit", TReadLimit());
    bool complement = attributes->Get("complement", false);
    bool fetchParityReplicas = attributes->Get("fetch_parity_replicas", false);

    ValidatePathAttributes(channelAttribute, lowerLimit, upperLimit);

    auto channel = channelAttribute.Get(TChannel::Universal());

    auto* chunkList = node->GetChunkList();

    auto visitor = New<TFetchChunkVisitor>(
        Bootstrap,
        chunkList,
        context,
        channel,
        fetchParityReplicas);

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

////////////////////////////////////////////////////////////////////////////////

} // NChunkServer
} // NYT
