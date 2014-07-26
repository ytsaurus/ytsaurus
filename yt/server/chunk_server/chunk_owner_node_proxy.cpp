#include "stdafx.h"
#include "private.h"
#include "chunk_owner_node_proxy.h"
#include "chunk.h"
#include "chunk_list.h"
#include "chunk_manager.h"
#include "chunk_tree_traversing.h"

#include <core/concurrency/scheduler.h>

#include <core/ytree/node.h>
#include <core/ytree/fluent.h>
#include <core/ytree/system_attribute_provider.h>
#include <core/ytree/attribute_helpers.h>

#include <core/erasure/codec.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/chunk_spec.h>

#include <ytlib/object_client/helpers.h>

#include <server/node_tracker_server/node_directory_builder.h>

namespace NYT {
namespace NChunkServer {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NCypressServer;
using namespace NTransactionServer;
using namespace NYson;
using namespace NYTree;
using namespace NNodeTrackerServer;
using namespace NVersionedTableClient;
using namespace NObjectClient;

using NChunkClient::TChannel;
using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TFetchChunkVisitor
    : public IChunkVisitor
{
public:
    typedef NRpc::TTypedServiceContext<TReqFetch, TRspFetch> TCtxFetch;
    typedef TIntrusivePtr<TCtxFetch> TCtxFetchPtr;

    TFetchChunkVisitor(
        NCellMaster::TBootstrap* bootstrap,
        TChunkList* chunkList,
        TCtxFetchPtr context,
        const TChannel& channel,
        bool fetchParityReplicas)
        : Bootstrap_(bootstrap)
        , ChunkList_(chunkList)
        , Context_(context)
        , Channel_(channel)
        , FetchParityReplicas_(fetchParityReplicas)
        , NodeDirectoryBuilder_(context->Response().mutable_node_directory())
    {
        if (!Context_->Request().fetch_all_meta_extensions()) {
            for (int tag : Context_->Request().extension_tags()) {
                ExtensionTags_.insert(tag);
            }
        }
    }

    void StartSession(const TReadLimit& lowerBound, const TReadLimit& upperBound)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        ++SessionCount_;

        TraverseChunkTree(
            CreatePreemptableChunkTraverserCallbacks(Bootstrap_),
            this,
            ChunkList_,
            lowerBound,
            upperBound);
    }

    void Complete()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
            YCHECK(!Completed_);

        Completed_ = true;
        if (SessionCount_ == 0 && !Finished_) {
            ReplySuccess();
        }
    }

private:
    NCellMaster::TBootstrap* Bootstrap_;
    TChunkList* ChunkList_;
    TCtxFetchPtr Context_;
    TChannel Channel_;
    bool FetchParityReplicas_;

    yhash_set<int> ExtensionTags_;
    TNodeDirectoryBuilder NodeDirectoryBuilder_;
    int SessionCount_ = 0;
    bool Completed_ = false;
    bool Finished_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void ReplySuccess()
    {
        YCHECK(!Finished_);
        Finished_ = true;

        auto* chunkSpecs = Context_->Response().mutable_chunks();
        int chunkCount = chunkSpecs->size();

        // Update the upper limit for the last journal chunk.
        if (chunkCount > 0) {
            auto* lastChunkSpec = chunkSpecs->Mutable(chunkCount - 1);
            auto chunkId = FromProto<TChunkId>(lastChunkSpec->chunk_id());
            if (TypeFromId(chunkId) == EObjectType::JournalChunk) {
                auto chunkManager = Bootstrap_->GetChunkManager();
                auto* chunk = chunkManager->FindChunk(chunkId);
                if (!chunk) {
                    Context_->Reply(TError(
                        NRpc::EErrorCode::Unavailable,
                        "Optimistic locking failed for chunk %v",
                        chunkId));
                    return;
                }

                auto lowerLimit = FromProto<TReadLimit>(lastChunkSpec->lower_limit());
                auto upperLimit = FromProto<TReadLimit>(lastChunkSpec->upper_limit());

                auto result = WaitFor(chunkManager->GetChunkQuorumRowCount(chunk));
                THROW_ERROR_EXCEPTION_IF_FAILED(result);

                i64 quorumRowCount = result.Value();
                i64 specLowerLimit = lowerLimit.HasRowIndex() ? lowerLimit.GetRowIndex() : 0;
                i64 specUpperLimit = upperLimit.HasRowIndex() ? upperLimit.GetRowIndex() : std::numeric_limits<i64>::max();

                i64 adjustedUpperLimit = std::min(quorumRowCount, specUpperLimit);
                if (adjustedUpperLimit > specLowerLimit) {
                    upperLimit.SetRowIndex(adjustedUpperLimit);
                    ToProto(lastChunkSpec->mutable_upper_limit(), upperLimit);
                } else {
                    chunkSpecs->RemoveLast();
                }
            }
        }

        Context_->SetResponseInfo("ChunkCount: %v", chunkCount);
        Context_->Reply();
    }

    void ReplyError(const TError& error)
    {
        if (Finished_)
            return;

        Finished_ = true;

        Context_->Reply(error);
    }


    virtual bool OnChunk(
        TChunk* chunk,
        i64 rowIndex,
        const TReadLimit& startLimit,
        const TReadLimit& endLimit) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto chunkManager = Bootstrap_->GetChunkManager();

        if (!chunk->IsConfirmed()) {
            ReplyError(TError("Cannot fetch an object containing an unconfirmed chunk %v",
                chunk->GetId()));
            return false;
        }

        auto* chunkSpec = Context_->Response().add_chunks();

        chunkSpec->set_table_row_index(rowIndex);

        if (!Channel_.IsUniversal()) {
            ToProto(chunkSpec->mutable_channel(), Channel_);
        }

        auto erasureCodecId = chunk->GetErasureCodec();
        int firstInfeasibleReplicaIndex =
            erasureCodecId == NErasure::ECodec::None || FetchParityReplicas_
                ? std::numeric_limits<int>::max() // all replicas are feasible
                : NErasure::GetCodec(erasureCodecId)->GetDataPartCount();

        auto replicas = chunk->GetReplicas();
        for (auto replica : replicas) {
            if (replica.GetIndex() < firstInfeasibleReplicaIndex) {
                NodeDirectoryBuilder_.Add(replica);
                chunkSpec->add_replicas(NYT::ToProto<ui32>(replica));
            }
        }

        ToProto(chunkSpec->mutable_chunk_id(), chunk->GetId());
        chunkSpec->set_erasure_codec(erasureCodecId);

        chunkSpec->mutable_chunk_meta()->set_type(chunk->ChunkMeta().type());
        chunkSpec->mutable_chunk_meta()->set_version(chunk->ChunkMeta().version());

        if (Context_->Request().fetch_all_meta_extensions()) {
            *chunkSpec->mutable_chunk_meta()->mutable_extensions() = chunk->ChunkMeta().extensions();
        } else {
            FilterProtoExtensions(
                chunkSpec->mutable_chunk_meta()->mutable_extensions(),
                chunk->ChunkMeta().extensions(),
                ExtensionTags_);
        }

        // Try to keep responses small -- avoid producing redundant limits.
        if (!IsTrivial(startLimit)) {
            ToProto(chunkSpec->mutable_lower_limit(), startLimit);
        }
        if (!IsTrivial(endLimit)) {
            ToProto(chunkSpec->mutable_upper_limit(), endLimit);
        }

        return true;
    }

    virtual void OnError(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        --SessionCount_;
            YCHECK(SessionCount_ >= 0);

        ReplyError(error);
    }

    virtual void OnFinish() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        --SessionCount_;
            YCHECK(SessionCount_ >= 0);

        if (Completed_ && !Finished_ && SessionCount_ == 0) {
            ReplySuccess();
        }
    }

};

typedef TIntrusivePtr<TFetchChunkVisitor> TFetchChunkVisitorPtr;

////////////////////////////////////////////////////////////////////////////////

class TChunkVisitorBase
    : public IChunkVisitor
{
public:
    TAsyncError Run()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TraverseChunkTree(
            CreatePreemptableChunkTraverserCallbacks(Bootstrap),
            this,
            ChunkList);

        return Promise;
    }

protected:
    NCellMaster::TBootstrap* Bootstrap;
    IYsonConsumer* Consumer;
    TChunkList* ChunkList;
    TPromise<TError> Promise;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    TChunkVisitorBase(
        NCellMaster::TBootstrap* bootstrap,
        TChunkList* chunkList,
        IYsonConsumer* consumer)
        : Bootstrap(bootstrap)
        , Consumer(consumer)
        , ChunkList(chunkList)
        , Promise(NewPromise<TError>())
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
    }

    virtual void OnError(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

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

private:
    virtual bool OnChunk(
        TChunk* chunk,
        i64 rowIndex,
        const TReadLimit& startLimit,
        const TReadLimit& endLimit) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        UNUSED(rowIndex);
        UNUSED(startLimit);
        UNUSED(endLimit);

        Consumer->OnListItem();
        Consumer->OnStringScalar(ToString(chunk->GetId()));

        return true;
    }

    virtual void OnFinish() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

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

private:
    virtual bool OnChunk(
        TChunk* chunk,
        i64 rowIndex,
        const TReadLimit& startLimit,
        const TReadLimit& endLimit) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        UNUSED(rowIndex);
        UNUSED(startLimit);
        UNUSED(endLimit);

        CodecInfo_[CodecExtractor_(chunk)].Accumulate(chunk->GetStatistics());
        return true;
    }

    virtual void OnFinish() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        BuildYsonFluently(Consumer)
            .DoMapFor(CodecInfo_, [=] (TFluentMap fluent, const typename TCodecInfoMap::value_type& pair) {
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

    typedef yhash_map<typename TCodecExtractor::TValue, TChunkTreeStatistics> TCodecInfoMap;
    TCodecInfoMap CodecInfo_;

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
            .Value(chunkList->GetId());
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
    IYsonConsumer* consumer)
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
    const TNullable<TYsonString>& oldValue,
    const TNullable<TYsonString>& newValue)
{
    UNUSED(oldValue);

    if (key == "compression_codec") {
        if (!newValue) {
            ThrowCannotRemoveAttribute(key);
        }
        ParseEnum<NCompression::ECodec>(ConvertTo<Stroka>(newValue.Get()));
        return;
    }

    if (key == "erasure_codec") {
        if (!newValue) {
            ThrowCannotRemoveAttribute(key);
        }
        ParseEnum<NErasure::ECodec>(ConvertTo<Stroka>(newValue.Get()));
        return;
    }
}

bool TChunkOwnerNodeProxy::SetBuiltinAttribute(
    const Stroka& key,
    const TYsonString& value)
{
    auto chunkManager = Bootstrap->GetChunkManager();

    if (key == "replication_factor") {
        ValidateNoTransaction();
        int replicationFactor = ConvertTo<int>(value);
        if (replicationFactor < MinReplicationFactor ||
            replicationFactor > MaxReplicationFactor)
        {
            THROW_ERROR_EXCEPTION("\"replication_factor\" must be in range [%v,%v]",
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
                chunkManager->ScheduleChunkPropertiesUpdate(node->GetChunkList());
            }
        }
        return true;
    }

    if (key == "vital") {
        ValidateNoTransaction();
        bool vital = ConvertTo<bool>(value);

        auto* node = GetThisTypedImpl<TChunkOwnerBase>();
        YCHECK(node->IsTrunk());

        if (node->GetVital() != vital) {
            node->SetVital(vital);

            if (IsLeader()) {
                chunkManager->ScheduleChunkPropertiesUpdate(node->GetChunkList());
            }
        }

        return true;
    }

    return TNontemplateCypressNodeProxyBase::SetBuiltinAttribute(key, value);
}

void TChunkOwnerNodeProxy::ValidateFetchParameters(
    const TChannel& /*channel*/,
    const TReadLimit& /*upperLimit*/,
    const TReadLimit& /*lowerLimit*/)
{ }

void TChunkOwnerNodeProxy::Clear()
{ }

void TChunkOwnerNodeProxy::ValidatePrepareForUpdate()
{
    const auto* node = GetThisTypedImpl<TChunkOwnerBase>();
    if (node->GetUpdateMode() != EUpdateMode::None) {
        THROW_ERROR_EXCEPTION("Node is already in %Qlv mode",
            node->GetUpdateMode());
    }
}

void TChunkOwnerNodeProxy::ValidateFetch()
{ }

DEFINE_YPATH_SERVICE_METHOD(TChunkOwnerNodeProxy, PrepareForUpdate)
{
    DeclareMutating();

    auto mode = EUpdateMode(request->mode());
    YCHECK(mode == EUpdateMode::Append || mode == EUpdateMode::Overwrite);

    context->SetRequestInfo("Mode: %v", mode);

    ValidateTransaction();
    ValidatePermission(
        EPermissionCheckScope::This,
        NSecurityServer::EPermission::Write);

    auto* node = LockThisTypedImpl<TChunkOwnerBase>(GetLockMode(mode));
    ValidatePrepareForUpdate();

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

            chunkManager->AttachToChunkList(newChunkList, snapshotChunkList);

            auto* deltaChunkList = chunkManager->CreateChunkList();
            chunkManager->AttachToChunkList(newChunkList, deltaChunkList);

            objectManager->UnrefObject(snapshotChunkList);

            resultChunkList = deltaChunkList;

            LOG_DEBUG_UNLESS(
                IsRecovery(),
                "Node is switched to \"append\" mode (NodeId: %v, NewChunkListId: %v, SnapshotChunkListId: %v, DeltaChunkListId: %v)",
                node->GetId(),
                newChunkList->GetId(),
                snapshotChunkList->GetId(),
                deltaChunkList->GetId());

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

            Clear();

            LOG_DEBUG_UNLESS(
                IsRecovery(),
                "Node is switched to \"overwrite\" mode (NodeId: %v, NewChunkListId: %v)",
                node->GetId(),
                newChunkList->GetId());
            break;
        }

        default:
            YUNREACHABLE();
    }

    node->SetUpdateMode(mode);

    SetModified();

    ToProto(response->mutable_chunk_list_id(), resultChunkList->GetId());
    context->SetResponseInfo("ChunkListId: %v",
        resultChunkList->GetId());

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TChunkOwnerNodeProxy, Fetch)
{
    DeclareNonMutating();

    context->SetRequestInfo();

    ValidatePermission(
        EPermissionCheckScope::This,
        NSecurityServer::EPermission::Read);
    ValidateFetch();

    auto channel = request->has_channel()
        ? NYT::FromProto<TChannel>(request->channel())
        : TChannel::Universal();
    auto lowerLimit = request->has_lower_limit()
        ? NYT::FromProto<TReadLimit>(request->lower_limit())
        : TReadLimit();
    auto upperLimit = request->has_upper_limit()
        ? NYT::FromProto<TReadLimit>(request->upper_limit())
        : TReadLimit();
    bool fetchParityReplicas = request->fetch_parity_replicas();

    ValidateFetchParameters(channel, lowerLimit, upperLimit);

    const auto* node = GetThisTypedImpl<TChunkOwnerBase>();
    auto* chunkList = node->GetChunkList();

    auto visitor = New<TFetchChunkVisitor>(
        Bootstrap,
        chunkList,
        context,
        channel,
        fetchParityReplicas);

    if (request->complement()) {
        if (lowerLimit.HasRowIndex() || lowerLimit.HasKey()) {
            visitor->StartSession(TReadLimit(), lowerLimit);
        }
        if (upperLimit.HasRowIndex() || upperLimit.HasKey()) {
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
