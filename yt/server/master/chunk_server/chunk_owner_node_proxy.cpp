#include "chunk.h"
#include "chunk_list.h"
#include "chunk_manager.h"
#include "chunk_owner_node_proxy.h"
#include "chunk_visitor.h"
#include "config.h"
#include "helpers.h"
#include "medium.h"
#include "private.h"

#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/multicell_manager.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/config_manager.h>

#include <yt/server/master/node_tracker_server/node_directory_builder.h>

#include <yt/server/lib/misc/interned_attributes.h>

#include <yt/server/master/object_server/object.h>

#include <yt/server/master/table_server/shared_table_schema.h>

#include <yt/server/master/security_server/security_manager.h>
#include <yt/server/master/security_server/security_tags.h>
#include <yt/server/master/security_server/access_log.h>

#include <yt/server/master/transaction_server/proto/transaction_manager.pb.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>

#include <yt/ytlib/file_client/file_chunk_writer.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/library/erasure/codec.h>

#include <yt/core/misc/numeric_helpers.h>

#include <yt/core/ytree/helpers.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/node.h>
#include <yt/core/ytree/system_attribute_provider.h>

#include <type_traits>

namespace NYT::NChunkServer {

using namespace NCrypto;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NChunkClient;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NCypressServer;
using namespace NNodeTrackerServer;
using namespace NNodeTrackerClient;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NYson;
using namespace NYTree;
using namespace NTableClient;

using NChunkClient::NProto::TReqFetch;
using NChunkClient::NProto::TRspFetch;
using NChunkClient::NProto::TMiscExt;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsAccessLoggedMethod(const TString& method)
{
    static const THashSet<TString> methodsForAccessLog = {
        "Fetch",
        "BeginUpload",
        "EndUpload"
    };
    return methodsForAccessLog.contains(method);
}

//! Adds #cellTag into #cellTags if the former is not a sentinel.
void InsertCellTag(TCellTagList* cellTags, TCellTag cellTag)
{
    if (cellTag >= MinValidCellTag && cellTag <= MaxValidCellTag) {
        cellTags->push_back(cellTag);
    }
}

//! Removes #cellTag from #cellTags if the former is present there.
void RemoveCellTag(TCellTagList* cellTags, TCellTag cellTag)
{
    cellTags->erase(
        std::remove(cellTags->begin(), cellTags->end(), cellTag),
        cellTags->end());
}

//! Sorts and removes duplicates from #cellTags.
void CanonizeCellTags(TCellTagList* cellTags)
{
    std::sort(cellTags->begin(), cellTags->end());
    cellTags->erase(
        std::unique(cellTags->begin(), cellTags->end()),
        cellTags->end());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TChunkOwnerNodeProxy::TFetchChunkVisitor
    : public IChunkVisitor
{
public:
    TFetchChunkVisitor(
        NCellMaster::TBootstrap* bootstrap,
        TChunkList* chunkList,
        TCtxFetchPtr rpcContext,
        TFetchContext&& fetchContext)
        : Bootstrap_(bootstrap)
        , ChunkList_(chunkList)
        , RpcContext_(std::move(rpcContext))
        , FetchContext_(std::move(fetchContext))
        , NodeDirectoryBuilder_(
            RpcContext_->Response().mutable_node_directory(),
            FetchContext_.AddressType)
    {
        const auto& request = RpcContext_->Request();
        if (!request.fetch_all_meta_extensions()) {
            ExtensionTags_.insert(request.extension_tags().begin(), request.extension_tags().end());
        }
    }

    void Run()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (FetchContext_.Ranges.empty()) {
            ReplySuccess();
            return;
        }

        TraverseCurrentRange();
    }

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    TChunkList* const ChunkList_;
    const TCtxFetchPtr RpcContext_;
    TFetchContext FetchContext_;

    int CurrentRangeIndex_ = 0;

    THashSet<int> ExtensionTags_;
    NNodeTrackerServer::TNodeDirectoryBuilder NodeDirectoryBuilder_;
    bool Finished_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void TraverseCurrentRange()
    {
        auto callbacks = CreatePreemptableChunkTraverserCallbacks(
            Bootstrap_,
            NCellMaster::EAutomatonThreadQueue::ChunkFetchingTraverser);
        TraverseChunkTree(
            std::move(callbacks),
            this,
            ChunkList_,
            FetchContext_.Ranges[CurrentRangeIndex_].LowerLimit(),
            FetchContext_.Ranges[CurrentRangeIndex_].UpperLimit());
    }

    void ReplySuccess()
    {
        YT_VERIFY(!Finished_);
        Finished_ = true;

        try {
            // Update upper limits for all returned journal chunks.
            auto& response = RpcContext_->Response();
            auto* chunkSpecs = response.mutable_chunks();
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            for (auto& chunkSpec : *chunkSpecs) {
                auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
                if (TypeFromId(chunkId) == EObjectType::JournalChunk) {
                    auto* chunk = chunkManager->FindChunk(chunkId);
                    if (!IsObjectAlive(chunk)) {
                        THROW_ERROR_EXCEPTION(
                            NChunkClient::EErrorCode::OptimisticLockFailure,
                            "Optimistic locking failed for chunk %v",
                            chunkId);
                    }

                    auto result = WaitFor(chunkManager->GetChunkQuorumInfo(chunk))
                        .ValueOrThrow();
                    i64 quorumRowCount = result.row_count();

                    auto lowerLimit = FromProto<TReadLimit>(chunkSpec.lower_limit());
                    if (!lowerLimit.HasRowIndex()) {
                        lowerLimit.SetRowIndex(0);
                    }
                    ToProto(chunkSpec.mutable_lower_limit(), lowerLimit);

                    auto upperLimit = FromProto<TReadLimit>(chunkSpec.upper_limit());
                    i64 upperLimitRowIndex = upperLimit.HasRowIndex() ? upperLimit.GetRowIndex() : std::numeric_limits<i64>::max();
                    upperLimit.SetRowIndex(std::min(upperLimitRowIndex, quorumRowCount));
                    ToProto(chunkSpec.mutable_upper_limit(), upperLimit);
                }
            }

            RpcContext_->SetResponseInfo("ChunkCount: %v", chunkSpecs->size());

            RpcContext_->Reply();
        } catch (const std::exception& ex) {
            RpcContext_->Reply(ex);
        }
    }

    void ReplyError(const TError& error)
    {
        if (Finished_)
            return;

        Finished_ = true;

        RpcContext_->Reply(error);
    }

    virtual bool OnChunk(
        TChunk* chunk,
        i64 rowIndex,
        std::optional<i32> tabletIndex,
        const TReadLimit& lowerLimit,
        const TReadLimit& upperLimit,
        TTransactionId timestampTransactionId) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& configManager = Bootstrap_->GetConfigManager();
        const auto& dynamicConfig = configManager->GetConfig()->ChunkManager;
        if (RpcContext_->Response().chunks_size() >= dynamicConfig->MaxChunksPerFetch) {
            ReplyError(TError("Attempt to fetch too many chunks in a single request")
                << TErrorAttribute("limit", dynamicConfig->MaxChunksPerFetch));
            return false;
        }

        if (!chunk->IsConfirmed()) {
            ReplyError(TError("Cannot fetch an object containing an unconfirmed chunk %v",
                chunk->GetId()));
            return false;
        }

        auto* chunkSpec = RpcContext_->Response().add_chunks();

        chunkSpec->set_table_row_index(rowIndex);
        if (tabletIndex.has_value()) {
            chunkSpec->set_tablet_index(*tabletIndex);
        }

        SmallVector<TNodePtrWithIndexes, TypicalReplicaCount> replicas;

        auto addJournalReplica = [&] (TNodePtrWithIndexes replica) {
            // For journal chunks, replica indexes are used to track states.
            // Hence we must replace index with #GenericChunkReplicaIndex.
            replicas.push_back(TNodePtrWithIndexes(replica.GetPtr(), GenericChunkReplicaIndex, replica.GetMediumIndex()));
            return true;
        };

        auto erasureCodecId = chunk->GetErasureCodec();
        int firstInfeasibleReplicaIndex = (erasureCodecId == NErasure::ECodec::None || FetchContext_.FetchParityReplicas)
            ? std::numeric_limits<int>::max() // all replicas are feasible
            : NErasure::GetCodec(erasureCodecId)->GetDataPartCount();

        auto addErasureReplica = [&] (TNodePtrWithIndexes replica) {
            if (replica.GetReplicaIndex() >= firstInfeasibleReplicaIndex) {
                return false;
            }
            replicas.push_back(replica);
            return true;
        };

        auto addRegularReplica = [&] (TNodePtrWithIndexes replica) {
            replicas.push_back(replica);
            return true;
        };

        std::function<bool(TNodePtrWithIndexes)> addReplica;
        switch (chunk->GetType()) {
            case EObjectType::Chunk:          addReplica = addRegularReplica; break;
            case EObjectType::ErasureChunk:   addReplica = addErasureReplica; break;
            case EObjectType::JournalChunk:   addReplica = addJournalReplica; break;
            default:                          YT_ABORT();
        }

        for (auto replica : chunk->StoredReplicas()) {
            addReplica(replica);
        }

        int cachedReplicaCount = 0;
        for (auto replica : chunk->CachedReplicas()) {
            if (cachedReplicaCount >= dynamicConfig->MaxCachedReplicasPerFetch) {
                break;
            }
            if (addReplica(replica)) {
                ++cachedReplicaCount;
            }
        }

        for (auto replica : replicas) {
            NodeDirectoryBuilder_.Add(replica);
            chunkSpec->add_replicas(NYT::ToProto<ui64>(replica));
        }

        ToProto(chunkSpec->mutable_chunk_id(), chunk->GetId());
        chunkSpec->set_erasure_codec(static_cast<int>(erasureCodecId));

        chunkSpec->mutable_chunk_meta()->set_type(chunk->ChunkMeta().type());
        chunkSpec->mutable_chunk_meta()->set_version(chunk->ChunkMeta().version());

        if (RpcContext_->Request().fetch_all_meta_extensions()) {
            *chunkSpec->mutable_chunk_meta()->mutable_extensions() = chunk->ChunkMeta().extensions();
        } else {
            FilterProtoExtensions(
                chunkSpec->mutable_chunk_meta()->mutable_extensions(),
                chunk->ChunkMeta().extensions(),
                ExtensionTags_);
        }

        // Try to keep responses small -- avoid producing redundant limits.
        if (!IsTrivial(lowerLimit)) {
            ToProto(chunkSpec->mutable_lower_limit(), lowerLimit);
        }
        if (!IsTrivial(upperLimit)) {
            ToProto(chunkSpec->mutable_upper_limit(), upperLimit);
        }

        chunkSpec->set_range_index(CurrentRangeIndex_);

        i64 lowerRowLimit = 0;
        if (lowerLimit.HasRowIndex()) {
            lowerRowLimit = lowerLimit.GetRowIndex();
        }
        i64 upperRowLimit = chunk->MiscExt().row_count();
        if (upperLimit.HasRowIndex()) {
            upperRowLimit = upperLimit.GetRowIndex();
        }

        // If one of row indexes is present, then fields row_count_override and
        // uncompressed_data_size_override estimate the chunk range
        // instead of the whole chunk.
        // To ensure the correct usage of this rule, row indexes should be
        // either both set or not.
        if (lowerLimit.HasRowIndex() && !upperLimit.HasRowIndex()) {
            chunkSpec->mutable_upper_limit()->set_row_index(upperRowLimit);
        }

        if (upperLimit.HasRowIndex() && !lowerLimit.HasRowIndex()) {
            chunkSpec->mutable_lower_limit()->set_row_index(lowerRowLimit);
        }

        chunkSpec->set_row_count_override(upperRowLimit - lowerRowLimit);
        i64 dataWeight = chunk->MiscExt().data_weight() > 0
            ? chunk->MiscExt().data_weight()
            : chunk->MiscExt().uncompressed_data_size();

        if (chunkSpec->row_count_override() >= chunk->MiscExt().row_count()) {
            chunkSpec->set_data_weight_override(dataWeight);
        } else {
            chunkSpec->set_data_weight_override(
                DivCeil(dataWeight, chunk->MiscExt().row_count()) * chunkSpec->row_count_override());
        }

        if (timestampTransactionId) {
            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            chunkSpec->set_override_timestamp(
                transactionManager->GetTimestampHolderTimestamp(timestampTransactionId));
        }

        return true;
    }

    virtual bool OnChunkView(TChunkView* /*chunkView*/) override
    {
        return false;
    }

    virtual void OnFinish(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (!error.IsOK()) {
            ReplyError(error);
            return;
        }

        if (Finished_) {
            return;
        }

        ++CurrentRangeIndex_;
        if (CurrentRangeIndex_ == FetchContext_.Ranges.size()) {
            ReplySuccess();
        } else {
            TraverseCurrentRange();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TChunkOwnerNodeProxy::TChunkOwnerNodeProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TChunkOwnerBase* trunkNode)
    : TNontemplateCypressNodeProxyBase(
        bootstrap,
        metadata,
        transaction,
        trunkNode)
{ }

ENodeType TChunkOwnerNodeProxy::GetType() const
{
    return ENodeType::Entity;
}

bool TChunkOwnerNodeProxy::DoInvoke(const NRpc::IServiceContextPtr& context)
{
    YT_LOG_ACCESS_IF(
        IsAccessLoggedMethod(context->GetMethod()),
        context,
        GetPath(),
        Transaction_);

    DISPATCH_YPATH_SERVICE_METHOD(Fetch,
        .SetHeavy(true)
        .SetResponseCodec(NCompression::ECodec::Lz4));
    DISPATCH_YPATH_SERVICE_METHOD(BeginUpload);
    DISPATCH_YPATH_SERVICE_METHOD(GetUploadParams);
    DISPATCH_YPATH_SERVICE_METHOD(EndUpload);
    return TNontemplateCypressNodeProxyBase::DoInvoke(context);
}

void TChunkOwnerNodeProxy::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    TNontemplateCypressNodeProxyBase::ListSystemAttributes(descriptors);

    const auto* node = GetThisImpl<TChunkOwnerBase>();
    auto isExternal = node->IsExternal();

    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkListId)
        .SetExternal(isExternal)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkIds)
        .SetExternal(isExternal)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CompressionStatistics)
        .SetExternal(isExternal)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ErasureStatistics)
        .SetExternal(isExternal)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MulticellStatistics)
        .SetExternal(isExternal)
        .SetOpaque(true));
    descriptors->push_back(EInternedAttributeKey::ChunkCount);
    descriptors->push_back(EInternedAttributeKey::UncompressedDataSize);
    descriptors->push_back(EInternedAttributeKey::CompressedDataSize);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::DataWeight)
        .SetPresent(node->HasDataWeight()));
    descriptors->push_back(EInternedAttributeKey::CompressionRatio);
    descriptors->push_back(EInternedAttributeKey::UpdateMode);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ReplicationFactor)
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Vital)
        .SetWritable(true)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Media)
        .SetWritable(true)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PrimaryMedium)
        .SetWritable(true)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CompressionCodec)
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ErasureCodec)
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::SecurityTags)
        .SetWritable(true)
        .SetReplicated(true));
}

bool TChunkOwnerNodeProxy::GetBuiltinAttribute(
    TInternedAttributeKey key,
    IYsonConsumer* consumer)
{
    auto* node = GetThisImpl<TChunkOwnerBase>();
    const auto* chunkList = node->GetChunkList();
    auto statistics = node->ComputeTotalStatistics();
    auto isExternal = node->IsExternal();

    switch (key) {
        case EInternedAttributeKey::ChunkListId:
            if (isExternal) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(chunkList->GetId());
            return true;

        case EInternedAttributeKey::ChunkCount:
            BuildYsonFluently(consumer)
                .Value(statistics.chunk_count());
            return true;

        case EInternedAttributeKey::UncompressedDataSize:
            BuildYsonFluently(consumer)
                .Value(statistics.uncompressed_data_size());
            return true;

        case EInternedAttributeKey::CompressedDataSize:
            BuildYsonFluently(consumer)
                .Value(statistics.compressed_data_size());
            return true;

        case EInternedAttributeKey::DataWeight:
            if (!node->HasDataWeight()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(statistics.data_weight());
            return true;

        case EInternedAttributeKey::CompressionRatio: {
            double ratio = statistics.uncompressed_data_size() > 0
                ? static_cast<double>(statistics.compressed_data_size()) / statistics.uncompressed_data_size()
                : 0;
            BuildYsonFluently(consumer)
                .Value(ratio);
            return true;
        }

        case EInternedAttributeKey::UpdateMode:
            BuildYsonFluently(consumer)
                .Value(FormatEnum(node->GetUpdateMode()));
            return true;

        case EInternedAttributeKey::Media: {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            const auto& replication = node->Replication();
            BuildYsonFluently(consumer)
                .Value(TSerializableChunkReplication(replication, chunkManager));
            return true;
        }

        case EInternedAttributeKey::ReplicationFactor: {
            const auto& replication = node->Replication();
            auto primaryMediumIndex = node->GetPrimaryMediumIndex();
            BuildYsonFluently(consumer)
                .Value(replication.Get(primaryMediumIndex).GetReplicationFactor());
            return true;
        }

        case EInternedAttributeKey::Vital:
            BuildYsonFluently(consumer)
                .Value(node->Replication().GetVital());
            return true;

        case EInternedAttributeKey::PrimaryMedium: {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            auto primaryMediumIndex = node->GetPrimaryMediumIndex();
            auto* medium = chunkManager->GetMediumByIndex(primaryMediumIndex);

            BuildYsonFluently(consumer)
                .Value(medium->GetName());
            return true;
        }

        case EInternedAttributeKey::CompressionCodec:
            BuildYsonFluently(consumer)
                .Value(node->GetCompressionCodec());
            return true;

        case EInternedAttributeKey::ErasureCodec:
            BuildYsonFluently(consumer)
                .Value(node->GetErasureCodec());
            return true;

        case EInternedAttributeKey::SecurityTags:
            BuildYsonFluently(consumer)
                .Value(node->GetSecurityTags().Items);
            return true;

        default:
            break;
    }

    return TNontemplateCypressNodeProxyBase::GetBuiltinAttribute(key, consumer);
}

TFuture<TYsonString> TChunkOwnerNodeProxy::GetBuiltinAttributeAsync(TInternedAttributeKey key)
{
    auto* node = GetThisImpl<TChunkOwnerBase>();
    auto* chunkList = node->GetChunkList();
    auto isExternal = node->IsExternal();

    switch (key) {
        case EInternedAttributeKey::ChunkIds: {
            if (isExternal) {
                break;
            }
            auto visitor = New<TChunkIdsAttributeVisitor>(
                Bootstrap_,
                chunkList);
            return visitor->Run();
        }

        case EInternedAttributeKey::CompressionStatistics:
            if (isExternal) {
                break;
            }
            return ComputeChunkStatistics(
                Bootstrap_,
                chunkList,
                [] (const TChunk* chunk) {
                    return CheckedEnumCast<NCompression::ECodec>(chunk->MiscExt().compression_codec());
                });

        case EInternedAttributeKey::ErasureStatistics:
            if (isExternal) {
                break;
            }
            return ComputeChunkStatistics(
                Bootstrap_,
                chunkList,
                [] (const TChunk* chunk) { return chunk->GetErasureCodec(); });

        case EInternedAttributeKey::MulticellStatistics:
            if (isExternal) {
                break;
            }
            return ComputeChunkStatistics(
                Bootstrap_,
                chunkList,
                [] (const TChunk* chunk) { return chunk->GetNativeCellTag(); });

        default:
            break;
    }

    return TNontemplateCypressNodeProxyBase::GetBuiltinAttributeAsync(key);
}

bool TChunkOwnerNodeProxy::SetBuiltinAttribute(
    TInternedAttributeKey key,
    const TYsonString& value)
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();

    switch (key) {
        case EInternedAttributeKey::ReplicationFactor: {
            ValidateStorageParametersUpdate();
            int replicationFactor = ConvertTo<int>(value);
            SetReplicationFactor(replicationFactor);
            return true;
        }

        case EInternedAttributeKey::Vital: {
            ValidateStorageParametersUpdate();
            bool vital = ConvertTo<bool>(value);
            SetVital(vital);
            return true;
        }

        case EInternedAttributeKey::PrimaryMedium: {
            ValidateStorageParametersUpdate();
            auto mediumName = ConvertTo<TString>(value);
            auto* medium = chunkManager->GetMediumByNameOrThrow(mediumName);
            SetPrimaryMedium(medium);
            return true;
        }

        case EInternedAttributeKey::Media: {
            ValidateStorageParametersUpdate();
            auto serializableReplication = ConvertTo<TSerializableChunkReplication>(value);
            auto replication = GetThisImpl<TChunkOwnerBase>()->Replication(); // Copying for modification.
            // Preserves vitality.
            serializableReplication.ToChunkReplication(&replication, chunkManager);
            SetReplication(replication);
            return true;
        }

        case EInternedAttributeKey::CompressionCodec: {
            ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

            const auto& uninternedKey = key.Unintern();
            auto* node = LockThisImpl<TChunkOwnerBase>(TLockRequest::MakeSharedAttribute(uninternedKey));
            node->SetCompressionCodec(ConvertTo<NCompression::ECodec>(value));

            return true;
        }

        case EInternedAttributeKey::ErasureCodec: {
            ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

            const auto& uninternedKey = key.Unintern();
            auto* node = LockThisImpl<TChunkOwnerBase>(TLockRequest::MakeSharedAttribute(uninternedKey));
            node->SetErasureCodec(ConvertTo<NErasure::ECodec>(value));

            return true;
        }

        case EInternedAttributeKey::Account: {
            if (!TNontemplateCypressNodeProxyBase::SetBuiltinAttribute(key, value)) {
                return false;
            }

            const auto& uninternedKey = key.Unintern();
            auto* node = LockThisImpl<TChunkOwnerBase>(TLockRequest::MakeSharedAttribute(uninternedKey));
            if (!node->IsExternal()) {
                chunkManager->ScheduleChunkRequisitionUpdate(node->GetChunkList());
            }
            return true;
        }

        case EInternedAttributeKey::SecurityTags: {
            auto* node = LockThisImpl<TChunkOwnerBase>();
            if (node->GetUpdateMode() == EUpdateMode::Append) {
                THROW_ERROR_EXCEPTION("Cannot change security tags of a node in %Qlv update mode",
                    node->GetUpdateMode());
            }

            TSecurityTags securityTags{
                ConvertTo<TSecurityTagsItems>(value)
            };
            securityTags.Normalize();
            securityTags.Validate();

            // TODO(babenko): audit
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Node security tags updated; node is switched to \"overwrite\" mode (NodeId: %v, OldSecurityTags: %v, NewSecurityTags: %v",
                node->GetVersionedId(),
                node->GetSecurityTags().Items,
                securityTags.Items);

            const auto& securityManager = Bootstrap_->GetSecurityManager();
            const auto& securityTagsRegistry = securityManager->GetSecurityTagsRegistry();
            node->SnapshotSecurityTags() = securityTagsRegistry->Intern(std::move(securityTags));
            node->SetUpdateMode(EUpdateMode::Overwrite);
            return true;
        }

        default:
            break;
    }

    return TNontemplateCypressNodeProxyBase::SetBuiltinAttribute(key, value);
}

void TChunkOwnerNodeProxy::SetReplicationFactor(int replicationFactor)
{
    auto* node = GetThisImpl<TChunkOwnerBase>();
    YT_VERIFY(node->IsTrunk());

    auto mediumIndex = node->GetPrimaryMediumIndex();
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto* medium = chunkManager->GetMediumByIndex(mediumIndex);

    auto replication = node->Replication();
    if (replication.Get(mediumIndex).GetReplicationFactor() == replicationFactor) {
        return;
    }

    ValidateReplicationFactor(replicationFactor);
    if (replicationFactor != 0) {
        ValidatePermission(medium, EPermission::Use);
    }

    auto policy = replication.Get(mediumIndex);
    policy.SetReplicationFactor(replicationFactor);
    replication.Set(mediumIndex, policy);
    ValidateChunkReplication(chunkManager, replication, mediumIndex);

    node->Replication() = replication;

    if (!node->IsExternal()) {
        chunkManager->ScheduleChunkRequisitionUpdate(node->GetChunkList());
    }
}

void TChunkOwnerNodeProxy::SetVital(bool vital)
{
    auto* node = GetThisImpl<TChunkOwnerBase>();
    YT_VERIFY(node->IsTrunk());

    auto& replication = node->Replication();
    if (replication.GetVital() == vital) {
        return;
    }

    replication.SetVital(vital);

    if (!node->IsExternal()) {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->ScheduleChunkRequisitionUpdate(node->GetChunkList());
    }
}

void TChunkOwnerNodeProxy::SetReplication(const TChunkReplication& replication)
{
    auto* node = GetThisImpl<TChunkOwnerBase>();
    const auto& chunkManager = Bootstrap_->GetChunkManager();

    YT_VERIFY(node->IsTrunk());

    auto primaryMediumIndex = node->GetPrimaryMediumIndex();
    ValidateMediaChange(node->Replication(), primaryMediumIndex, replication);

    node->Replication() = replication;

    if (!node->IsExternal()) {
        chunkManager->ScheduleChunkRequisitionUpdate(node->GetChunkList());
    }

    const auto* primaryMedium = chunkManager->GetMediumByIndex(primaryMediumIndex);

    YT_LOG_DEBUG_UNLESS(
        IsRecovery(),
        "Chunk owner replication changed (NodeId: %v, PrimaryMedium: %v, Replication: %v)",
        node->GetId(),
        primaryMedium->GetName(),
        node->Replication());
 }

void TChunkOwnerNodeProxy::SetPrimaryMedium(TMedium* medium)
{
    auto* node = GetThisImpl<TChunkOwnerBase>();
    YT_VERIFY(node->IsTrunk());

    TChunkReplication newReplication;
    if (!ValidatePrimaryMediumChange(
        medium,
        node->Replication(),
        node->GetPrimaryMediumIndex(),
        &newReplication))
    {
        return;
    }

    node->Replication() = newReplication;
    node->SetPrimaryMediumIndex(medium->GetIndex());

    if (!node->IsExternal()) {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->ScheduleChunkRequisitionUpdate(node->GetChunkList());
    }

    YT_LOG_DEBUG_UNLESS(
        IsRecovery(),
        "Chunk owner primary medium changed (NodeId: %v, PrimaryMedium: %v)",
        node->GetId(),
        medium->GetName());
}

void TChunkOwnerNodeProxy::ValidateFetch(TFetchContext* /*context*/)
{ }

void TChunkOwnerNodeProxy::ValidateInUpdate()
{
    auto* node = GetThisImpl<TChunkOwnerBase>();
    if (node->GetUpdateMode() == EUpdateMode::None) {
        THROW_ERROR_EXCEPTION("Node is not in an update mode");
    }
}

void TChunkOwnerNodeProxy::ValidateBeginUpload()
{ }

void TChunkOwnerNodeProxy::ValidateStorageParametersUpdate()
{
    ValidateNoTransaction();
}

void TChunkOwnerNodeProxy::GetBasicAttributes(TGetBasicAttributesContext* context)
{
    TObjectProxyBase::GetBasicAttributes(context);

    const auto* node = GetThisImpl<TChunkOwnerBase>();
    if (node->IsExternal()) {
        context->ExternalCellTag = node->GetExternalCellTag();
    }

    if (context->PopulateSecurityTags) {
        context->SecurityTags = node->GetSecurityTags();
    }

    auto* transaction = GetTransaction();
    if (node->IsExternal()) {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        context->ExternalTransactionId = transactionManager->GetNearestExternalizedTransactionAncestor(
            transaction,
            node->GetExternalCellTag());
    }
}

DEFINE_YPATH_SERVICE_METHOD(TChunkOwnerNodeProxy, Fetch)
{
    DeclareNonMutating();

    context->SetRequestInfo();

    // NB: No need for a permission check;
    // the client must have invoked GetBasicAttributes.

    ValidateNotExternal();

    TFetchContext fetchContext;
    fetchContext.FetchParityReplicas = request->fetch_parity_replicas();
    fetchContext.AddressType = request->has_address_type()
        ? CheckedEnumCast<EAddressType>(request->address_type())
        : EAddressType::InternalRpc;
    fetchContext.Ranges = FromProto<std::vector<TReadRange>>(request->ranges());
    ValidateFetch(&fetchContext);

    const auto* node = GetThisImpl<TChunkOwnerBase>();
    auto* chunkList = node->GetChunkList();

    auto visitor = New<TFetchChunkVisitor>(
        Bootstrap_,
        chunkList,
        context,
        std::move(fetchContext));
    visitor->Run();
}

DEFINE_YPATH_SERVICE_METHOD(TChunkOwnerNodeProxy, BeginUpload)
{
    DeclareMutating();

    TChunkOwnerBase::TBeginUploadContext uploadContext;
    uploadContext.Mode = CheckedEnumCast<EUpdateMode>(request->update_mode());
    if (uploadContext.Mode != EUpdateMode::Append && uploadContext.Mode != EUpdateMode::Overwrite) {
        THROW_ERROR_EXCEPTION("Invalid update mode %Qlv for a chunk owner node",
            uploadContext.Mode);
    }

    auto lockMode = CheckedEnumCast<ELockMode>(request->lock_mode());

    auto uploadTransactionTitle = request->has_upload_transaction_title()
        ? std::make_optional(request->upload_transaction_title())
        : std::nullopt;

    auto uploadTransactionTimeout = request->has_upload_transaction_timeout()
        ? std::make_optional(FromProto<TDuration>(request->upload_transaction_timeout()))
        : std::nullopt;

    auto uploadTransactionIdHint = FromProto<TTransactionId>(request->upload_transaction_id());

    auto replicatedToCellTags = FromProto<TCellTagList>(request->upload_transaction_secondary_cell_tags());

    auto* node = GetThisImpl<TChunkOwnerBase>();
    auto nativeCellTag = node->GetNativeCellTag();
    auto externalCellTag = node->GetExternalCellTag();

    // Make sure |replicatedToCellTags| contains the external cell tag,
    // does not contain the native cell tag, is sorted, and contains no duplicates.
    InsertCellTag(&replicatedToCellTags, externalCellTag);
    CanonizeCellTags(&replicatedToCellTags);
    RemoveCellTag(&replicatedToCellTags, nativeCellTag);

    // Construct |replicateStartToCellTags| containing the tags of cells
    // the upload transaction will be ultimately replicated to. This list never contains
    // the external cell tag.
    auto replicateStartToCellTags = replicatedToCellTags;
    RemoveCellTag(&replicateStartToCellTags, externalCellTag);

    context->SetRequestInfo(
        "UpdateMode: %v, LockMode: %v, "
        "Title: %v, Timeout: %v, ReplicatedToCellTags: %v",
        uploadContext.Mode,
        lockMode,
        uploadTransactionTitle,
        uploadTransactionTimeout,
        replicatedToCellTags);

    // NB: No need for a permission check;
    // the client must have invoked GetBasicAttributes.

    ValidateBeginUpload();

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    const auto& objectManager = Bootstrap_->GetObjectManager();
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    const auto& transactionManager = Bootstrap_->GetTransactionManager();

    auto* uploadTransaction = transactionManager->StartTransaction(
        /* parent */ Transaction_,
        /* prerequisiteTransactions */ {},
        replicatedToCellTags,
        /* replicateStart */ false,
        uploadTransactionTimeout,
        /* deadline */ std::nullopt,
        uploadTransactionTitle,
        EmptyAttributes(),
        uploadTransactionIdHint);

    auto* lockedNode = cypressManager
        ->LockNode(TrunkNode_, uploadTransaction, lockMode, false, true)
        ->As<TChunkOwnerBase>();

    switch (uploadContext.Mode) {
        case EUpdateMode::Append: {
            if (node->IsExternal() || node->GetType() == EObjectType::Journal) {
                YT_LOG_DEBUG_UNLESS(
                    IsRecovery(),
                    "Node is switched to \"append\" mode (NodeId: %v)",
                    lockedNode->GetId());
            } else {
                auto* snapshotChunkList = lockedNode->GetChunkList();

                if (snapshotChunkList->GetKind() == EChunkListKind::Static) {
                    auto* newChunkList = chunkManager->CreateChunkList(EChunkListKind::Static);
                    newChunkList->AddOwningNode(lockedNode);

                    snapshotChunkList->RemoveOwningNode(lockedNode);
                    lockedNode->SetChunkList(newChunkList);
                    objectManager->RefObject(newChunkList);

                    chunkManager->AttachToChunkList(newChunkList, snapshotChunkList);

                    auto* deltaChunkList = chunkManager->CreateChunkList(EChunkListKind::Static);
                    chunkManager->AttachToChunkList(newChunkList, deltaChunkList);

                    objectManager->UnrefObject(snapshotChunkList);

                    YT_LOG_DEBUG_UNLESS(
                        IsRecovery(),
                        "Node is switched to \"append\" mode (NodeId: %v, NewChunkListId: %v, SnapshotChunkListId: %v, DeltaChunkListId: %v)",
                        node->GetId(),
                        newChunkList->GetId(),
                        snapshotChunkList->GetId(),
                        deltaChunkList->GetId());

                } else if (snapshotChunkList->GetKind() == EChunkListKind::SortedDynamicRoot) {
                    auto* newChunkList = chunkManager->CreateChunkList(EChunkListKind::SortedDynamicRoot);
                    newChunkList->AddOwningNode(lockedNode);
                    lockedNode->SetChunkList(newChunkList);
                    objectManager->RefObject(newChunkList);

                    for (int index = 0; index < snapshotChunkList->Children().size(); ++index) {
                        auto* appendChunkList = chunkManager->CreateChunkList(EChunkListKind::SortedDynamicSubtablet);
                        chunkManager->AttachToChunkList(newChunkList, appendChunkList);
                    }

                    snapshotChunkList->RemoveOwningNode(lockedNode);
                    objectManager->UnrefObject(snapshotChunkList);

                } else {
                    YT_ABORT();
                }

            }
            break;
        }

        case EUpdateMode::Overwrite: {
            if (node->IsExternal() || node->GetType() == EObjectType::Journal) {
                YT_LOG_DEBUG_UNLESS(
                    IsRecovery(),
                    "Node is switched to \"overwrite\" mode (NodeId: %v)",
                    node->GetId());
            } else {
                auto* oldChunkList = lockedNode->GetChunkList();

                YT_VERIFY(oldChunkList->GetKind() == EChunkListKind::Static ||
                    oldChunkList->GetKind() == EChunkListKind::SortedDynamicRoot);

                oldChunkList->RemoveOwningNode(lockedNode);

                auto* newChunkList = chunkManager->CreateChunkList(oldChunkList->GetKind());
                newChunkList->AddOwningNode(lockedNode);
                lockedNode->SetChunkList(newChunkList);
                objectManager->RefObject(newChunkList);

                if (oldChunkList->GetKind() == EChunkListKind::SortedDynamicRoot) {
                    for (int index = 0; index < oldChunkList->Children().size(); ++index) {
                        auto* appendChunkList = chunkManager->CreateChunkList(EChunkListKind::SortedDynamicTablet);
                        chunkManager->AttachToChunkList(newChunkList, appendChunkList);
                    }
                }

                objectManager->UnrefObject(oldChunkList);

                YT_LOG_DEBUG_UNLESS(
                    IsRecovery(),
                    "Node is switched to \"overwrite\" mode (NodeId: %v, NewChunkListId: %v)",
                    node->GetId(),
                    newChunkList->GetId());
            }
            break;
        }

        default:
            YT_ABORT();
    }

    lockedNode->BeginUpload(uploadContext);

    auto uploadTransactionId = uploadTransaction->GetId();
    ToProto(response->mutable_upload_transaction_id(), uploadTransactionId);

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    response->set_cell_tag(externalCellTag == NotReplicatedCellTag
        ? multicellManager->GetCellTag()
        : externalCellTag);

    auto externalizedTransactionId = node->IsExternal()
        ? transactionManager->ExternalizeTransaction(Transaction_, externalCellTag)
        : GetObjectId(Transaction_);

    if (node->IsExternal()) {
        auto replicationRequest = TChunkOwnerYPathProxy::BeginUpload(FromObjectId(GetId()));
        SetTransactionId(replicationRequest, externalizedTransactionId);
        replicationRequest->set_update_mode(static_cast<int>(uploadContext.Mode));
        replicationRequest->set_lock_mode(static_cast<int>(lockMode));
        ToProto(replicationRequest->mutable_upload_transaction_id(), uploadTransactionId);
        if (uploadTransactionTitle) {
            replicationRequest->set_upload_transaction_title(*uploadTransactionTitle);
        }
        // NB: upload_transaction_timeout must remain null
        // NB: upload_transaction_secondary_cell_tags must remain empty
        multicellManager->PostToMaster(replicationRequest, externalCellTag);
    }

    if (!replicateStartToCellTags.empty()) {
        NTransactionServer::NProto::TReqStartTransaction startRequest;
        startRequest.set_dont_replicate(true);
        ToProto(startRequest.mutable_hint_id(), uploadTransactionId);
        if (externalizedTransactionId) {
            ToProto(startRequest.mutable_parent_id(), externalizedTransactionId);
        }
        if (uploadTransactionTitle) {
            startRequest.set_title(*uploadTransactionTitle);
        }
        multicellManager->PostToMasters(startRequest, replicateStartToCellTags);
    }

    context->SetResponseInfo("UploadTransactionId: %v", uploadTransactionId);
    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TChunkOwnerNodeProxy, GetUploadParams)
{
    DeclareNonMutating();

    bool fetchLastKey = request->fetch_last_key();

    context->SetRequestInfo("FetchLastKey: %v", fetchLastKey);

    ValidateNotExternal();
    ValidateInUpdate();

    auto* node = GetThisImpl<TChunkOwnerBase>();
    if (node->GetChunkList()->GetKind() == EChunkListKind::Static) {
        auto* snapshotChunkList = node->GetSnapshotChunkList();
        auto* deltaChunkList = node->GetDeltaChunkList();

        const auto& uploadChunkListId = deltaChunkList->GetId();
        ToProto(response->mutable_chunk_list_id(), uploadChunkListId);

        if (fetchLastKey) {
            TOwningKey lastKey;
            if (!IsEmpty(snapshotChunkList)) {
                lastKey = GetUpperBoundKeyOrThrow(snapshotChunkList);
            }
            ToProto(response->mutable_last_key(), lastKey);
        }

        std::optional<TMD5Hasher> md5Hasher;
        node->GetUploadParams(&md5Hasher);
        ToProto(response->mutable_md5_hasher(), md5Hasher);

        context->SetResponseInfo("UploadChunkListId: %v, HasLastKey: %v",
            uploadChunkListId,
            response->has_last_key());

    } else if (node->GetChunkList()->GetKind() == EChunkListKind::SortedDynamicRoot) {
        auto* chunkList = node->GetChunkList();
        auto* trunkChunkList = node->GetTrunkNode()->As<TChunkOwnerBase>()->GetChunkList();

        for (auto* tabletList : trunkChunkList->Children()) {
            ToProto(response->add_pivot_keys(), tabletList->AsChunkList()->GetPivotKey());
        }

        for (auto* tabletList : chunkList->Children()) {
            YT_VERIFY(
                tabletList->AsChunkList()->GetKind() == EChunkListKind::SortedDynamicSubtablet ||
                tabletList->AsChunkList()->GetKind() == EChunkListKind::SortedDynamicTablet);
            ToProto(response->add_tablet_chunk_list_ids(), tabletList->GetId());
        }
    } else {
        THROW_ERROR_EXCEPTION("Unsupported chunk list kind");
    }

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TChunkOwnerNodeProxy, EndUpload)
{
    DeclareMutating();

    TChunkOwnerBase::TEndUploadContext uploadContext;

    uploadContext.SchemaMode = CheckedEnumCast<ETableSchemaMode>(request->schema_mode());

    if (request->has_statistics()) {
        uploadContext.Statistics = &request->statistics();
    }

    if (request->has_optimize_for()) {
        uploadContext.OptimizeFor = CheckedEnumCast<EOptimizeFor>(request->optimize_for());
    }

    if (request->has_md5_hasher()) {
        uploadContext.MD5Hasher = FromProto<std::optional<TMD5Hasher>>(request->md5_hasher());
    }

    if (request->has_table_schema()) {
        const auto& registry = Bootstrap_->GetCypressManager()->GetSharedTableSchemaRegistry();
        uploadContext.Schema = registry->GetSchema(FromProto<TTableSchema>(request->table_schema()));
    }

    if (request->has_security_tags()) {
        TSecurityTags securityTags{
            FromProto<TSecurityTagsItems>(request->security_tags().items())
        };
        securityTags.Normalize();
        securityTags.Validate();

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        const auto& securityTagsRegistry = securityManager->GetSecurityTagsRegistry();
        uploadContext.SecurityTags = securityTagsRegistry->Intern(std::move(securityTags));
    }

    if (request->has_compression_codec()) {
        uploadContext.CompressionCodec = CheckedEnumCast<NCompression::ECodec>(request->compression_codec());
    }

    if (request->has_erasure_codec()) {
        uploadContext.ErasureCodec = CheckedEnumCast<NErasure::ECodec>(request->erasure_codec());
    }

    context->SetRequestInfo("SchemaMode: %v, Statistics: %v, CompressionCodec: %v, ErasureCodec: %v, OptimizeFor: %v, "
        "MD5Hasher: %v",
        uploadContext.SchemaMode,
        uploadContext.Statistics,
        uploadContext.CompressionCodec,
        uploadContext.ErasureCodec,
        uploadContext.OptimizeFor,
        uploadContext.MD5Hasher.has_value());

    ValidateTransaction();
    ValidateInUpdate();

    auto* node = GetThisImpl<TChunkOwnerBase>();
    YT_VERIFY(node->GetTransaction() == Transaction_);

    if (node->IsExternal()) {
        ExternalizeToMaster(context, node->GetExternalCellTag());
    }

    node->EndUpload(uploadContext);

    SetModified();

    if (node->IsNative()) {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->CommitTransaction(Transaction_, NullTimestamp);
    }

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
