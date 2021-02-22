#include "chunk.h"
#include "dynamic_store.h"
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

#include <yt/server/master/cypress_server/helpers.h>

#include <yt/server/master/node_tracker_server/node_directory_builder.h>

#include <yt/server/lib/misc/interned_attributes.h>

#include <yt/server/master/object_server/object.h>

#include <yt/server/master/table_server/shared_table_schema.h>

#include <yt/server/master/tablet_server/tablet_manager.h>

#include <yt/server/master/security_server/security_manager.h>
#include <yt/server/master/security_server/security_tags.h>
#include <yt/server/master/security_server/access_log.h>

#include <yt/server/master/transaction_server/proto/transaction_manager.pb.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/file_client/file_chunk_writer.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/library/erasure/impl/codec.h>

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
using namespace NTabletServer;
using namespace NCellMaster;

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

void BuildChunkSpec(
    TChunk* chunk,
    std::optional<i64> rowIndex,
    std::optional<int> tabletIndex,
    const TReadLimit& lowerLimit,
    const TReadLimit& upperLimit,
    TTransactionId timestampTransactionId,
    bool fetchParityReplicas,
    bool fetchAllMetaExtensions,
    const THashSet<int>& extensionTags,
    NNodeTrackerServer::TNodeDirectoryBuilder* nodeDirectoryBuilder,
    TBootstrap* bootstrap,
    NChunkClient::NProto::TChunkSpec* chunkSpec)
{
    const auto& configManager = bootstrap->GetConfigManager();
    const auto& dynamicConfig = configManager->GetConfig()->ChunkManager;

    if (rowIndex) {
        chunkSpec->set_table_row_index(*rowIndex);
    }

    if (tabletIndex) {
        chunkSpec->set_tablet_index(*tabletIndex);
    }

    auto erasureCodecId = chunk->GetErasureCodec();
    int firstInfeasibleReplicaIndex = (erasureCodecId == NErasure::ECodec::None || fetchParityReplicas)
        ? std::numeric_limits<int>::max() // all replicas are feasible
        : NErasure::GetCodec(erasureCodecId)->GetDataPartCount();

    SmallVector<TNodePtrWithIndexes, TypicalReplicaCount> replicas;

    auto addReplica = [&] (TNodePtrWithIndexes replica)  {
        if (replica.GetReplicaIndex() >= firstInfeasibleReplicaIndex) {
            return false;
        }
        replicas.push_back(replica);
        nodeDirectoryBuilder->Add(replica);
        return true;
    };

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

    ToProto(chunkSpec->mutable_replicas(), replicas);
    ToProto(chunkSpec->mutable_chunk_id(), chunk->GetId());
    chunkSpec->set_erasure_codec(ToProto<int>(erasureCodecId));

    chunkSpec->mutable_chunk_meta()->set_type(chunk->ChunkMeta().type());
    chunkSpec->mutable_chunk_meta()->set_format(chunk->ChunkMeta().format());
    chunkSpec->mutable_chunk_meta()->set_features(chunk->ChunkMeta().features());

    if (fetchAllMetaExtensions) {
        *chunkSpec->mutable_chunk_meta()->mutable_extensions() = chunk->ChunkMeta().extensions();
    } else {
        FilterProtoExtensions(
            chunkSpec->mutable_chunk_meta()->mutable_extensions(),
            chunk->ChunkMeta().extensions(),
            extensionTags);
    }

    // Try to keep responses small -- avoid producing redundant limits.
    if (!lowerLimit.IsTrivial()) {
        ToProto(chunkSpec->mutable_lower_limit(), lowerLimit);
    }
    if (!upperLimit.IsTrivial()) {
        ToProto(chunkSpec->mutable_upper_limit(), upperLimit);
    }

    i64 lowerRowLimit = lowerLimit.GetRowIndex().value_or(0);
    i64 upperRowLimit = upperLimit.GetRowIndex().value_or(chunk->MiscExt().row_count());

    // If one of row indexes is present, then fields row_count_override and
    // uncompressed_data_size_override estimate the chunk range
    // instead of the whole chunk.
    // To ensure the correct usage of this rule, row indexes should be
    // either both set or not.
    if (lowerLimit.GetRowIndex() && !upperLimit.GetRowIndex()) {
        chunkSpec->mutable_upper_limit()->set_row_index(upperRowLimit);
    }

    if (upperLimit.GetRowIndex() && !lowerLimit.GetRowIndex()) {
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
        const auto& transactionManager = bootstrap->GetTransactionManager();
        chunkSpec->set_override_timestamp(
            transactionManager->GetTimestampHolderTimestamp(timestampTransactionId));
    }
}

void BuildDynamicStoreSpec(
    const TDynamicStore* dynamicStore,
    std::optional<int> tabletIndex,
    const TReadLimit& lowerLimit,
    const TReadLimit& upperLimit,
    NNodeTrackerServer::TNodeDirectoryBuilder* nodeDirectoryBuilder,
    TBootstrap* bootstrap,
    NChunkClient::NProto::TChunkSpec* chunkSpec)
{
    const auto& tabletManager = bootstrap->GetTabletManager();
    auto* tablet = dynamicStore->GetTablet();

    ToProto(chunkSpec->mutable_chunk_id(), dynamicStore->GetId());
    ToProto(chunkSpec->mutable_tablet_id(), GetObjectId(tablet));
    if (tabletIndex) {
        chunkSpec->set_tablet_index(*tabletIndex);
    }

    // Something non-zero.
    chunkSpec->set_row_count_override(1);
    chunkSpec->set_data_weight_override(1);

    // NB: table_row_index is not filled here since:
    // 1) dynamic store reader receives it from the node;
    // 2) we cannot determine it at master when there are multiple consecutive dynamic stores.

    if (auto* node = tabletManager->FindTabletLeaderNode(tablet)) {
        auto replica = TNodePtrWithIndexes(node, GenericChunkReplicaIndex, DefaultStoreMediumIndex);
        nodeDirectoryBuilder->Add(replica);
        chunkSpec->add_replicas(ToProto<ui64>(replica));
    }

    if (!lowerLimit.IsTrivial()) {
        ToProto(chunkSpec->mutable_lower_limit(), lowerLimit);
    }
    if (!upperLimit.IsTrivial()) {
        ToProto(chunkSpec->mutable_upper_limit(), upperLimit);
    }
    chunkSpec->set_row_index_is_absolute(true);
}

////////////////////////////////////////////////////////////////////////////////

class TChunkOwnerNodeProxy::TFetchChunkVisitor
    : public IChunkVisitor
{
public:
    TFetchChunkVisitor(
        NCellMaster::TBootstrap* bootstrap,
        TChunkList* chunkList,
        TCtxFetchPtr rpcContext,
        TFetchContext&& fetchContext,
        TComparator comparator)
        : Bootstrap_(bootstrap)
        , ChunkList_(chunkList)
        , RpcContext_(std::move(rpcContext))
        , FetchContext_(std::move(fetchContext))
        , Comparator_(std::move(comparator))
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
    const TComparator Comparator_;

    int CurrentRangeIndex_ = 0;

    THashSet<int> ExtensionTags_;
    NNodeTrackerServer::TNodeDirectoryBuilder NodeDirectoryBuilder_;
    bool Finished_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void TraverseCurrentRange()
    {
        auto context = CreateAsyncChunkTraverserContext(
            Bootstrap_,
            NCellMaster::EAutomatonThreadQueue::ChunkFetchingTraverser);
        TraverseChunkTree(
            std::move(context),
            this,
            ChunkList_,
            FetchContext_.Ranges[CurrentRangeIndex_].LowerLimit(),
            FetchContext_.Ranges[CurrentRangeIndex_].UpperLimit(),
            Comparator_);
    }

    void ReplySuccess()
    {
        YT_VERIFY(!Finished_);
        Finished_ = true;

        RpcContext_->SetResponseInfo("ChunkCount: %v", RpcContext_->Response().chunks_size());
        RpcContext_->Reply();
    }

    void ReplyError(const TError& error)
    {
        if (Finished_) {
            return;
        }
        Finished_ = true;

        RpcContext_->Reply(error);
    }

    virtual bool OnChunk(
        TChunk* chunk,
        std::optional<i64> rowIndex,
        std::optional<int> tabletIndex,
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

        BuildChunkSpec(
            chunk,
            rowIndex,
            tabletIndex,
            lowerLimit,
            upperLimit,
            timestampTransactionId,
            FetchContext_.FetchParityReplicas,
            RpcContext_->Request().fetch_all_meta_extensions(),
            ExtensionTags_,
            &NodeDirectoryBuilder_,
            Bootstrap_,
            chunkSpec);
        chunkSpec->set_range_index(CurrentRangeIndex_);

        ValidateChunkFeatures(
            chunk->GetId(),
            chunkSpec->chunk_meta().features(),
            FetchContext_.SupportedChunkFeatures);

        return true;
    }

    virtual bool OnChunkView(TChunkView* /*chunkView*/) override
    {
        if (FetchContext_.ThrowOnChunkViews) {
            THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::InvalidInputChunk,
                "Chunk view cannot be copied to remote cluster");
        }

        return false;
    }

    virtual bool OnDynamicStore(
        TDynamicStore* dynamicStore,
        std::optional<int> tabletIndex,
        const TReadLimit& lowerLimit,
        const TReadLimit& upperLimit) override
    {
        if (FetchContext_.OmitDynamicStores) {
            return true;
        }

        if (dynamicStore->IsFlushed()) {
            if (auto* chunk = dynamicStore->GetFlushedChunk()) {
                auto relativeLowerLimit = lowerLimit;
                auto relativeUpperLimit = upperLimit;
                i64 tabletRowIndex = dynamicStore->GetTableRowIndex();
                if (relativeLowerLimit.GetRowIndex()) {
                    relativeLowerLimit.SetRowIndex(std::max<i64>(
                        *relativeLowerLimit.GetRowIndex() - tabletRowIndex,
                        0));
                }
                if (relativeUpperLimit.GetRowIndex()) {
                    relativeUpperLimit.SetRowIndex(std::max<i64>(
                        *relativeUpperLimit.GetRowIndex() - tabletRowIndex,
                        0));
                }
                return OnChunk(
                    chunk,
                    dynamicStore->GetTableRowIndex(),
                    tabletIndex,
                    relativeLowerLimit,
                    relativeUpperLimit,
                    /*timestampTransactionId*/ {});
            }
        } else {
            auto* chunkSpec = RpcContext_->Response().add_chunks();
            BuildDynamicStoreSpec(
                dynamicStore,
                tabletIndex,
                lowerLimit,
                upperLimit,
                &NodeDirectoryBuilder_,
                Bootstrap_,
                chunkSpec);
            chunkSpec->set_range_index(CurrentRangeIndex_);
        }
        return true;
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
    const auto& config = Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager;

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
            if (TrunkNode_->GetType() == EObjectType::Journal) {
                THROW_ERROR_EXCEPTION("Journal compression codec cannot be set");
            }

            ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

            const auto& uninternedKey = key.Unintern();
            auto* node = LockThisImpl<TChunkOwnerBase>(TLockRequest::MakeSharedAttribute(uninternedKey));

            ValidateCompressionCodec(
                value,
                config->DeprecatedCodecIds,
                config->DeprecatedCodecNameToAlias);

            node->SetCompressionCodec(ConvertTo<NCompression::ECodec>(value));

            return true;
        }

        case EInternedAttributeKey::ErasureCodec: {
            if (TrunkNode_->GetType() == EObjectType::Journal) {
                THROW_ERROR_EXCEPTION("Journal erasure codec cannot be changed after creation");
            }

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
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Node security tags updated; node is switched to \"overwrite\" mode (NodeId: %v, OldSecurityTags: %v, NewSecurityTags: %v",
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
    ValidatePermission(medium, EPermission::Use);

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

    YT_LOG_DEBUG_IF(
        IsMutationLoggingEnabled(),
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

    YT_LOG_DEBUG_IF(
        IsMutationLoggingEnabled(),
        "Chunk owner primary medium changed (NodeId: %v, PrimaryMedium: %v)",
        node->GetId(),
        medium->GetName());
}

void TChunkOwnerNodeProxy::ValidateReadLimit(const NChunkClient::NProto::TReadLimit& /* readLimit */) const
{ }

TComparator TChunkOwnerNodeProxy::GetComparator() const
{
    return TComparator();
}

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
    TBase::GetBasicAttributes(context);

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

    context->SetRequestInfo("OmitDynamicStores: %v, ThrowOnChunkViews: %v",
        request->omit_dynamic_stores(),
        request->throw_on_chunk_views());

    // NB: No need for a permission check;
    // the client must have invoked GetBasicAttributes.

    ValidateNotExternal();

    TFetchContext fetchContext;
    fetchContext.FetchParityReplicas = request->fetch_parity_replicas();
    fetchContext.OmitDynamicStores = request->omit_dynamic_stores();
    fetchContext.ThrowOnChunkViews = request->throw_on_chunk_views();
    fetchContext.SupportedChunkFeatures = request->supported_chunk_features();
    fetchContext.AddressType = request->has_address_type()
        ? CheckedEnumCast<EAddressType>(request->address_type())
        : EAddressType::InternalRpc;

    const auto* node = GetThisImpl<TChunkOwnerBase>();
    auto* chunkList = node->GetChunkList();
    const auto& comparator = GetComparator();
    for (const auto& protoRange : request->ranges()) {
        ValidateReadLimit(protoRange.lower_limit());
        ValidateReadLimit(protoRange.upper_limit());

        auto& range = fetchContext.Ranges.emplace_back();
        FromProto(&range, protoRange, comparator.GetLength());
    }

    auto visitor = New<TFetchChunkVisitor>(
        Bootstrap_,
        chunkList,
        context,
        std::move(fetchContext),
        std::move(comparator));
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

    auto* uploadTransaction = transactionManager->StartUploadTransaction(
        /* parent */ Transaction_,
        replicatedToCellTags,
        uploadTransactionTimeout,
        uploadTransactionTitle,
        uploadTransactionIdHint);

    auto* lockedNode = cypressManager
        ->LockNode(TrunkNode_, uploadTransaction, lockMode, false, true)
        ->As<TChunkOwnerBase>();

    if (!node->IsExternal()) {
        switch (uploadContext.Mode) {
            case EUpdateMode::Append: {
                auto* snapshotChunkList = lockedNode->GetChunkList();
                switch (snapshotChunkList->GetKind()) {
                    case EChunkListKind::Static: {
                        auto* newChunkList = chunkManager->CreateChunkList(EChunkListKind::Static);
                        newChunkList->AddOwningNode(lockedNode);

                        snapshotChunkList->RemoveOwningNode(lockedNode);
                        lockedNode->SetChunkList(newChunkList);
                        objectManager->RefObject(newChunkList);

                        chunkManager->AttachToChunkList(newChunkList, snapshotChunkList);

                        auto* deltaChunkList = chunkManager->CreateChunkList(EChunkListKind::Static);
                        chunkManager->AttachToChunkList(newChunkList, deltaChunkList);

                        objectManager->UnrefObject(snapshotChunkList);

                        context->SetIncrementalResponseInfo("NewChunkListId: %v, SnapshotChunkListId: %v, DeltaChunkListId: %v",
                            newChunkList->GetId(),
                            snapshotChunkList->GetId(),
                            deltaChunkList->GetId());
                        break;
                    }

                    case EChunkListKind::SortedDynamicRoot: {
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

                        context->SetIncrementalResponseInfo("NewChunkListId: %v, SnapshotChunkListId: %v",
                            newChunkList->GetId(),
                            snapshotChunkList->GetId());
                        break;
                    }

                    case EChunkListKind::JournalRoot:
                        break;

                    default:
                        THROW_ERROR_EXCEPTION("Unsupported chunk list kind %Qlv",
                            snapshotChunkList->GetKind());
                }
                break;
            }

            case EUpdateMode::Overwrite: {
                auto* oldChunkList = lockedNode->GetChunkList();
                switch (oldChunkList->GetKind()) {
                    case EChunkListKind::Static:
                    case EChunkListKind::SortedDynamicRoot: {
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

                        context->SetIncrementalResponseInfo("NewChunkListId: %v",
                            newChunkList->GetId());
                        break;
                    }

                    case EChunkListKind::JournalRoot:
                        break;

                    default:
                        THROW_ERROR_EXCEPTION("Unsupported chunk list kind %Qlv",
                            oldChunkList->GetKind());
                }
                break;
            }

            default:
                THROW_ERROR_EXCEPTION("Unsupported update mode %Qlv",
                    uploadContext.Mode);
        }
    }

    lockedNode->BeginUpload(uploadContext);

    auto uploadTransactionId = uploadTransaction->GetId();
    ToProto(response->mutable_upload_transaction_id(), uploadTransactionId);

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    response->set_cell_tag(externalCellTag == NotReplicatedCellTag
        ? multicellManager->GetCellTag()
        : externalCellTag);

    auto maybeExternalizeTransaction = [&] (TCellTag dstCellTag) {
        return node->IsExternal()
            ? transactionManager->ExternalizeTransaction(Transaction_, {dstCellTag})
            : GetObjectId(Transaction_);
    };

    if (node->IsExternal()) {
        auto externalizedTransactionId = maybeExternalizeTransaction(externalCellTag);

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
        for (auto dstCellTag : replicateStartToCellTags) {
            auto externalizedTransactionId = maybeExternalizeTransaction(dstCellTag);

            NTransactionServer::NProto::TReqStartForeignTransaction startRequest;
            ToProto(startRequest.mutable_id(), uploadTransactionId);
            if (externalizedTransactionId) {
                ToProto(startRequest.mutable_parent_id(), externalizedTransactionId);
            }
            if (uploadTransactionTitle) {
                startRequest.set_title(*uploadTransactionTitle);
            }
            startRequest.set_upload(true);

            multicellManager->PostToMaster(startRequest, dstCellTag);
        }
    }

    context->SetIncrementalResponseInfo("UploadTransactionId: %v",
        uploadTransactionId);
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
    auto* chunkList = node->GetChunkList();
    switch (chunkList->GetKind()) {
        case EChunkListKind::Static:
        case EChunkListKind::JournalRoot: {
            auto* snapshotChunkList = node->GetSnapshotChunkList();
            auto* deltaChunkList = node->GetDeltaChunkList();

            const auto& uploadChunkListId = deltaChunkList->GetId();
            ToProto(response->mutable_chunk_list_id(), uploadChunkListId);

            if (fetchLastKey) {
                TLegacyOwningKey lastKey;
                if (!IsEmpty(snapshotChunkList)) {
                    lastKey = GetUpperBoundKeyOrThrow(snapshotChunkList);
                }
                ToProto(response->mutable_last_key(), lastKey);
            }

            response->set_row_count(snapshotChunkList->Statistics().RowCount);

            std::optional<TMD5Hasher> md5Hasher;
            node->GetUploadParams(&md5Hasher);
            ToProto(response->mutable_md5_hasher(), md5Hasher);

            context->SetIncrementalResponseInfo("UploadChunkListId: %v, HasLastKey: %v, RowCount: %v",
                uploadChunkListId,
                response->has_last_key(),
                response->row_count());
            break;
        }

        case EChunkListKind::SortedDynamicRoot: {
            auto* trunkChunkList = node->GetTrunkNode()->As<TChunkOwnerBase>()->GetChunkList();

            for (auto* tabletList : trunkChunkList->Children()) {
                ToProto(response->add_pivot_keys(), tabletList->AsChunkList()->GetPivotKey());
            }

            for (auto* tabletList : chunkList->Children()) {
                auto chunkListKind = tabletList->AsChunkList()->GetKind();
                if (chunkListKind != EChunkListKind::SortedDynamicSubtablet &&
                    chunkListKind != EChunkListKind::SortedDynamicTablet)
                {
                    THROW_ERROR_EXCEPTION("Chunk list %v has unexpected kind %Qlv",
                        tabletList->GetId(),
                        chunkListKind);
                }
                ToProto(response->add_tablet_chunk_list_ids(), tabletList->GetId());
            }

            break;
        }

        default:
            THROW_ERROR_EXCEPTION("Chunk list %v has unexpected kind %Qlv",
                chunkList->GetId(),
                chunkList->GetKind());
    }

    response->set_max_heavy_columns(Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->MaxHeavyColumns);

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
        ExternalizeToMasters(context, {node->GetExternalCellTag()});
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
