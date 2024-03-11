#include "chunk.h"
#include "dynamic_store.h"
#include "chunk_list.h"
#include "chunk_view.h"
#include "chunk_manager.h"
#include "chunk_owner_node_proxy.h"
#include "chunk_reincarnator.h"
#include "chunk_visitor.h"
#include "config.h"
#include "helpers.h"
#include "medium_base.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/cypress_server/helpers.h>

#include <yt/yt/server/master/node_tracker_server/node_directory_builder.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/table_server/master_table_schema.h>

#include <yt/yt/server/master/tablet_server/tablet_manager.h>

#include <yt/yt/server/master/security_server/access_log.h>
#include <yt/yt/server/master/security_server/helpers.h>
#include <yt/yt/server/master/security_server/security_manager.h>
#include <yt/yt/server/master/security_server/security_tags.h>

#include <yt/yt/server/master/sequoia_server/config.h>

#include <yt/yt/server/master/table_server/table_manager.h>

#include <yt/yt/server/master/transaction_server/proto/transaction_manager.pb.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/file_client/file_chunk_writer.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/sequoia_client/client.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/misc/numeric_helpers.h>

#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/system_attribute_provider.h>

#include <type_traits>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCrypto;
using namespace NCypressClient;
using namespace NCypressServer;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NSequoiaClient;
using namespace NTableClient;
using namespace NTableServer;
using namespace NTabletServer;
using namespace NTransactionServer;
using namespace NYson;
using namespace NYTree;

using NChunkClient::NProto::TDataStatistics;

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
    const TChunkLocationPtrWithReplicaInfoList& chunkReplicas,
    std::optional<i64> rowIndex,
    std::optional<int> tabletIndex,
    const TReadLimit& lowerLimit,
    const TReadLimit& upperLimit,
    const TChunkViewModifier* modifier,
    bool fetchParityReplicas,
    bool fetchAllMetaExtensions,
    const THashSet<int>& extensionTags,
    NNodeTrackerServer::TNodeDirectoryBuilder* nodeDirectoryBuilder,
    TBootstrap* bootstrap,
    NChunkClient::NProto::TChunkSpec* chunkSpec)
{
    if (rowIndex) {
        chunkSpec->set_table_row_index(*rowIndex);
    }

    if (tabletIndex) {
        chunkSpec->set_tablet_index(*tabletIndex);
    }

    auto erasureCodecId = chunk->GetErasureCodec();
    auto firstInfeasibleReplicaIndex = (erasureCodecId == NErasure::ECodec::None || fetchParityReplicas)
        ? std::numeric_limits<int>::max() // all replicas are feasible
        : NErasure::GetCodec(erasureCodecId)->GetDataPartCount();

    TNodePtrWithReplicaAndMediumIndexList replicas;
    replicas.reserve(chunkReplicas.size());

    auto addReplica = [&] (TChunkLocationPtrWithReplicaInfo replica)  {
        if (replica.GetReplicaIndex() >= firstInfeasibleReplicaIndex) {
            return false;
        }
        const auto* location = replica.GetPtr();
        replicas.emplace_back(location->GetNode(), replica.GetReplicaIndex(), location->GetEffectiveMediumIndex());
        nodeDirectoryBuilder->Add(replica);
        return true;
    };

    for (auto replica : chunkReplicas) {
        addReplica(replica);
    }

    ToProto(chunkSpec->mutable_legacy_replicas(), replicas);
    ToProto(chunkSpec->mutable_replicas(), replicas);
    ToProto(chunkSpec->mutable_chunk_id(), chunk->GetId());
    chunkSpec->set_erasure_codec(ToProto<int>(erasureCodecId));
    chunkSpec->set_striped_erasure(chunk->GetStripedErasure());

    ToProto(
        chunkSpec->mutable_chunk_meta(),
        chunk->ChunkMeta(),
        fetchAllMetaExtensions ? nullptr : &extensionTags);

    // Try to keep responses small -- avoid producing redundant limits.
    if (!lowerLimit.IsTrivial()) {
        ToProto(chunkSpec->mutable_lower_limit(), lowerLimit);
    }
    if (!upperLimit.IsTrivial()) {
        ToProto(chunkSpec->mutable_upper_limit(), upperLimit);
    }

    i64 lowerRowLimit = lowerLimit.GetRowIndex().value_or(0);
    i64 upperRowLimit = upperLimit.GetRowIndex().value_or(chunk->GetRowCount());

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
    i64 dataWeight = chunk->GetDataWeight() > 0
        ? chunk->GetDataWeight()
        : chunk->GetUncompressedDataSize();

    if (chunkSpec->row_count_override() >= chunk->GetRowCount()) {
        chunkSpec->set_data_weight_override(dataWeight);
    } else {
        // NB: If overlayed chunk is nested into another, it has zero row count and non-zero data weight.
        i64 dataWeightPerRow = DivCeil(dataWeight, std::max<i64>(chunk->GetRowCount(), 1));
        chunkSpec->set_data_weight_override(dataWeightPerRow * chunkSpec->row_count_override());
    }

    if (modifier) {
        if (auto timestampTransactionId = modifier->GetTransactionId()) {
            const auto& transactionManager = bootstrap->GetTransactionManager();
            chunkSpec->set_override_timestamp(
                transactionManager->GetTimestampHolderTimestamp(timestampTransactionId));
        }

        if (auto maxClipTimestamp = modifier->GetMaxClipTimestamp()) {
            chunkSpec->set_max_clip_timestamp(maxClipTimestamp);
        }
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
        nodeDirectoryBuilder->Add(node);
        chunkSpec->add_legacy_replicas(ToProto<ui32>(TNodePtrWithReplicaIndex(node, GenericChunkReplicaIndex)));
        chunkSpec->add_replicas(ToProto<ui64>(TNodePtrWithReplicaAndMediumIndex(node, GenericChunkReplicaIndex, GenericMediumIndex)));
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
        TChunkLists chunkLists,
        TCtxFetchPtr rpcContext,
        TFetchContext&& fetchContext,
        TComparator comparator)
        : Bootstrap_(bootstrap)
        , ChunkLists_(std::move(chunkLists))
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
    TChunkLists ChunkLists_;
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
            ChunkLists_,
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

    bool OnChunk(
        TChunk* chunk,
        TChunkList* /*parent*/,
        std::optional<i64> rowIndex,
        std::optional<int> tabletIndex,
        const TReadLimit& lowerLimit,
        const TReadLimit& upperLimit,
        const TChunkViewModifier* modifier) override
    {
        Bootstrap_->VerifyPersistentStateRead();

        const auto& configManager = Bootstrap_->GetConfigManager();
        const auto& chunkManager = Bootstrap_->GetChunkManager();

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

        auto ephemeralChunk = TEphemeralObjectPtr<TChunk>(chunk);
        // This is context switch, chunk may die.
        auto replicasOrError = chunkManager->GetChunkReplicas(ephemeralChunk);
        if (!replicasOrError.IsOK()) {
            ReplyError(replicasOrError);
            return false;
        }

        if (!IsObjectAlive(ephemeralChunk)) {
            ReplyError(TError("Chunk %v died during replica fetch",
                ephemeralChunk->GetId()));
            return false;
        }

        const auto& replicas = replicasOrError.Value();

        auto* chunkSpec = RpcContext_->Response().add_chunks();

        BuildChunkSpec(
            chunk,
            replicas,
            rowIndex,
            tabletIndex,
            lowerLimit,
            upperLimit,
            modifier,
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

    bool OnChunkView(TChunkView* /*chunkView*/) override
    {
        if (FetchContext_.ThrowOnChunkViews) {
            THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::InvalidInputChunk,
                "Chunk view cannot be copied to remote cluster");
        }

        return false;
    }

    bool OnDynamicStore(
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

                i64 chunkStartRowIndex = dynamicStore->GetTableRowIndex();
                i64 chunkRowCount = chunk->GetStatistics().RowCount;

                if (relativeLowerLimit.GetRowIndex()) {
                    i64 relativeLowerRowIndex = *relativeLowerLimit.GetRowIndex() - chunkStartRowIndex;
                    if (relativeLowerRowIndex >= chunkRowCount) {
                        return true;
                    }
                    relativeLowerLimit.SetRowIndex(std::max<i64>(relativeLowerRowIndex, 0));
                }
                if (relativeUpperLimit.GetRowIndex()) {
                    i64 relativeUpperRowIndex = *relativeUpperLimit.GetRowIndex() - chunkStartRowIndex;
                    if (relativeUpperRowIndex <= 0) {
                        return true;
                    }
                    relativeUpperLimit.SetRowIndex(std::min<i64>(relativeUpperRowIndex, chunkRowCount));
                }

                return OnChunk(
                    chunk,
                    /*parent*/ nullptr,
                    dynamicStore->GetTableRowIndex(),
                    tabletIndex,
                    relativeLowerLimit,
                    relativeUpperLimit,
                    /*modifier*/ nullptr);
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

    void OnFinish(const TError& error) override
    {
        Bootstrap_->VerifyPersistentStateRead();

        if (!error.IsOK()) {
            ReplyError(error);
            return;
        }

        if (Finished_) {
            return;
        }

        ++CurrentRangeIndex_;
        if (CurrentRangeIndex_ == std::ssize(FetchContext_.Ranges)) {
            ReplySuccess();
        } else {
            TraverseCurrentRange();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ENodeType TChunkOwnerNodeProxy::GetType() const
{
    return ENodeType::Entity;
}

bool TChunkOwnerNodeProxy::DoInvoke(const IYPathServiceContextPtr& context)
{
    YT_LOG_ACCESS_IF(
        IsAccessLoggedMethod(context->GetMethod()),
        context,
        GetId(),
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
    auto isTrunk = node->IsTrunk();

    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkListId)
        .SetExternal(isExternal)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::HunkChunkListId)
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
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkFormatStatistics)
        .SetExternal(isExternal)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkMediaStatistics)
        .SetExternal(isExternal)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::SnapshotStatistics)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::DeltaStatistics)
        .SetOpaque(true));
    descriptors->push_back(EInternedAttributeKey::ChunkCount);
    descriptors->push_back(EInternedAttributeKey::UncompressedDataSize);
    descriptors->push_back(EInternedAttributeKey::CompressedDataSize);
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
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::HunkMedia)
        .SetWritable(true)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PrimaryMedium)
        .SetWritable(true)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::HunkPrimaryMedium)
        .SetWritable(true)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CompressionCodec)
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ErasureCodec)
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::EnableStripedErasure)
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::HunkErasureCodec)
        .SetWritable(true)
        .SetPresent(node->GetType() == EObjectType::Table));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::SecurityTags)
        .SetWritable(true)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkMergerMode)
        .SetWritable(true)
        .SetReplicated(true)
        .SetExternal(isExternal));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkMergerStatus)
        .SetExternal(isExternal)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::EnableSkynetSharing)
        .SetWritable(true)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkMergerTraversalInfo)
        .SetExternal(isExternal)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::VersionedResourceUsage)
        .SetPresent(!isTrunk));
    descriptors->emplace_back(EInternedAttributeKey::ScheduleReincarnation)
        .SetWritable(!isExternal)
        .SetPresent(false);
}

bool TChunkOwnerNodeProxy::GetBuiltinAttribute(
    TInternedAttributeKey key,
    IYsonConsumer* consumer)
{
    auto* node = GetThisImpl<TChunkOwnerBase>();
    const auto* chunkList = node->GetChunkList();
    const auto* hunkChunkList = node->GetHunkChunkList();
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

        case EInternedAttributeKey::HunkChunkListId:
            if (isExternal) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(GetObjectId(hunkChunkList));
            return true;

        case EInternedAttributeKey::ChunkCount:
            BuildYsonFluently(consumer)
                .Value(statistics.chunk_count());
            return true;

        case EInternedAttributeKey::SnapshotStatistics:
            BuildYsonFluently(consumer)
                .Value(node->SnapshotStatistics());
            return true;

        case EInternedAttributeKey::DeltaStatistics:
            BuildYsonFluently(consumer)
                .Value(node->DeltaStatistics());
            return true;

        case EInternedAttributeKey::UncompressedDataSize:
            BuildYsonFluently(consumer)
                .Value(statistics.uncompressed_data_size());
            return true;

        case EInternedAttributeKey::CompressedDataSize:
            BuildYsonFluently(consumer)
                .Value(statistics.compressed_data_size());
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
                .Value(node->GetUpdateMode());
            return true;

        case EInternedAttributeKey::Media: {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            const auto& replication = node->Replication();
            BuildYsonFluently(consumer)
                .Value(TSerializableChunkReplication(replication, chunkManager));
            return true;
        }

        case EInternedAttributeKey::HunkMedia: {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            auto replication = node->HunkReplication();
            auto defaultMedium = chunkManager->GetMediumByNameOrThrow(NChunkClient::DefaultStoreMediumName);
            if (!replication.Contains(defaultMedium->GetIndex())) {
                replication.Aggregate(defaultMedium->GetIndex(), TReplicationPolicy(DefaultReplicationFactor, false));
            }
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

        case EInternedAttributeKey::HunkPrimaryMedium: {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            auto primaryHunkMediumIndex = node->GetHunkPrimaryMediumIndex();
            auto* medium = chunkManager->GetMediumByIndex(primaryHunkMediumIndex);

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

        case EInternedAttributeKey::EnableStripedErasure:
            BuildYsonFluently(consumer)
                .Value(node->GetEnableStripedErasure());
            return true;

        case EInternedAttributeKey::HunkErasureCodec:
            // NB: Table node will override this.
            return false;

        case EInternedAttributeKey::SecurityTags:
            BuildYsonFluently(consumer)
                .Value(node->ComputeSecurityTags().Items);
            return true;

        case EInternedAttributeKey::ChunkMergerMode:
            if (isExternal) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(FormatEnum(node->GetChunkMergerMode()));
            return true;

        case EInternedAttributeKey::ChunkMergerStatus: {
            if (isExternal) {
                break;
            }

            const auto& chunkManager = Bootstrap_->GetChunkManager();
            BuildYsonFluently(consumer)
                .Value(chunkManager->GetNodeChunkMergerStatus(node->GetId()));
            return true;
        }

        case EInternedAttributeKey::EnableSkynetSharing:
            BuildYsonFluently(consumer)
                .Value(node->GetEnableSkynetSharing());
            return true;

        case EInternedAttributeKey::ChunkMergerTraversalInfo: {
            if (isExternal) {
                break;
            }

            const auto& traversalInfo = node->ChunkMergerTraversalInfo();
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("chunk_count").Value(traversalInfo.ChunkCount)
                    .Item("config_version").Value(traversalInfo.ConfigVersion)
                .EndMap();
            return true;
        }

        case EInternedAttributeKey::VersionedResourceUsage: {
            if (node->IsTrunk()) {
                break;
            }

            TDataStatistics extraResourceUsage;
            auto* originator = node->GetOriginator()->As<TChunkOwnerBase>();
            switch (node->GetUpdateMode()) {
                case EUpdateMode::Overwrite:
                    extraResourceUsage = node->ComputeTotalStatistics();
                    break;

                case EUpdateMode::Append:
                    extraResourceUsage = node->DeltaStatistics();
                    break;

                case EUpdateMode::None:
                    switch (originator->GetUpdateMode()) {
                        case EUpdateMode::Overwrite:
                        case EUpdateMode::None:
                            if (node->GetRevision() != originator->GetRevision()) {
                                extraResourceUsage = node->ComputeTotalStatistics();
                            }
                            break;

                        case EUpdateMode::Append:
                            break;

                        default:
                            YT_ABORT();
                    }
                    break;

                default:
                    YT_ABORT();
            }

            BuildYsonFluently(consumer)
                .Value(extraResourceUsage);
            return true;
        }

        default:
            break;
    }

    return TNontemplateCypressNodeProxyBase::GetBuiltinAttribute(key, consumer);
}

TFuture<TYsonString> TChunkOwnerNodeProxy::GetBuiltinAttributeAsync(TInternedAttributeKey key)
{
    auto* node = GetThisImpl<TChunkOwnerBase>();
    auto chunkLists = node->GetChunkLists();
    auto isExternal = node->IsExternal();

    switch (key) {
        case EInternedAttributeKey::ChunkIds: {
            if (isExternal) {
                break;
            }
            auto visitor = New<TChunkIdsAttributeVisitor>(
                Bootstrap_,
                chunkLists);
            return visitor->Run();
        }

        case EInternedAttributeKey::CompressionStatistics:
            if (isExternal) {
                break;
            }
            return ComputeChunkStatistics(
                Bootstrap_,
                chunkLists,
                [] (const TChunk* chunk) { return chunk->GetCompressionCodec(); });

        case EInternedAttributeKey::ErasureStatistics:
            if (isExternal) {
                break;
            }
            return ComputeChunkStatistics(
                Bootstrap_,
                chunkLists,
                [] (const TChunk* chunk) { return chunk->GetErasureCodec(); });

        case EInternedAttributeKey::MulticellStatistics:
            if (isExternal) {
                break;
            }
            return ComputeChunkStatistics(
                Bootstrap_,
                chunkLists,
                [] (const TChunk* chunk) { return chunk->GetNativeCellTag(); });

        case EInternedAttributeKey::ChunkFormatStatistics:
            if (isExternal) {
                break;
            }
            return ComputeChunkStatistics(
                Bootstrap_,
                chunkLists,
                [] (const TChunk* chunk) { return chunk->GetChunkFormat(); });

        case EInternedAttributeKey::ChunkMediaStatistics: {
            if (isExternal) {
                break;
            }

            auto chunkManager = Bootstrap_->GetChunkManager();
            return ComputeChunkStatistics(
                Bootstrap_,
                chunkLists,
                [chunkManager] (const TChunk* chunk) -> std::optional<int> {
                    // TODO(aleksandra-zh): batch getting replicas.
                    auto ephemeralChunk = TEphemeralObjectPtr<TChunk>(const_cast<TChunk*>(chunk));
                    // This is context switch, chunk may die.
                    auto replicas = chunkManager->GetChunkReplicas(ephemeralChunk)
                        .ValueOrThrow();
                    if (replicas.empty()) {
                        return std::nullopt;
                    }

                    // We should choose a single medium for the chunk if there are replicas
                    // with different media. We choose the most frequent medium if more than
                    // half replicas belong to it, otherwise arbitrary one.
                    int chosenMediumIndex = -1;
                    int chosenMediumReplicaCount = 0;

                    for (auto replica : replicas) {
                        int mediumIndex = replica.GetPtr()->GetEffectiveMediumIndex();
                        if (mediumIndex == chosenMediumIndex || chosenMediumReplicaCount == 0) {
                            chosenMediumIndex = mediumIndex;
                            ++chosenMediumReplicaCount;
                        } else {
                            --chosenMediumReplicaCount;
                        }
                    }

                    YT_VERIFY(chosenMediumIndex != -1);
                    return chosenMediumIndex;
                },
                [=] (int mediumIndex) {
                    return chunkManager->GetMediumByIndexOrThrow(mediumIndex)->GetName();
                });
        }

        default:
            break;
    }

    return TNontemplateCypressNodeProxyBase::GetBuiltinAttributeAsync(key);
}

bool TChunkOwnerNodeProxy::SetBuiltinAttribute(
    TInternedAttributeKey key,
    const TYsonString& value,
    bool force)
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

        case EInternedAttributeKey::HunkPrimaryMedium: {
            ValidateStorageParametersUpdate();
            auto mediumName = ConvertTo<TString>(value);
            auto* medium = chunkManager->GetMediumByNameOrThrow(mediumName);
            SetHunkPrimaryMedium(medium);
            return true;
        }

        case EInternedAttributeKey::Media: {
            ValidateStorageParametersUpdate();
            auto serializableReplication = ConvertTo<TSerializableChunkReplication>(value);
            // Copying for modification.
            auto replication = GetThisImpl<TChunkOwnerBase>()->Replication();
            // Preserves vitality.
            serializableReplication.ToChunkReplication(&replication, chunkManager);
            SetReplication(replication);
            return true;
        }

        case EInternedAttributeKey::HunkMedia: {
            ValidateStorageParametersUpdate();
            auto serializableReplication = ConvertTo<TSerializableChunkReplication>(value);
            // Copying for modification.
            auto replication = GetThisImpl<TChunkOwnerBase>()->HunkReplication();
            // Preserves vitality.
            serializableReplication.ToChunkReplication(&replication, chunkManager);
            SetHunkReplication(replication);
            return true;
        }

        case EInternedAttributeKey::CompressionCodec: {
            if (TrunkNode_->GetType() == EObjectType::Journal) {
                THROW_ERROR_EXCEPTION("Journal compression codec cannot be set");
            }

            ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

            const auto& uninternedKey = key.Unintern();
            auto codec = ConvertTo<NCompression::ECodec>(value);

            ValidateCompressionCodec(
                value,
                config->DeprecatedCodecIds,
                config->DeprecatedCodecNameToAlias);

            auto* node = LockThisImpl<TChunkOwnerBase>(TLockRequest::MakeSharedAttribute(uninternedKey));
            node->SetCompressionCodec(codec);

            return true;
        }

        case EInternedAttributeKey::ErasureCodec: {
            if (TrunkNode_->GetType() == EObjectType::Journal) {
                THROW_ERROR_EXCEPTION("Journal erasure codec cannot be changed after creation");
            }

            ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

            const auto& uninternedKey = key.Unintern();
            auto codec = ConvertTo<NErasure::ECodec>(value);
            auto* node = LockThisImpl<TChunkOwnerBase>(TLockRequest::MakeSharedAttribute(uninternedKey));
            node->SetErasureCodec(codec);

            return true;
        }

        case EInternedAttributeKey::EnableStripedErasure: {
            if (TrunkNode_->GetType() == EObjectType::Journal) {
                THROW_ERROR_EXCEPTION("Striped erasure cannot be enabled for journals");
            }

            ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

            const auto& uninternedKey = key.Unintern();
            auto enableStripedErasure = ConvertTo<bool>(value);
            auto* node = LockThisImpl<TChunkOwnerBase>(TLockRequest::MakeSharedAttribute(uninternedKey));
            node->SetEnableStripedErasure(enableStripedErasure);

            return true;
        }

        case EInternedAttributeKey::HunkErasureCodec:
            // NB: Table node will override this.
            THROW_ERROR_EXCEPTION("Hunk erasure codec can only be set for tables");

        case EInternedAttributeKey::Account: {
            if (!TNontemplateCypressNodeProxyBase::SetBuiltinAttribute(key, value, force)) {
                return false;
            }

            const auto& uninternedKey = key.Unintern();
            auto* node = LockThisImpl<TChunkOwnerBase>(TLockRequest::MakeSharedAttribute(uninternedKey));
            if (!node->IsExternal()) {
                ScheduleRequisitionUpdate();
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
            YT_LOG_DEBUG("Node security tags updated; node is switched to \"overwrite\" mode (NodeId: %v, OldSecurityTags: %v, NewSecurityTags: %v",
                node->GetVersionedId(),
                node->ComputeSecurityTags().Items,
                securityTags.Items);

            const auto& securityManager = Bootstrap_->GetSecurityManager();
            const auto& securityTagsRegistry = securityManager->GetSecurityTagsRegistry();
            node->SnapshotSecurityTags() = securityTagsRegistry->Intern(std::move(securityTags));
            node->SetUpdateMode(EUpdateMode::Overwrite);
            return true;
        }

        case EInternedAttributeKey::ChunkMergerMode: {
            if (config->ChunkMerger->AllowSettingChunkMergerMode) {
                ValidatePermission(EPermissionCheckScope::This, EPermission::Write);
            } else {
                ValidatePermission(EPermissionCheckScope::This, EPermission::Administer);
            }

            const auto& uninternedKey = key.Unintern();
            auto* node = LockThisImpl<TChunkOwnerBase>(TLockRequest::MakeSharedAttribute(uninternedKey));

            auto mode = ConvertTo<EChunkMergerMode>(value);
            node->SetChunkMergerMode(mode);

            if (!node->IsExternal()) {
                MaybeScheduleChunkMerge();
            }
            return true;
        }

        case EInternedAttributeKey::EnableSkynetSharing: {
            ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

            const auto& uninternedKey = key.Unintern();
            auto enable = ConvertTo<bool>(value);
            auto* node = LockThisImpl<TChunkOwnerBase>(TLockRequest::MakeSharedAttribute(uninternedKey));
            node->SetEnableSkynetSharing(enable);

            return true;
        }

        case EInternedAttributeKey::ScheduleReincarnation: {
            auto* node = GetThisImpl<TChunkOwnerBase>();
            if (node->IsExternal()) {
                THROW_ERROR_EXCEPTION("Reincarnation cannot be scheduled for external chunk owners");
            }

            ValidateSuperuserOnAttributeModification(Bootstrap_->GetSecurityManager(), key.Unintern());

            const auto& chunkReincarnator = chunkManager->GetChunkReincarnator();
            chunkReincarnator->ScheduleReincarnation(
                node->GetChunkList(),
                ConvertTo<TChunkReincarnationOptions>(value));

            return true;
        }

        default:
            break;
    }

    return TNontemplateCypressNodeProxyBase::SetBuiltinAttribute(key, value, force);
}

void TChunkOwnerNodeProxy::OnStorageParametersUpdated()
{
    auto* node = GetThisImpl<TChunkOwnerBase>();
    if (node->IsExternal()) {
        return;
    }

    ScheduleRequisitionUpdate();

    const auto& tabletManager = Bootstrap_->GetTabletManager();
    tabletManager->OnNodeStorageParametersUpdated(node);
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
    OnStorageParametersUpdated();
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
    OnStorageParametersUpdated();
}

TString TChunkOwnerNodeProxy::DoSetReplication(TChunkReplication* replicationStorage, const TChunkReplication& replication, int mediumIndex) {
    auto* node = GetThisImpl<TChunkOwnerBase>();
    const auto& chunkManager = Bootstrap_->GetChunkManager();

    YT_VERIFY(node->IsTrunk());

    ValidateMediaChange(*replicationStorage, mediumIndex, replication);

    *replicationStorage = replication;
    OnStorageParametersUpdated();

    const auto* primaryMedium = chunkManager->GetMediumByIndex(mediumIndex);
    return primaryMedium->GetName();
}

void TChunkOwnerNodeProxy::SetReplication(const TChunkReplication& replication)
{
    auto* node = GetThisImpl<TChunkOwnerBase>();
    auto name = DoSetReplication(&node->Replication(), replication, node->GetPrimaryMediumIndex());
    YT_LOG_DEBUG(
        "Chunk owner replication changed (NodeId: %v, PrimaryMedium: %v, Replication: %v)",
        node->GetId(),
        name,
        node->Replication());
}

void TChunkOwnerNodeProxy::SetHunkReplication(const TChunkReplication& replication)
{
    auto* node = GetThisImpl<TChunkOwnerBase>();
    auto name = DoSetReplication(&node->HunkReplication(), replication, node->GetHunkPrimaryMediumIndex());
    YT_LOG_DEBUG(
        "Chunk owner hunk replication changed (NodeId: %v, Replication: %v, HunkReplication %v)",
        node->GetId(),
        name,
        node->HunkReplication());
}

void TChunkOwnerNodeProxy::SetHunkPrimaryMedium(TMedium* medium)
{
    auto* node = GetThisImpl<TChunkOwnerBase>();
    YT_VERIFY(node->IsTrunk());

    TChunkReplication newReplication;
    if (!ValidatePrimaryMediumChange(
        medium,
        node->HunkReplication(),
        node->GetHunkPrimaryMediumIndex(),
        &newReplication))
    {
        return;
    }

    node->HunkReplication() = newReplication;
    node->SetHunkPrimaryMediumIndex(medium->GetIndex());
    OnStorageParametersUpdated();

    YT_LOG_DEBUG(
        "Chunk owner hunk primary medium changed (NodeId: %v, PrimaryMedium: %v)",
        node->GetId(),
        medium->GetName());
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
    OnStorageParametersUpdated();

    YT_LOG_DEBUG(
        "Chunk owner primary medium changed (NodeId: %v, PrimaryMedium: %v)",
        node->GetId(),
        medium->GetName());
}

void TChunkOwnerNodeProxy::ValidateReadLimit(const NChunkClient::NProto::TReadLimit& /*readLimit*/) const
{ }

void TChunkOwnerNodeProxy::MaybeScheduleChunkMerge()
{
    auto* node = GetThisImpl<TChunkOwnerBase>();

    if (node->IsTrunk() && node->GetChunkMergerMode() != EChunkMergerMode::None) {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->ScheduleChunkMerge(node);
    }
}

void TChunkOwnerNodeProxy::ScheduleRequisitionUpdate()
{
    auto* node = GetThisImpl<TChunkOwnerBase>();

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    for (auto* chunkList : node->GetChunkLists()) {
        if (chunkList) {
            chunkManager->ScheduleChunkRequisitionUpdate(chunkList);
        }
    }
}

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
        context->SecurityTags = node->ComputeSecurityTags();
    }

    auto* transaction = GetTransaction();
    if (node->IsExternal()) {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        context->ExternalTransactionId = transactionManager->GetNearestExternalizedTransactionAncestor(
            transaction,
            node->GetExternalCellTag());
    }
}

void TChunkOwnerNodeProxy::ReplicateBeginUploadRequestToExternalCell(
    TChunkOwnerBase* node,
    TTransactionId uploadTransactionId,
    NChunkClient::NProto::TReqBeginUpload* request,
    TChunkOwnerBase::TBeginUploadContext& uploadContext) const
{
    auto externalCellTag = node->GetExternalCellTag();
    const auto& transactionManager = Bootstrap_->GetTransactionManager();
    auto externalizedTransactionId = node->IsExternal()
        ? transactionManager->ExternalizeTransaction(Transaction_, {externalCellTag})
        : GetObjectId(Transaction_);

    auto replicationRequest = TChunkOwnerYPathProxy::BeginUpload(FromObjectId(GetId()));
    replicationRequest->set_update_mode(request->update_mode());
    replicationRequest->set_lock_mode(request->lock_mode());

    if (request->has_schema_mode()) {
        replicationRequest->set_schema_mode(request->schema_mode());
    }

    ToProto(replicationRequest->mutable_upload_transaction_id(), uploadTransactionId);
    if (request->has_upload_transaction_title()) {
        replicationRequest->set_upload_transaction_title(request->upload_transaction_title());
    }

    // NB: Journals and files have no schema, thus no need to replicate one.
    if (uploadContext.TableSchema) {
        auto tableSchemaId = uploadContext.TableSchema->GetId();
        ToProto(replicationRequest->mutable_table_schema_id(), tableSchemaId);
    }

    if (uploadContext.ChunkSchema) {
        if (!uploadContext.ChunkSchema->IsExported(externalCellTag)) {
            ToProto(replicationRequest->mutable_chunk_schema(), uploadContext.ChunkSchema->AsTableSchema());
        }

        auto chunkSchemaId = uploadContext.ChunkSchema->GetId();
        ToProto(replicationRequest->mutable_chunk_schema_id(), chunkSchemaId);
    }

    SetTransactionId(replicationRequest, externalizedTransactionId);
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    // NB: upload_transaction_timeout must remain null
    // NB: upload_transaction_secondary_cell_tags must remain empty
    multicellManager->PostToMaster(replicationRequest, externalCellTag);
}

void TChunkOwnerNodeProxy::ReplicateEndUploadRequestToExternalCell(
    TChunkOwnerBase* node,
    NChunkClient::NProto::TReqEndUpload* request,
    TChunkOwnerBase::TEndUploadContext& uploadContext) const
{
    auto externalCellTag = node->GetExternalCellTag();
    const auto& transactionManager = Bootstrap_->GetTransactionManager();
    auto externalizedTransactionId = transactionManager->ExternalizeTransaction(Transaction_, {externalCellTag});

    auto replicationRequest = TChunkOwnerYPathProxy::EndUpload(FromObjectId(GetId()));
    if (request->has_statistics()) {
        replicationRequest->mutable_statistics()->CopyFrom(request->statistics());
    }
    if (request->has_optimize_for()) {
        replicationRequest->set_optimize_for(request->optimize_for());
    }
    if (request->has_chunk_format()) {
        replicationRequest->set_chunk_format(request->chunk_format());
    }
    if (request->has_compression_codec()) {
        replicationRequest->set_compression_codec(request->compression_codec());
    }
    if (request->has_erasure_codec()) {
        replicationRequest->set_erasure_codec(request->erasure_codec());
    }
    if (request->has_md5_hasher()) {
        replicationRequest->mutable_md5_hasher()->CopyFrom(request->md5_hasher());
    }
    if (request->has_security_tags()) {
        replicationRequest->mutable_security_tags()->CopyFrom(request->security_tags());
    }

    // COMPAT(h0pless): remove this when clients will send table schema options during begin upload.
    // NB: Journals and files have no schema, thus no need to replicate one.
    if (uploadContext.TableSchema) {
        auto tableSchemaId = uploadContext.TableSchema->GetId();
        // Schema was exported during EndUpload call, it's safe to send id only.
        ToProto(replicationRequest->mutable_table_schema_id(), tableSchemaId);
    }
    if (request->has_schema_mode()) {
        replicationRequest->set_schema_mode(request->schema_mode());
    }

    SetTransactionId(replicationRequest, externalizedTransactionId);
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    multicellManager->PostToMaster(replicationRequest, externalCellTag);
}

TMasterTableSchema* TChunkOwnerNodeProxy::CalculateEffectiveMasterTableSchema(
    TChunkOwnerBase* node,
    TTableSchemaPtr schema,
    TMasterTableSchemaId schemaId,
    TTransaction* schemaHolder)
{
    const auto& tableManager = Bootstrap_->GetTableManager();
    if (node->IsNative()) {
        if (schema) {
            return tableManager->GetOrCreateNativeMasterTableSchema(*schema, schemaHolder);
        }

        if (schemaId) {
            return tableManager->GetMasterTableSchemaOrThrow(schemaId);
        }

        return tableManager->GetEmptyMasterTableSchema();
    }

    if (schema) {
        // COMPAT(h0pless): Remove this after schema migration is complete.
        if (!schemaId) {
            YT_LOG_ALERT("Created native schema on an external cell tag (NodeId: %v, TransactionId: %v)",
                node->GetId(),
                schemaHolder->GetId());
            return tableManager->GetOrCreateNativeMasterTableSchema(*schema, schemaHolder);
        }

        return tableManager->CreateImportedTemporaryMasterTableSchema(*schema, schemaHolder, schemaId);
    }

    if (!schemaId) {
        YT_LOG_ALERT("Used empty native schema on an external cell tag (NodeId: %v)",
            node->GetId());
        return tableManager->GetEmptyMasterTableSchema();
    }

    return tableManager->GetMasterTableSchema(schemaId);
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
    auto chunkLists = node->GetChunkLists();
    const auto& comparator = GetComparator();
    for (const auto& protoRange : request->ranges()) {
        ValidateReadLimit(protoRange.lower_limit());
        ValidateReadLimit(protoRange.upper_limit());

        auto& range = fetchContext.Ranges.emplace_back();
        FromProto(&range, protoRange, comparator.GetLength());
    }

    auto visitor = New<TFetchChunkVisitor>(
        Bootstrap_,
        std::move(chunkLists),
        context,
        std::move(fetchContext),
        std::move(comparator));
    visitor->Run();
}

DEFINE_YPATH_SERVICE_METHOD(TChunkOwnerNodeProxy, BeginUpload)
{
    DeclareMutating();

    TChunkOwnerBase::TBeginUploadContext uploadContext(Bootstrap_);
    uploadContext.Mode = CheckedEnumCast<EUpdateMode>(request->update_mode());
    if (uploadContext.Mode != EUpdateMode::Append && uploadContext.Mode != EUpdateMode::Overwrite) {
        THROW_ERROR_EXCEPTION("Invalid update mode %Qlv for a chunk owner node",
            uploadContext.Mode);
    }

    YT_LOG_ACCESS(
        context,
        GetId(),
        GetPath(),
        Transaction_,
        {{"mode", FormatEnum(uploadContext.Mode)}});

    auto lockMode = CheckedEnumCast<ELockMode>(request->lock_mode());

    auto uploadTransactionTitle = request->has_upload_transaction_title()
        ? std::make_optional(request->upload_transaction_title())
        : std::nullopt;

    auto uploadTransactionTimeout = request->has_upload_transaction_timeout()
        ? std::make_optional(FromProto<TDuration>(request->upload_transaction_timeout()))
        : std::nullopt;

    auto tableSchema = request->has_table_schema()
        ? FromProto<TTableSchemaPtr>(request->table_schema())
        : nullptr;

    auto tableSchemaId = FromProto<TMasterTableSchemaId>(request->table_schema_id());

    auto chunkSchema = request->has_chunk_schema()
        ? FromProto<TTableSchemaPtr>(request->chunk_schema())
        : nullptr;

    auto chunkSchemaId = FromProto<TMasterTableSchemaId>(request->chunk_schema_id());

    uploadContext.SchemaMode = CheckedEnumCast<ETableSchemaMode>(request->schema_mode());

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
        "SchemaMode: %v, UpdateMode: %v, LockMode: %v, Title: %v, "
        "Timeout: %v, ReplicatedToCellTags: %v, TableSchemaId: %v, ChunkSchemaId: %v",
        uploadContext.SchemaMode,
        uploadContext.Mode,
        lockMode,
        uploadTransactionTitle,
        uploadTransactionTimeout,
        replicatedToCellTags,
        tableSchemaId,
        chunkSchemaId);

    // NB: No need for a permission check;
    // the client must have invoked GetBasicAttributes.

    ValidateBeginUpload();

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    const auto& transactionManager = Bootstrap_->GetTransactionManager();

    YT_LOG_ALERT_IF(!IsSchemafulType(node->GetType()) && (tableSchema || tableSchemaId),
        "Received a schema or schema ID while beginning upload into a non-schemaful node (NodeId: %v, Schema: %v, SchemaId: %v)",
        node->GetId(),
        tableSchema,
        tableSchemaId);

    YT_LOG_ALERT_IF(!IsSchemafulType(node->GetType()) && (chunkSchema || chunkSchemaId),
        "Received a chunk schema or chunk schema ID while beginning upload into a non-schemaful node (NodeId: %v, ChunkSchema: %v, ChunkSchemaId: %v)",
        node->GetId(),
        chunkSchema,
        chunkSchemaId);

    std::vector<TTransaction*> prerequisiteTransactions;
    prerequisiteTransactions.reserve(request->upload_prerequisite_transaction_ids_size());
    for (auto id : request->upload_prerequisite_transaction_ids()) {
        auto transactionId = FromProto<TTransactionId>(id);
        auto* transaction = transactionManager->GetAndValidatePrerequisiteTransaction(transactionId);
        prerequisiteTransactions.push_back(transaction);
    }

    auto* uploadTransaction = transactionManager->StartUploadTransaction(
        /*parent*/ Transaction_,
        prerequisiteTransactions,
        replicatedToCellTags,
        uploadTransactionTimeout,
        uploadTransactionTitle,
        uploadTransactionIdHint);

    auto* lockedNode = cypressManager
        ->LockNode(TrunkNode_, uploadTransaction, lockMode, false, true)
        ->As<TChunkOwnerBase>();

    const auto& tableManager = Bootstrap_->GetTableManager();
    if (IsSchemafulType(node->GetType())) {
        tableManager->ValidateTableSchemaCorrespondence(
            node->GetVersionedId(),
            tableSchema,
            tableSchemaId);

        uploadContext.TableSchema = CalculateEffectiveMasterTableSchema(node, tableSchema, tableSchemaId, uploadTransaction);

        // NB: Chunk schema is at least as strict as the table schema, possibly more strict.
        // Thus we can send extra information only when they differ, and otherwise treat them the same way.
        if (chunkSchema || chunkSchemaId) {
            tableManager->ValidateTableSchemaCorrespondence(
                node->GetVersionedId(),
                chunkSchema,
                chunkSchemaId,
                /*isChunkSchema*/ true);

            uploadContext.ChunkSchema = CalculateEffectiveMasterTableSchema(node, chunkSchema, chunkSchemaId, uploadTransaction);
            ToProto(response->mutable_upload_chunk_schema_id(), uploadContext.ChunkSchema->GetId());
        } else {
            ToProto(response->mutable_upload_chunk_schema_id(), uploadContext.TableSchema->GetId());
        }
    }

    if (!node->IsExternal()) {
        switch (uploadContext.Mode) {
            case EUpdateMode::Append: {
                auto* snapshotChunkList = lockedNode->GetChunkList();
                switch (snapshotChunkList->GetKind()) {
                    case EChunkListKind::Static: {
                        YT_VERIFY(!lockedNode->GetHunkChunkList());

                        auto* newChunkList = chunkManager->CreateChunkList(EChunkListKind::Static);
                        newChunkList->AddOwningNode(lockedNode);

                        snapshotChunkList->RemoveOwningNode(lockedNode);
                        lockedNode->SetChunkList(newChunkList);

                        chunkManager->AttachToChunkList(newChunkList, {snapshotChunkList});

                        auto* deltaChunkList = chunkManager->CreateChunkList(EChunkListKind::Static);
                        chunkManager->AttachToChunkList(newChunkList, {deltaChunkList});

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

                        for (int index = 0; index < std::ssize(snapshotChunkList->Children()); ++index) {
                            auto* appendChunkList = chunkManager->CreateChunkList(EChunkListKind::SortedDynamicSubtablet);
                            chunkManager->AttachToChunkList(newChunkList, {appendChunkList});
                        }

                        snapshotChunkList->RemoveOwningNode(lockedNode);

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
                auto* oldMainChunkList = lockedNode->GetChunkList();
                switch (oldMainChunkList->GetKind()) {
                    case EChunkListKind::Static:
                    case EChunkListKind::SortedDynamicRoot: {
                        auto processChunkList = [&] (EChunkListContentType contentType, EChunkListKind appendChunkListKind) {
                            auto* oldChunkList = lockedNode->GetChunkList(contentType);
                            if (!oldChunkList) {
                                YT_VERIFY(contentType == EChunkListContentType::Hunk && oldMainChunkList->GetKind() == EChunkListKind::Static);
                                return;
                            }

                            auto* newChunkList = chunkManager->CreateChunkList(oldChunkList->GetKind());
                            newChunkList->AddOwningNode(lockedNode);
                            lockedNode->SetChunkList(contentType, newChunkList);

                            if (oldMainChunkList->GetKind() == EChunkListKind::SortedDynamicRoot) {
                                for (int index = 0; index < std::ssize(oldMainChunkList->Children()); ++index) {
                                    auto* appendChunkList = chunkManager->CreateChunkList(appendChunkListKind);
                                    chunkManager->AttachToChunkList(newChunkList, {appendChunkList});
                                }
                            }

                            oldChunkList->RemoveOwningNode(lockedNode);
                        };
                        processChunkList(EChunkListContentType::Main, EChunkListKind::SortedDynamicTablet);
                        processChunkList(EChunkListContentType::Hunk, EChunkListKind::Hunk);

                        if (oldMainChunkList->GetKind() == EChunkListKind::SortedDynamicRoot) {
                            context->SetIncrementalResponseInfo("NewChunkListId: %v, NewHunkChunkListId: %v",
                                lockedNode->GetChunkList()->GetId(),
                                lockedNode->GetHunkChunkList()->GetId());
                        } else {
                            context->SetIncrementalResponseInfo("NewChunkListId: %v",
                                lockedNode->GetChunkList()->GetId());
                        }
                        break;
                    }

                    case EChunkListKind::JournalRoot:
                        break;

                    default:
                        THROW_ERROR_EXCEPTION("Unsupported chunk list kind %Qlv",
                            oldMainChunkList->GetKind());
                }
                break;
            }

            default:
                THROW_ERROR_EXCEPTION("Unsupported update mode %Qlv",
                    uploadContext.Mode);
        }
    }

    auto uploadTransactionId = uploadTransaction->GetId();
    ToProto(response->mutable_upload_transaction_id(), uploadTransactionId);

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    response->set_cell_tag(ToProto<int>(externalCellTag == NotReplicatedCellTagSentinel
        ? multicellManager->GetCellTag()
        : externalCellTag));

    lockedNode->BeginUpload(uploadContext);

    if (node->IsExternal()) {
        ReplicateBeginUploadRequestToExternalCell(node, uploadTransactionId, request, uploadContext);
    }

    if (!replicateStartToCellTags.empty()) {
        for (auto dstCellTag : replicateStartToCellTags) {
            auto externalizedTransactionId =
                transactionManager->ExternalizeTransaction(Transaction_, {dstCellTag});

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

    TChunkOwnerBase::TEndUploadContext uploadContext(Bootstrap_);

    // COMPAT(h0pless): remove this when clients will send table schema options during begin upload.
    auto tableSchema = request->has_table_schema()
        ? FromProto<TTableSchemaPtr>(request->table_schema())
        : nullptr;
    auto tableSchemaId = FromProto<TMasterTableSchemaId>(request->table_schema_id());
    uploadContext.SchemaMode = CheckedEnumCast<ETableSchemaMode>(request->schema_mode());

    if (request->has_statistics()) {
        uploadContext.Statistics = &request->statistics();
    }

    if (request->has_optimize_for()) {
        uploadContext.OptimizeFor = CheckedEnumCast<EOptimizeFor>(request->optimize_for());
    }

    if (request->has_chunk_format()) {
        uploadContext.ChunkFormat = CheckedEnumCast<EChunkFormat>(request->chunk_format());
    }

    if (request->has_md5_hasher()) {
        uploadContext.MD5Hasher = FromProto<std::optional<TMD5Hasher>>(request->md5_hasher());
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

    context->SetRequestInfo("Statistics: %v, CompressionCodec: %v, ErasureCodec: %v, ChunkFormat: %v, MD5Hasher: %v, OptimizeFor: %v",
        uploadContext.Statistics,
        uploadContext.CompressionCodec,
        uploadContext.ErasureCodec,
        uploadContext.ChunkFormat,
        uploadContext.MD5Hasher.has_value(),
        uploadContext.OptimizeFor);

    ValidateTransaction();
    ValidateInUpdate();

    if (uploadContext.ChunkFormat && uploadContext.OptimizeFor) {
        ValidateTableChunkFormatAndOptimizeFor(*uploadContext.ChunkFormat, *uploadContext.OptimizeFor);
    }

    auto* node = GetThisImpl<TChunkOwnerBase>();
    YT_VERIFY(node->GetTransaction() == Transaction_);

    YT_LOG_ALERT_IF(!IsSchemafulType(node->GetType()) && (tableSchema || tableSchemaId),
        "Received a schema or schema ID while ending upload into a non-schemaful node (NodeId: %v, Schema: %v, SchemaId: %v)",
        node->GetId(),
        tableSchema,
        tableSchemaId);

    const auto& tableManager = Bootstrap_->GetTableManager();
    if (IsTableType(node->GetType())) {
        tableManager->ValidateTableSchemaCorrespondence(
            node->GetVersionedId(),
            tableSchema,
            tableSchemaId);

        uploadContext.TableSchema = CalculateEffectiveMasterTableSchema(node, tableSchema, tableSchemaId, Transaction_);
    }

    node->EndUpload(uploadContext);

    if (node->IsExternal()) {
        ReplicateEndUploadRequestToExternalCell(node, request, uploadContext);
    }

    SetModified(EModificationType::Content);

    if (node->IsNative()) {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->CommitMasterTransaction(Transaction_, /*options*/ {});
    }

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
