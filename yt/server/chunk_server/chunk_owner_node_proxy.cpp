#include "chunk.h"
#include "chunk_list.h"
#include "chunk_manager.h"
#include "chunk_owner_node_proxy.h"
#include "chunk_visitor.h"
#include "config.h"
#include "helpers.h"
#include "medium.h"
#include "private.h"

#include <yt/server/cell_master/config.h>
#include <yt/server/cell_master/multicell_manager.h>

#include <yt/server/node_tracker_server/node_directory_builder.h>

#include <yt/server/object_server/object.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>

#include <yt/ytlib/file_client/file_chunk_writer.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/erasure/codec.h>

#include <yt/core/misc/numeric_helpers.h>

#include <yt/core/ytree/helpers.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/node.h>
#include <yt/core/ytree/system_attribute_provider.h>

#include <type_traits>

namespace NYT {
namespace NChunkServer {

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

class TFetchChunkVisitor
    : public IChunkVisitor
{
public:
    typedef NRpc::TTypedServiceContext<TReqFetch, TRspFetch> TCtxFetch;
    typedef TIntrusivePtr<TCtxFetch> TCtxFetchPtr;

    TFetchChunkVisitor(
        NCellMaster::TBootstrap* bootstrap,
        TChunkManagerConfigPtr config,
        TChunkList* chunkList,
        TCtxFetchPtr context,
        bool fetchParityReplicas,
        EAddressType addressType,
        const std::vector<TReadRange>& ranges)
        : Bootstrap_(bootstrap)
        , Config_(config)
        , ChunkList_(chunkList)
        , Context_(context)
        , FetchParityReplicas_(fetchParityReplicas)
        , Ranges_(ranges)
        , NodeDirectoryBuilder_(
            context->Response().mutable_node_directory(),
            addressType)
    {
        if (!Context_->Request().fetch_all_meta_extensions()) {
            for (int tag : Context_->Request().extension_tags()) {
                ExtensionTags_.insert(tag);
            }
        }
    }

    void Run()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (Ranges_.empty()) {
            ReplySuccess();
            return;
        }

        TraverseCurrentRange();
    }

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    const TChunkManagerConfigPtr Config_;
    TChunkList* const ChunkList_;
    const TCtxFetchPtr Context_;
    const bool FetchParityReplicas_;

    std::vector<TReadRange> Ranges_;
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
            Ranges_[CurrentRangeIndex_].LowerLimit(),
            Ranges_[CurrentRangeIndex_].UpperLimit());
    }

    void ReplySuccess()
    {
        YCHECK(!Finished_);
        Finished_ = true;

        try {
            // Update upper limits for all returned journal chunks.
            auto* chunkSpecs = Context_->Response().mutable_chunks();
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

            Context_->SetResponseInfo("ChunkCount: %v", chunkSpecs->size());
            Context_->Reply();
        } catch (const std::exception& ex) {
            Context_->Reply(ex);
        }
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
        const TReadLimit& lowerLimit,
        const TReadLimit& upperLimit) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (Context_->Response().chunks_size() >= Config_->MaxChunksPerFetch) {
            ReplyError(TError("Attempt to fetch too many chunks in a single request")
                << TErrorAttribute("limit", Config_->MaxChunksPerFetch));
            return false;
        }

        const auto& config = Bootstrap_->GetConfig()->ChunkManager;

        if (!chunk->IsConfirmed()) {
            ReplyError(TError("Cannot fetch an object containing an unconfirmed chunk %v",
                chunk->GetId()));
            return false;
        }

        auto* chunkSpec = Context_->Response().add_chunks();

        chunkSpec->set_table_row_index(rowIndex);

        SmallVector<TNodePtrWithIndexes, TypicalReplicaCount> replicas;

        auto addJournalReplica = [&] (TNodePtrWithIndexes replica) {
            // For journal chunks, replica indexes are used to track states.
            // Hence we must replace index with #GenericChunkReplicaIndex.
            replicas.push_back(TNodePtrWithIndexes(replica.GetPtr(), GenericChunkReplicaIndex, replica.GetMediumIndex()));
            return true;
        };

        auto erasureCodecId = chunk->GetErasureCodec();
        int firstInfeasibleReplicaIndex = (erasureCodecId == NErasure::ECodec::None || FetchParityReplicas_)
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
            default:                          Y_UNREACHABLE();
        }

        for (auto replica : chunk->StoredReplicas()) {
            addReplica(replica);
        }

        int cachedReplicaCount = 0;
        for (auto replica : chunk->CachedReplicas()) {
            if (cachedReplicaCount >= config->MaxCachedReplicasPerFetch) {
                break;
            }
            if (addReplica(replica)) {
                ++cachedReplicaCount;
            }
        }

        for (auto replica : replicas) {
            NodeDirectoryBuilder_.Add(replica);
            chunkSpec->add_replicas(NYT::ToProto<ui32>(replica));
        }

        ToProto(chunkSpec->mutable_chunk_id(), chunk->GetId());
        chunkSpec->set_erasure_codec(static_cast<int>(erasureCodecId));

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
        if (CurrentRangeIndex_ == Ranges_.size()) {
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
    DISPATCH_YPATH_HEAVY_SERVICE_METHOD(Fetch);
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

    descriptors->push_back(TAttributeDescriptor("chunk_list_id")
        .SetExternal(isExternal)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor("chunk_ids")
        .SetExternal(isExternal)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor("compression_statistics")
        .SetExternal(isExternal)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor("erasure_statistics")
        .SetExternal(isExternal)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor("multicell_statistics")
        .SetExternal(isExternal)
        .SetOpaque(true));
    descriptors->push_back("chunk_count");
    descriptors->push_back("uncompressed_data_size");
    descriptors->push_back("compressed_data_size");
    descriptors->push_back(TAttributeDescriptor("data_weight")
        .SetPresent(node->HasDataWeight()));
    descriptors->push_back("compression_ratio");
    descriptors->push_back("update_mode");
    descriptors->push_back(TAttributeDescriptor("replication_factor")
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor("vital")
        .SetWritable(true)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor("media")
        .SetWritable(true)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor("primary_medium")
        .SetWritable(true)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor("compression_codec")
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor("erasure_codec")
        .SetWritable(true));
}

bool TChunkOwnerNodeProxy::GetBuiltinAttribute(
    const TString& key,
    IYsonConsumer* consumer)
{
    auto* node = GetThisImpl<TChunkOwnerBase>();
    const auto* chunkList = node->GetChunkList();
    auto statistics = node->ComputeTotalStatistics();
    auto isExternal = node->IsExternal();

    if (!isExternal) {
        if (key == "chunk_list_id") {
            BuildYsonFluently(consumer)
                .Value(chunkList->GetId());
            return true;
        }
    }

    if (key == "chunk_count") {
        BuildYsonFluently(consumer)
            .Value(statistics.chunk_count());
        return true;
    }

    if (key == "uncompressed_data_size") {
        BuildYsonFluently(consumer)
            .Value(statistics.uncompressed_data_size());
        return true;
    }

    if (key == "compressed_data_size") {
        BuildYsonFluently(consumer)
            .Value(statistics.compressed_data_size());
        return true;
    }

    if (key == "data_weight" && node->HasDataWeight()) {
        BuildYsonFluently(consumer)
            .Value(statistics.data_weight());
        return true;
    }

    if (key == "compression_ratio") {
        double ratio = statistics.uncompressed_data_size() > 0
            ? static_cast<double>(statistics.compressed_data_size()) / statistics.uncompressed_data_size()
            : 0;
        BuildYsonFluently(consumer)
            .Value(ratio);
        return true;
    }

    if (key == "update_mode") {
        BuildYsonFluently(consumer)
            .Value(FormatEnum(node->GetUpdateMode()));
        return true;
    }

    if (key == "media") {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& replication = node->Replication();
        BuildYsonFluently(consumer)
            .Value(TSerializableChunkReplication(replication, chunkManager));
        return true;
    }

    if (key == "replication_factor") {
        const auto& replication = node->Replication();
        auto primaryMediumIndex = node->GetPrimaryMediumIndex();
        BuildYsonFluently(consumer)
            .Value(replication[primaryMediumIndex].GetReplicationFactor());
        return true;
    }

    if (key == "vital") {
        BuildYsonFluently(consumer)
            .Value(node->Replication().GetVital());
        return true;
    }

    if (key == "primary_medium") {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto primaryMediumIndex = node->GetPrimaryMediumIndex();
        auto* medium = chunkManager->GetMediumByIndex(primaryMediumIndex);

        BuildYsonFluently(consumer)
            .Value(medium->GetName());
        return true;
    }

    if (key == "compression_codec") {
        BuildYsonFluently(consumer)
            .Value(node->GetCompressionCodec());
        return true;
    }

    if (key == "erasure_codec") {
        BuildYsonFluently(consumer)
            .Value(node->GetErasureCodec());
        return true;
    }

    return TNontemplateCypressNodeProxyBase::GetBuiltinAttribute(key, consumer);
}

TFuture<TYsonString> TChunkOwnerNodeProxy::GetBuiltinAttributeAsync(const TString& key)
{
    auto* node = GetThisImpl<TChunkOwnerBase>();
    auto* chunkList = node->GetChunkList();
    auto isExternal = node->IsExternal();

    if (!isExternal) {
        if (key == "chunk_ids") {
            auto visitor = New<TChunkIdsAttributeVisitor>(
                Bootstrap_,
                chunkList);
            return visitor->Run();
        }

        if (key == "compression_statistics") {
            return ComputeChunkStatistics(
                Bootstrap_,
                chunkList,
                [] (const TChunk* chunk) { return NCompression::ECodec(chunk->MiscExt().compression_codec()); });
        }

        if (key == "erasure_statistics") {
            return ComputeChunkStatistics(
                Bootstrap_,
                chunkList,
                [] (const TChunk* chunk) { return chunk->GetErasureCodec(); });
        }

        if (key == "multicell_statistics") {
            return ComputeChunkStatistics(
                Bootstrap_,
                chunkList,
                [] (const TChunk* chunk) { return CellTagFromId(chunk->GetId()); });
        }
    }

    return TNontemplateCypressNodeProxyBase::GetBuiltinAttributeAsync(key);
}

bool TChunkOwnerNodeProxy::SetBuiltinAttribute(
    const TString& key,
    const TYsonString& value)
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();

    auto* node = GetThisImpl<TChunkOwnerBase>();

    if (key == "replication_factor") {
        ValidateStorageParametersUpdate();
        int replicationFactor = ConvertTo<int>(value);
        SetReplicationFactor(replicationFactor);
        return true;
    }

    if (key == "vital") {
        ValidateStorageParametersUpdate();
        bool vital = ConvertTo<bool>(value);
        SetVital(vital);
        return true;
    }

    if (key == "primary_medium") {
        ValidateStorageParametersUpdate();
        auto mediumName = ConvertTo<TString>(value);
        auto* medium = chunkManager->GetMediumByNameOrThrow(mediumName);
        SetPrimaryMedium(medium);
        return true;
    }

    if (key == "media") {
        ValidateStorageParametersUpdate();
        auto serializableReplication = ConvertTo<TSerializableChunkReplication>(value);
        auto replication = node->Replication(); // Copying for modification.
        // Preserves vitality.
        serializableReplication.ToChunkReplication(&replication, chunkManager);
        SetReplication(replication);
        return true;
    }

    if (key == "compression_codec") {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

        auto* node = LockThisImpl<TChunkOwnerBase>(TLockRequest::MakeSharedAttribute(key));
        node->SetCompressionCodec(ConvertTo<NCompression::ECodec>(value));

        return true;
    }

    if (key == "erasure_codec") {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

        auto* node = LockThisImpl<TChunkOwnerBase>(TLockRequest::MakeSharedAttribute(key));
        node->SetErasureCodec(ConvertTo<NErasure::ECodec>(value));

        return true;
    }

    if (key == "account") {
        if (!TNontemplateCypressNodeProxyBase::SetBuiltinAttribute(key, value)) {
            return false;
        }

        auto* node = LockThisImpl<TChunkOwnerBase>(TLockRequest::MakeSharedAttribute(key));
        if (!node->IsExternal()) {
            chunkManager->ScheduleChunkRequisitionUpdate(node->GetChunkList());
        }
        return true;
    }

    return TNontemplateCypressNodeProxyBase::SetBuiltinAttribute(key, value);
}

void TChunkOwnerNodeProxy::SetReplicationFactor(int replicationFactor)
{
    auto* node = GetThisImpl<TChunkOwnerBase>();
    YCHECK(node->IsTrunk());

    auto mediumIndex = node->GetPrimaryMediumIndex();
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto* medium = chunkManager->GetMediumByIndex(mediumIndex);

    auto replication = node->Replication();
    if (replication[mediumIndex].GetReplicationFactor() == replicationFactor) {
        return;
    }

    ValidateReplicationFactor(replicationFactor);
    if (replicationFactor != 0) {
        ValidatePermission(medium, EPermission::Use);
    }

    replication[mediumIndex].SetReplicationFactor(replicationFactor);
    ValidateChunkReplication(chunkManager, replication, mediumIndex);

    node->Replication() = replication;

    if (!node->IsExternal()) {
        chunkManager->ScheduleChunkRequisitionUpdate(node->GetChunkList());
    }
}

void TChunkOwnerNodeProxy::SetVital(bool vital)
{
    auto* node = GetThisImpl<TChunkOwnerBase>();
    YCHECK(node->IsTrunk());

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

    YCHECK(node->IsTrunk());

    auto primaryMediumIndex = node->GetPrimaryMediumIndex();
    ValidateMediaChange(node->Replication(), primaryMediumIndex, replication);

    node->Replication() = replication;

    if (!node->IsExternal()) {
        chunkManager->ScheduleChunkRequisitionUpdate(node->GetChunkList());
    }

    const auto* primaryMedium = chunkManager->GetMediumByIndex(primaryMediumIndex);

    LOG_DEBUG_UNLESS(
        IsRecovery(),
        "Chunk owner replication changed (NodeId: %v, PrimaryMedium: %v, Replication: %v)",
        node->GetId(),
        primaryMedium->GetName(),
        node->Replication());
 }

void TChunkOwnerNodeProxy::SetPrimaryMedium(TMedium* medium)
{
    auto* node = GetThisImpl<TChunkOwnerBase>();
    YCHECK(node->IsTrunk());

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

    LOG_DEBUG_UNLESS(
        IsRecovery(),
        "Chunk owner primary medium changed (NodeId: %v, PrimaryMedium: %v)",
        node->GetId(),
        medium->GetName());
}

void TChunkOwnerNodeProxy::ValidateFetchParameters(const std::vector<TReadRange>& /*ranges*/)
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

void TChunkOwnerNodeProxy::ValidateFetch()
{ }

void TChunkOwnerNodeProxy::ValidateStorageParametersUpdate()
{
    ValidateNoTransaction();
}

DEFINE_YPATH_SERVICE_METHOD(TChunkOwnerNodeProxy, Fetch)
{
    DeclareNonMutating();

    context->SetRequestInfo();

    // NB: No need for a permission check;
    // the client must have invoked GetBasicAttributes.

    ValidateNotExternal();
    ValidateFetch();

    bool fetchParityReplicas = request->fetch_parity_replicas();
    auto addressType = request->has_address_type()
        ? static_cast<EAddressType>(request->address_type())
        : EAddressType::InternalRpc;

    auto ranges = FromProto<std::vector<TReadRange>>(request->ranges());
    ValidateFetchParameters(ranges);

    const auto* node = GetThisImpl<TChunkOwnerBase>();
    auto* chunkList = node->GetChunkList();

    auto visitor = New<TFetchChunkVisitor>(
        Bootstrap_,
        Bootstrap_->GetConfig()->ChunkManager,
        chunkList,
        context,
        fetchParityReplicas,
        addressType,
        ranges);

    visitor->Run();
}

DEFINE_YPATH_SERVICE_METHOD(TChunkOwnerNodeProxy, BeginUpload)
{
    DeclareMutating();

    auto updateMode = EUpdateMode(request->update_mode());
    if (updateMode != EUpdateMode::Append && updateMode != EUpdateMode::Overwrite) {
        THROW_ERROR_EXCEPTION("Invalid update mode for a chunk owner node")
            << TErrorAttribute("update_mode", updateMode);
    }

    auto lockMode = ELockMode(request->lock_mode());
    YCHECK(lockMode == ELockMode::Shared ||
           lockMode == ELockMode::Exclusive);

    auto uploadTransactionTitle = request->has_upload_transaction_title()
        ? MakeNullable(request->upload_transaction_title())
        : Null;

    auto uploadTransactionTimeout = request->has_upload_transaction_timeout()
        ? MakeNullable(FromProto<TDuration>(request->upload_transaction_timeout()))
        : Null;

    auto uploadTransactionIdHint = FromProto<TTransactionId>(request->upload_transaction_id());

    auto uploadTransactionSecondaryCellTags = FromProto<TCellTagList>(request->upload_transaction_secondary_cell_tags());

    auto* node = GetThisImpl<TChunkOwnerBase>();
    auto externalCellTag = node->GetExternalCellTag();

    // Make sure |uploadTransactionSecondaryCellTags| contains the external cell tag,
    // does not contain the primary cell tag, is sorted, and contains no duplicates.
    InsertCellTag(&uploadTransactionSecondaryCellTags, externalCellTag);
    CanonizeCellTags(&uploadTransactionSecondaryCellTags);
    RemoveCellTag(&uploadTransactionSecondaryCellTags, Bootstrap_->GetPrimaryCellTag());

    // Construct |uploadTransactionReplicationCellTags| containing the tags of cells
    // the upload transaction must be replicated to. This list never contains
    // the external cell tag.
    auto uploadTransactionReplicationCellTags = uploadTransactionSecondaryCellTags;
    RemoveCellTag(&uploadTransactionReplicationCellTags, externalCellTag);

    context->SetRequestInfo(
        "UpdateMode: %v, LockMode: %v, "
        "Title: %v, Timeout: %v, SecondaryCellTags: %v",
        updateMode,
        lockMode,
        uploadTransactionTitle,
        uploadTransactionTimeout,
        uploadTransactionSecondaryCellTags);

    // NB: No need for a permission check;
    // the client must have invoked GetBasicAttributes.

    ValidateBeginUpload();

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    const auto& objectManager = Bootstrap_->GetObjectManager();
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    const auto& transactionManager = Bootstrap_->GetTransactionManager();

    auto* uploadTransaction = transactionManager->StartTransaction(
        Transaction,
        uploadTransactionSecondaryCellTags,
        uploadTransactionReplicationCellTags,
        uploadTransactionTimeout,
        uploadTransactionTitle,
        EmptyAttributes(),
        uploadTransactionIdHint);

    auto* lockedNode = cypressManager
        ->LockNode(TrunkNode, uploadTransaction, lockMode)
        ->As<TChunkOwnerBase>();

    switch (updateMode) {
        case EUpdateMode::Append: {
            if (node->IsExternal() || node->GetType() == EObjectType::Journal) {
                LOG_DEBUG_UNLESS(
                    IsRecovery(),
                    "Node is switched to \"append\" mode (NodeId: %v)",
                    lockedNode->GetId());

            } else {
                auto* snapshotChunkList = lockedNode->GetChunkList();

                auto* newChunkList = chunkManager->CreateChunkList(EChunkListKind::Static);
                newChunkList->AddOwningNode(lockedNode);

                snapshotChunkList->RemoveOwningNode(lockedNode);
                lockedNode->SetChunkList(newChunkList);
                objectManager->RefObject(newChunkList);

                chunkManager->AttachToChunkList(newChunkList, snapshotChunkList);

                auto* deltaChunkList = chunkManager->CreateChunkList(EChunkListKind::Static);
                chunkManager->AttachToChunkList(newChunkList, deltaChunkList);

                objectManager->UnrefObject(snapshotChunkList);

                LOG_DEBUG_UNLESS(
                    IsRecovery(),
                    "Node is switched to \"append\" mode (NodeId: %v, NewChunkListId: %v, SnapshotChunkListId: %v, DeltaChunkListId: %v)",
                    node->GetId(),
                    newChunkList->GetId(),
                    snapshotChunkList->GetId(),
                    deltaChunkList->GetId());

            }
            break;
        }

        case EUpdateMode::Overwrite: {
            if (node->IsExternal() || node->GetType() == EObjectType::Journal) {
                LOG_DEBUG_UNLESS(
                    IsRecovery(),
                    "Node is switched to \"overwrite\" mode (NodeId: %v)",
                    node->GetId());
            } else {
                auto* oldChunkList = lockedNode->GetChunkList();
                oldChunkList->RemoveOwningNode(lockedNode);
                objectManager->UnrefObject(oldChunkList);

                auto* newChunkList = chunkManager->CreateChunkList(EChunkListKind::Static);
                newChunkList->AddOwningNode(lockedNode);
                lockedNode->SetChunkList(newChunkList);
                objectManager->RefObject(newChunkList);

                LOG_DEBUG_UNLESS(
                    IsRecovery(),
                    "Node is switched to \"overwrite\" mode (NodeId: %v, NewChunkListId: %v)",
                    node->GetId(),
                    newChunkList->GetId());
            }
            break;
        }

        default:
            Y_UNREACHABLE();
    }

    lockedNode->BeginUpload(updateMode);

    const auto& uploadTransactionId = uploadTransaction->GetId();
    ToProto(response->mutable_upload_transaction_id(), uploadTransactionId);

    response->set_cell_tag(externalCellTag == NotReplicatedCellTag ? Bootstrap_->GetPrimaryCellTag() : externalCellTag);

    const auto& multicellManager = Bootstrap_->GetMulticellManager();

    if (node->IsExternal()) {
        auto replicationRequest = TChunkOwnerYPathProxy::BeginUpload(FromObjectId(GetId()));
        replicationRequest->set_update_mode(static_cast<int>(updateMode));
        replicationRequest->set_lock_mode(static_cast<int>(lockMode));
        ToProto(replicationRequest->mutable_upload_transaction_id(), uploadTransactionId);
        if (uploadTransactionTitle) {
            replicationRequest->set_upload_transaction_title(*uploadTransactionTitle);
        }
        // NB: upload_transaction_timeout must be null
        // NB: upload_transaction_secondary_cell_tags must be empty
        SetTransactionId(replicationRequest, GetObjectId(GetTransaction()));

        multicellManager->PostToMaster(replicationRequest, externalCellTag);
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
    auto* snapshotChunkList = node->GetSnapshotChunkList();
    auto* deltaChunkList = node->GetDeltaChunkList();

    const auto& uploadChunkListId = deltaChunkList->GetId();
    ToProto(response->mutable_chunk_list_id(), uploadChunkListId);

    if (fetchLastKey) {
        TOwningKey lastKey;
        if (!IsEmpty(snapshotChunkList)) {
            lastKey = GetMaxKey(snapshotChunkList);
        }
        ToProto(response->mutable_last_key(), lastKey);
    }

    TNullable<TMD5Hasher> md5Hasher;
    node->GetUploadParams(&md5Hasher);
    ToProto(response->mutable_md5_hasher(), md5Hasher);

    context->SetResponseInfo("UploadChunkListId: %v, HasLastKey: %v",
        uploadChunkListId,
        response->has_last_key());
    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TChunkOwnerNodeProxy, EndUpload)
{
    DeclareMutating();
    ValidateTransaction();
    ValidateInUpdate();

    auto schema = request->has_table_schema()
        ? FromProto<TTableSchema>(request->table_schema())
        : TTableSchema();
    auto schemaMode = ETableSchemaMode(request->schema_mode());
    const auto* statistics = request->has_statistics() ? &request->statistics() : nullptr;

    auto* node = GetThisImpl<TChunkOwnerBase>();
    YCHECK(node->GetTransaction() == Transaction);

    TNullable<EOptimizeFor> optimizeFor;
    if (request->has_optimize_for()) {
        optimizeFor = EOptimizeFor(request->optimize_for());
    }

    if (request->has_compression_codec()) {
        node->SetCompressionCodec(NCompression::ECodec(request->compression_codec()));
    }

    if (request->has_erasure_codec()) {
        node->SetErasureCodec(NErasure::ECodec(request->erasure_codec()));
    }

    if (node->IsExternal()) {
        PostToMaster(context, node->GetExternalCellTag());
    }

    if (request->has_md5_hasher()) {
        YCHECK(node->GetType() == EObjectType::File);
    }

    TNullable<TMD5Hasher> md5Hasher;
    if (request->has_md5_hasher()) {
        FromProto(&md5Hasher, request->md5_hasher());
    }

    node->EndUpload(statistics, schema, schemaMode, optimizeFor, md5Hasher);

    SetModified();

    if (Bootstrap_->IsPrimaryMaster()) {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->CommitTransaction(Transaction, NullTimestamp);
    }

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
