#include "chunk.h"

#include "chunk_list.h"
#include "chunk_location.h"
#include "chunk_tree_statistics.h"
#include "helpers.h"
#include "domestic_medium.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/serialize.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>

#include <yt/yt/server/master/node_tracker_server/node.h>

#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/journal_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NChunkServer {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NJournalClient;
using namespace NObjectServer;
using namespace NObjectClient;
using namespace NSecurityServer;
using namespace NCellMaster;
using namespace NJournalClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

const TChunk::TEmptyChunkReplicasData TChunk::EmptyChunkReplicasData = {};

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

// COMPAT(shakurov)
using TLegacyChunkExportDataList32 = std::array<TChunkExportData, 32>;

} // namespace NYT::NChunkServer

// COMPAT(shakurov)
Y_DECLARE_PODTYPE(NYT::NChunkServer::TLegacyChunkExportDataList32);

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

void TChunkExportData::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, RefCounter);
    Persist(context, ChunkRequisitionIndex);
}

////////////////////////////////////////////////////////////////////////////////

TChunk::TChunk(TChunkId id)
    : TChunkTree(id)
    , ChunkMeta_(TImmutableChunkMeta::CreateNull())
    , ShardIndex_(GetChunkShardIndex(id))
    , AggregatedRequisitionIndex_(IsErasure()
        ? MigrationErasureChunkRequisitionIndex
        : MigrationChunkRequisitionIndex)
    , LocalRequisitionIndex_(AggregatedRequisitionIndex_)
{ }

TChunkTreeStatistics TChunk::GetStatistics() const
{
    TChunkTreeStatistics result;
    if (IsSealed()) {
        result.RowCount = GetRowCount();
        result.LogicalRowCount = GetRowCount();
        result.UncompressedDataSize = GetUncompressedDataSize();
        result.CompressedDataSize = GetCompressedDataSize();
        result.DataWeight = GetDataWeight();
        result.LogicalDataWeight = GetDataWeight();
        if (IsErasure()) {
            result.ErasureDiskSpace = GetDiskSpace();
        } else {
            result.RegularDiskSpace = GetDiskSpace();
        }
        result.ChunkCount = 1;
        result.LogicalChunkCount = 1;
        result.Rank = 0;
    }
    return result;
}

i64 TChunk::GetPartDiskSpace() const
{
    auto result = GetDiskSpace();
    auto codecId = GetErasureCodec();
    if (codecId != NErasure::ECodec::None) {
        auto* codec = NErasure::GetCodec(codecId);
        result /= codec->GetTotalPartCount();
    }

    return result;
}

TString TChunk::GetLowercaseObjectName() const
{
    switch (GetType()) {
        case EObjectType::Chunk:
            return Format("chunk %v", GetId());
        case EObjectType::ErasureChunk:
            return Format("erasure chunk %v", GetId());
        case EObjectType::JournalChunk:
            return Format("journal chunk %v", GetId());
        case EObjectType::ErasureJournalChunk:
            return Format("erasure journal chunk %v", GetId());
        default:
            YT_ABORT();
    }
}

TString TChunk::GetCapitalizedObjectName() const
{
    switch (GetType()) {
        case EObjectType::Chunk:
            return Format("Chunk %v", GetId());
        case EObjectType::ErasureChunk:
            return Format("Erasure chunk %v", GetId());
        case EObjectType::JournalChunk:
            return Format("Journal chunk %v", GetId());
        case EObjectType::ErasureJournalChunk:
            return Format("Erasure journal chunk %v", GetId());
        default:
            YT_ABORT();
    }
}

TString TChunk::GetObjectPath() const
{
    return Format("//sys/chunks/%v", GetId());
}

void TChunk::Save(NCellMaster::TSaveContext& context) const
{
    TChunkTree::Save(context);

    using NYT::Save;
    Save(context, ChunkMeta_);
    Save(context, AggregatedRequisitionIndex_);
    Save(context, LocalRequisitionIndex_);
    Save(context, GetReadQuorum());
    Save(context, GetWriteQuorum());
    Save(context, LogReplicaLagLimit_);
    Save(context, GetDiskSpace());
    Save(context, GetErasureCodec());
    Save(context, GetMovable());
    Save(context, GetOverlayed());
    Save(context, GetStripedErasure());
    Save(context, GetSealable());
    Save(context, GetHistoricallyNonVital());
    {
        // COMPAT(shakurov)
        TCompactVector<TChunkTree*, TypicalChunkParentCount> parents;
        for (auto [chunkTree, refCount] : Parents_) {
            for (auto i = 0; i < refCount; ++i) {
                parents.push_back(chunkTree);
            }
        }
        std::sort(parents.begin(), parents.end(), TObjectIdComparer());
        Save(context, parents);
    }
    if (ReplicasData_) {
        Save(context, true);
        Save(context, *ReplicasData_);
    } else {
        Save(context, false);
    }

    TUniquePtrSerializer<>::Save(context, CellIndexToExportData_);

    Save(context, EndorsementRequired_);
    Save(context, ConsistentReplicaPlacementHash_);
}

void TChunk::Load(NCellMaster::TLoadContext& context)
{
    TChunkTree::Load(context);

    using NYT::Load;

    Load(context, ChunkMeta_);

    Load(context, AggregatedRequisitionIndex_);
    Load(context, LocalRequisitionIndex_);

    SetReadQuorum(Load<i8>(context));
    SetWriteQuorum(Load<i8>(context));

    Load(context, LogReplicaLagLimit_);

    SetDiskSpace(Load<i64>(context));

    SetErasureCodec(Load<NErasure::ECodec>(context));
    SetMovable(Load<bool>(context));
    SetOverlayed(Load<bool>(context));
    SetStripedErasure(Load<bool>(context));
    SetSealable(Load<bool>(context));

    // COMPAT(gritukan)
    if (context.GetVersion() >= EMasterReign::HistoricallyNonVital) {
        SetHistoricallyNonVital(Load<bool>(context));
    } else {
        SetHistoricallyNonVital(false);
    }

    auto parents = Load<TCompactVector<TChunkTree*, TypicalChunkParentCount>>(context);
    for (auto* parent : parents) {
        ++Parents_[parent];
    }

    // COMPAT(shakurov)
    if (context.GetVersion() < EMasterReign::DropChunkExpirationTracker) {
        // Used to be expiration time.
        Load<TInstant>(context);
    }

    if (Load<bool>(context)) {
        MutableReplicasData()->Load(context);
    }

    // COMPAT(shakurov)
    if (context.GetVersion() < EMasterReign::SimplerChunkExportDataSaveLoad) {
        auto exportCounter = Load<ui8>(context);
        if (exportCounter > 0) {
            CellIndexToExportData_ = std::make_unique<TCellIndexToChunkExportData>();

            // 255 is a special marker that was never in trunk. It was only used
            // in the 22.4 branch to facilitate migration from fixed-length
            // array to variable-size map - while preserving snapshot
            // compatibility and avoiding reign promotion.
            if (exportCounter == 255) {
                Load(context, *CellIndexToExportData_);
                YT_VERIFY(std::any_of(
                    CellIndexToExportData_->begin(), CellIndexToExportData_->end(),
                    [] (auto pair) { return pair.second.RefCounter != 0; }));
            } else {
                TLegacyChunkExportDataList32 legacyExportDataList{};
                TPodSerializer::Load(context, legacyExportDataList);
                YT_VERIFY(std::any_of(
                        legacyExportDataList.begin(), legacyExportDataList.end(),
                        [] (auto data) { return data.RefCounter != 0; }));
                for (auto cellIndex = 0; cellIndex < 32; ++cellIndex) {
                    auto data = legacyExportDataList[cellIndex];
                    if (data.RefCounter != 0) {
                        CellIndexToExportData_->emplace(cellIndex, data);
                    }
                }
            }
        } // Else leave CellIndexToExportData_ null.
    } else {
        TUniquePtrSerializer<>::Load(context, CellIndexToExportData_);
    }

    YT_VERIFY(!CellIndexToExportData_ ||
        std::any_of(
            CellIndexToExportData_->begin(),
            CellIndexToExportData_->end(),
            [] (auto pair) {
                return pair.second.RefCounter != 0;
            }));

    Load(context, EndorsementRequired_);

    Load(context, ConsistentReplicaPlacementHash_);

    if (IsConfirmed()) {
        auto miscExt = ChunkMeta_->GetExtension<TMiscExt>();
        OnMiscExtUpdated(miscExt);
    }
}

void TChunk::AddParent(TChunkTree* parent)
{
    ++Parents_[parent];
}

void TChunk::RemoveParent(TChunkTree* parent)
{
    auto it = Parents_.find(parent);
    YT_VERIFY(it != Parents_.end());
    if (--it->second == 0) {
        Parents_.erase(it);
    }
}

int TChunk::GetParentCount() const
{
    auto result = 0;
    for (auto [parent, cardinality] : Parents_) {
        result += cardinality;
    }
    return result;
}

bool TChunk::HasParents() const
{
    return !Parents_.empty();
}

void TChunk::AddReplica(
    TChunkLocationPtrWithReplicaInfo replica,
    const TDomesticMedium* medium,
    bool approved)
{
    auto* data = MutableReplicasData();
    if (IsJournal()) {
        for (auto& existingReplica : data->MutableStoredReplicas()) {
            if (existingReplica.ToGenericState() == replica.ToGenericState()) {
                existingReplica = replica;
                return;
            }
        }
    }

    if (approved) {
        ++data->ApprovedReplicaCount;
    }

    data->AddStoredReplica(replica);
    if (!medium->GetTransient()) {
        auto lastSeenReplicas = data->MutableLastSeenReplicas();
        auto nodeId = GetChunkLocationNodeId(replica);
        if (IsErasure()) {
            lastSeenReplicas[replica.GetReplicaIndex()] = nodeId;
        } else {
            lastSeenReplicas[data->CurrentLastSeenReplicaIndex] = nodeId;
            data->CurrentLastSeenReplicaIndex = (data->CurrentLastSeenReplicaIndex + 1) % lastSeenReplicas.size();
        }
    }
}

void TChunk::RemoveReplica(
    TChunkLocationPtrWithReplicaIndex replica,
    bool approved)
{
    auto* data = MutableReplicasData();
    if (approved) {
        --data->ApprovedReplicaCount;
        YT_ASSERT(data->ApprovedReplicaCount >= 0);
    }

    auto storedReplicas = data->GetStoredReplicas();
    for (int replicaIndex = 0; replicaIndex < std::ssize(storedReplicas); ++replicaIndex) {
        auto existingReplica = storedReplicas[replicaIndex];
        if (existingReplica.GetPtr() == replica.GetPtr() && existingReplica.GetReplicaIndex() == replica.GetReplicaIndex()) {
            data->RemoveStoredReplica(replicaIndex);
            return;
        }
    }
    YT_ABORT();
}

void TChunk::ApproveReplica(TChunkLocationPtrWithReplicaInfo replica)
{
    auto* data = MutableReplicasData();
    ++data->ApprovedReplicaCount;

    if (IsJournal()) {
        auto genericReplica = replica.ToGenericState();
        for (auto& existingReplica : data->MutableStoredReplicas()) {
            if (existingReplica.ToGenericState() == genericReplica) {
                existingReplica = replica;
                return;
            }
        }
        YT_ABORT();
    }
}

int TChunk::GetApprovedReplicaCount() const
{
    return ReplicasData().ApprovedReplicaCount;
}

void TChunk::SetApprovedReplicaCount(int count)
{
    MutableReplicasData()->ApprovedReplicaCount = count;
}

void TChunk::Confirm(
    const TChunkInfo& chunkInfo,
    const TChunkMeta& chunkMeta)
{
    // YT-3251
    if (!HasProtoExtension<TMiscExt>(chunkMeta.extensions())) {
        THROW_ERROR_EXCEPTION("Missing TMiscExt in chunk meta");
    }

    Y_UNUSED(CheckedEnumCast<EChunkType>(chunkMeta.type()));
    Y_UNUSED(CheckedEnumCast<EChunkFormat>(chunkMeta.format()));

    ChunkMeta_ = FromProto<TImmutableChunkMetaPtr>(chunkMeta);

    SetDiskSpace(chunkInfo.disk_space());

    auto miscExt = ChunkMeta_->GetExtension<TMiscExt>();
    OnMiscExtUpdated(miscExt);

    YT_VERIFY(IsConfirmed());
}

bool TChunk::GetMovable() const
{
    return Flags_.Movable;
}

void TChunk::SetMovable(bool value)
{
    Flags_.Movable = value;
}

bool TChunk::GetOverlayed() const
{
    return Flags_.Overlayed;
}

void TChunk::SetOverlayed(bool value)
{
    Flags_.Overlayed = value;
}

void TChunk::SetRowCount(i64 rowCount)
{
    YT_VERIFY(IsJournalChunkType(GetType()));

    auto miscExt = ChunkMeta_->GetExtension<TMiscExt>();
    miscExt.set_row_count(rowCount);

    NChunkClient::NProto::TChunkMeta protoMeta;
    ToProto(&protoMeta, ChunkMeta_);
    SetProtoExtension(protoMeta.mutable_extensions(), miscExt);
    ChunkMeta_ = FromProto<TImmutableChunkMetaPtr>(protoMeta);

    OnMiscExtUpdated(miscExt);
}

bool TChunk::IsConfirmed() const
{
    return ChunkMeta_->GetType() != EChunkType::Unknown;
}

bool TChunk::IsAvailable() const
{
    if (!ReplicasData_) {
        // Actually it makes no sense calling IsAvailable for foreign chunks.
        return false;
    }

    const auto& storedReplicas = ReplicasData_->GetStoredReplicas();
    switch (GetType()) {
        case EObjectType::Chunk:
            return !storedReplicas.empty();

        case EObjectType::ErasureChunk:
        case EObjectType::ErasureJournalChunk: {
            auto* codec = NErasure::GetCodec(GetErasureCodec());
            int dataPartCount = codec->GetDataPartCount();
            NErasure::TPartIndexSet missingIndexSet((1 << dataPartCount) - 1);
            for (auto replica : storedReplicas) {
                missingIndexSet.reset(replica.GetReplicaIndex());
            }
            return missingIndexSet.none();
        }

        case EObjectType::JournalChunk: {
            if (std::ssize(storedReplicas) >= GetReadQuorum()) {
                return true;
            }
            for (auto replica : storedReplicas) {
                if (replica.GetReplicaState() == EChunkReplicaState::Sealed) {
                    return true;
                }
            }
            return false;
        }

        default:
            YT_ABORT();
    }
}

bool TChunk::IsSealed() const
{
    if (!IsConfirmed()) {
        return false;
    }

    if (!IsJournal()) {
        return true;
    }

    return Flags_.Sealed;
}

void TChunk::SetSealed(bool value)
{
    Flags_.Sealed = value;
}

bool TChunk::GetStripedErasure() const
{
    return Flags_.StripedErasure;
}

void TChunk::SetStripedErasure(bool value)
{
    Flags_.StripedErasure = value;
}

bool TChunk::GetSealable() const
{
    return Flags_.Sealable;
}

void TChunk::SetSealable(bool value)
{
    Flags_.Sealable = value;
}

bool TChunk::GetHistoricallyNonVital() const
{
    return Flags_.HistoricallyNonVital;
}

void TChunk::SetHistoricallyNonVital(bool value)
{
    YT_ASSERT(!value || !IsErasure());

    Flags_.HistoricallyNonVital = value;
}

i64 TChunk::GetPhysicalSealedRowCount() const
{
    YT_VERIFY(Flags_.Sealed);
    return GetPhysicalChunkRowCount(GetRowCount(), GetOverlayed());
}

void TChunk::Seal(const TChunkSealInfo& info)
{
    YT_VERIFY(IsConfirmed() && !IsSealed());
    YT_VERIFY(!Flags_.Sealed);
    YT_VERIFY(GetRowCount() == 0);
    YT_VERIFY(GetUncompressedDataSize() == 0);
    YT_VERIFY(GetCompressedDataSize() == 0);
    YT_VERIFY(GetDiskSpace() == 0);

    auto miscExt = ChunkMeta_->GetExtension<TMiscExt>();
    miscExt.set_sealed(true);
    if (info.has_first_overlayed_row_index()) {
        miscExt.set_first_overlayed_row_index(info.first_overlayed_row_index());
    }
    miscExt.set_row_count(info.row_count());
    miscExt.set_uncompressed_data_size(info.uncompressed_data_size());
    miscExt.set_compressed_data_size(info.compressed_data_size());

    NChunkClient::NProto::TChunkMeta protoMeta;
    ToProto(&protoMeta, ChunkMeta_);
    SetProtoExtension(protoMeta.mutable_extensions(), miscExt);
    ChunkMeta_ = FromProto<TImmutableChunkMetaPtr>(protoMeta);

    OnMiscExtUpdated(miscExt);

    SetDiskSpace(info.uncompressed_data_size()); // an approximation
}

int TChunk::GetPhysicalReplicationFactor(int mediumIndex, const TChunkRequisitionRegistry* registry) const
{
    auto mediumReplicationPolicy = GetAggregatedReplication(registry).Get(mediumIndex);
    if (!mediumReplicationPolicy) {
        return 0;
    }

    if (IsErasure()) {
        auto* codec = NErasure::GetCodec(GetErasureCodec());
        return mediumReplicationPolicy.GetDataPartsOnly()
            ? codec->GetDataPartCount()
            : codec->GetTotalPartCount();
    } else {
        return mediumReplicationPolicy.GetReplicationFactor();
    }
}

int TChunk::GetMaxReplicasPerFailureDomain(
    int mediumIndex,
    std::optional<int> replicationFactorOverride,
    const TChunkRequisitionRegistry* registry) const
{
    switch (GetType()) {
        case EObjectType::Chunk: {
            if (replicationFactorOverride) {
                return *replicationFactorOverride;
            }
            auto replicationFactor = GetAggregatedReplicationFactor(mediumIndex, registry);
            return std::max(replicationFactor - 1, 1);
        }

        case EObjectType::ErasureChunk: {
            auto* codec = NErasure::GetCodec(GetErasureCodec());
            return codec->GetGuaranteedRepairablePartCount();
        }

        case EObjectType::JournalChunk:
        case EObjectType::ErasureJournalChunk: {
            YT_ASSERT(!replicationFactorOverride);
            auto replicaCount = GetPhysicalReplicationFactor(mediumIndex, registry);
            // #ReadQuorum replicas are required to read journal chunk, so no more
            // than #replicaCount - #ReadQuorum replicas can be placed in the same rack.
            return std::max(replicaCount - ReadQuorum_, 1);
        }

        default:
            YT_ABORT();
    }
}

TChunkExportData TChunk::GetExportData(int cellIndex) const
{
    if (!IsExported()) {
        return {};
    }

    auto it = CellIndexToExportData_->find(cellIndex);
    return it == CellIndexToExportData_->end()
        ? TChunkExportData{}
        : it->second;
}

bool TChunk::IsExportedToCell(int cellIndex) const
{
    if (!IsExported()) {
        return false;
    }

    auto it = CellIndexToExportData_->find(cellIndex);
    if (it == CellIndexToExportData_->end()) {
        return false;
    }

    if (it->second.RefCounter == 0) {
        YT_LOG_ALERT("Chunk export data has zero reference counter "
            "(ChunkId: %v, CellIndex: %v)",
            GetId(),
            cellIndex);
    }

    return true;
}

void TChunk::RefUsedRequisitions(TChunkRequisitionRegistry* registry) const
{
    registry->Ref(AggregatedRequisitionIndex_);
    registry->Ref(LocalRequisitionIndex_);

    if (!IsExported()) {
        return;
    }

    for (auto [cellIndex, data] : *CellIndexToExportData_) {
        if (data.RefCounter != 0) {
            registry->Ref(data.ChunkRequisitionIndex);
        } else {
            YT_LOG_ALERT("Chunk export data has zero reference counter "
                "(ChunkId: %v, CellIndex: %v)",
                GetId(),
                cellIndex);
        }
    }
}

void TChunk::UnrefUsedRequisitions(
    TChunkRequisitionRegistry* registry,
    const NObjectServer::IObjectManagerPtr& objectManager) const
{
    registry->Unref(AggregatedRequisitionIndex_, objectManager);
    registry->Unref(LocalRequisitionIndex_, objectManager);

    if (!IsExported()) {
        return;
    }

    for (auto [cellIndex, data] : *CellIndexToExportData_) {
        if (data.RefCounter != 0) {
            registry->Unref(data.ChunkRequisitionIndex, objectManager);
        } else {
            YT_LOG_ALERT("Chunk export data has zero reference counter "
                "(ChunkId: %v, CellIndex: %v)",
                GetId(),
                cellIndex);
        }
    }
}

inline TChunkRequisition TChunk::ComputeAggregatedRequisition(const TChunkRequisitionRegistry* registry)
{
    auto result = registry->GetRequisition(LocalRequisitionIndex_);

    // Shortcut for non-exported chunk.
    if (!IsExported()) {
        return result;
    }

    for (auto [cellIndex, data] : *CellIndexToExportData_) {
        if (data.RefCounter != 0) {
            result |= registry->GetRequisition(data.ChunkRequisitionIndex);
        } else {
            YT_LOG_ALERT("Chunk export data has zero reference counter "
                "(ChunkId: %v, CellIndex: %v)",
                GetId(),
                cellIndex);
        }
    }

    return result;
}

void TChunk::Export(int cellIndex, TChunkRequisitionRegistry* registry)
{
    if (!CellIndexToExportData_) {
        CellIndexToExportData_ = std::make_unique<TCellIndexToChunkExportData>();
    }

    auto [it, inserted] = CellIndexToExportData_->emplace(cellIndex, TChunkExportData{});
    auto& data = it->second;
    ++data.RefCounter;
    if (inserted) {
        YT_VERIFY(data.RefCounter == 1);
        YT_VERIFY(data.ChunkRequisitionIndex == EmptyChunkRequisitionIndex);
        registry->Ref(data.ChunkRequisitionIndex);
        // NB: an empty requisition doesn't affect the aggregated requisition
        // and thus doesn't call for updating the latter.
    } else {
        YT_VERIFY(data.RefCounter > 1);
    }
}

void TChunk::Unexport(
    int cellIndex,
    int importRefCounter,
    TChunkRequisitionRegistry* registry,
    const NObjectServer::IObjectManagerPtr& objectManager)
{
    YT_VERIFY(IsExported());

    auto it = CellIndexToExportData_->find(cellIndex);
    YT_VERIFY(it != CellIndexToExportData_->end());
    auto& data = it->second;
    if ((data.RefCounter -= importRefCounter) == 0) {
        registry->Unref(data.ChunkRequisitionIndex, objectManager);

        CellIndexToExportData_->erase(it);

        if (CellIndexToExportData_->empty()) {
            CellIndexToExportData_.reset();
        }

        UpdateAggregatedRequisitionIndex(registry, objectManager);
    }
}

i64 TChunk::GetMasterMemoryUsage() const
{
    auto memoryUsage =
        sizeof(TChunk) +
        sizeof(TChunkDynamicData) +
        ChunkMeta_->GetTotalByteSize();
    if (IsErasure()) {
        memoryUsage += sizeof(TErasureChunkReplicasData);
    } else {
        memoryUsage += sizeof(TRegularChunkReplicasData);
    }

    return memoryUsage;
}

EChunkType TChunk::GetChunkType() const
{
    return ChunkMeta_->GetType();
}

EChunkFormat TChunk::GetChunkFormat() const
{
    return ChunkMeta_->GetFormat();
}

bool TChunk::HasConsistentReplicaPlacementHash() const
{
    return
        ConsistentReplicaPlacementHash_ != NullConsistentReplicaPlacementHash &&
        !IsErasure(); // CRP with erasure is not supported.
}

void TChunk::OnMiscExtUpdated(const TMiscExt& miscExt)
{
    RowCount_ = miscExt.row_count();
    CompressedDataSize_ = miscExt.compressed_data_size();
    UncompressedDataSize_ = miscExt.uncompressed_data_size();
    DataWeight_ = miscExt.data_weight();
    auto firstOverlayedRowIndex = miscExt.has_first_overlayed_row_index()
        ? std::make_optional(miscExt.first_overlayed_row_index())
        : std::nullopt;
    SetFirstOverlayedRowIndex(firstOverlayedRowIndex);
    MaxBlockSize_ = miscExt.max_data_block_size();
    CompressionCodec_ = FromProto<NCompression::ECodec>(miscExt.compression_codec());
    SystemBlockCount_ = miscExt.system_block_count();
    SetSealed(miscExt.sealed());
    SetStripedErasure(miscExt.striped_erasure());
}

////////////////////////////////////////////////////////////////////////////////

template <size_t TypicalStoredReplicaCount, size_t LastSeenReplicaCount>
void TChunk::TReplicasData<TypicalStoredReplicaCount, LastSeenReplicaCount>::Initialize()
{
    std::fill(LastSeenReplicas.begin(), LastSeenReplicas.end(), InvalidNodeId);
}

template <size_t TypicalStoredReplicaCount, size_t LastSeenReplicaCount>
TRange<TChunkLocationPtrWithReplicaInfo> TChunk::TReplicasData<TypicalStoredReplicaCount, LastSeenReplicaCount>::GetStoredReplicas() const
{
    return MakeRange(StoredReplicas);
}

template <size_t TypicalStoredReplicaCount, size_t LastSeenReplicaCount>
TMutableRange<TChunkLocationPtrWithReplicaInfo> TChunk::TReplicasData<TypicalStoredReplicaCount, LastSeenReplicaCount>::MutableStoredReplicas()
{
    return MakeMutableRange(StoredReplicas);
}

template <size_t TypicalStoredReplicaCount, size_t LastSeenReplicaCount>
void TChunk::TReplicasData<TypicalStoredReplicaCount, LastSeenReplicaCount>::AddStoredReplica(TChunkLocationPtrWithReplicaInfo replica)
{
    StoredReplicas.push_back(replica);
}

template <size_t TypicalStoredReplicaCount, size_t LastSeenReplicaCount>
void TChunk::TReplicasData<TypicalStoredReplicaCount, LastSeenReplicaCount>::RemoveStoredReplica(int replicaIndex)
{
    std::swap(StoredReplicas[replicaIndex], StoredReplicas.back());
    StoredReplicas.pop_back();
    StoredReplicas.shrink_to_small();
}

template <size_t TypicalStoredReplicaCount, size_t LastSeenReplicaCount>
TRange<TNodeId> TChunk::TReplicasData<TypicalStoredReplicaCount, LastSeenReplicaCount>::GetLastSeenReplicas() const
{
    return MakeRange(LastSeenReplicas);
}

template <size_t TypicalStoredReplicaCount, size_t LastSeenReplicaCount>
TMutableRange<TNodeId> TChunk::TReplicasData<TypicalStoredReplicaCount, LastSeenReplicaCount>::MutableLastSeenReplicas()
{
    return MakeMutableRange(LastSeenReplicas);
}

template <size_t TypicalStoredReplicaCount, size_t LastSeenReplicaCount>
void TChunk::TReplicasData<TypicalStoredReplicaCount, LastSeenReplicaCount>::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, StoredReplicas);

    // COMPAT(kvk1920, gritukan)
    if (context.GetVersion() < EMasterReign::RemoveCacheMedium) {
        auto cachedReplicasPresented = Load<bool>(context);
        if (cachedReplicasPresented) {
            // Cached replicas are simply dropped.
            Load<THashSet<TChunkLocationPtrWithReplicaInfo>>(context);
        }
    }

    Load(context, LastSeenReplicas);
    Load(context, CurrentLastSeenReplicaIndex);
    Load(context, ApprovedReplicaCount);
}

template <size_t TypicalStoredReplicaCount, size_t LastSeenReplicaCount>
void TChunk::TReplicasData<TypicalStoredReplicaCount, LastSeenReplicaCount>::Save(TSaveContext& context) const
{
    using NYT::Save;

    // NB: RemoveReplica calls do not commute and their order is not
    // deterministic (i.e. when unregistering a node we traverse certain hashtables).
    TVectorSerializer<TDefaultSerializer, TSortedTag>::Save(context, StoredReplicas);
    Save(context, LastSeenReplicas);
    Save(context, CurrentLastSeenReplicaIndex);
    Save(context, ApprovedReplicaCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
