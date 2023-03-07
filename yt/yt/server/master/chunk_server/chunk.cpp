#include "chunk.h"
#include "chunk_list.h"
#include "chunk_tree_statistics.h"
#include "medium.h"

#include <yt/server/master/cell_master/serialize.h>
#include <yt/server/master/cell_master/bootstrap.h>

#include <yt/server/master/chunk_server/chunk_manager.h>

#include <yt/server/master/security_server/account.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/client/object_client/helpers.h>

#include <yt/library/erasure/codec.h>

namespace NYT::NChunkServer {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NObjectServer;
using namespace NObjectClient;
using namespace NSecurityServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

const TChunk::TCachedReplicas TChunk::EmptyCachedReplicas;
const TChunk::TReplicasData TChunk::EmptyReplicasData = {};

////////////////////////////////////////////////////////////////////////////////

TChunk::TChunk(TChunkId id)
    : TChunkTree(id)
    , AggregatedRequisitionIndex_(IsErasure()
        ? MigrationErasureChunkRequisitionIndex
        : MigrationChunkRequisitionIndex)
    , LocalRequisitionIndex_(AggregatedRequisitionIndex_)
{
    ChunkMeta_.set_type(static_cast<int>(EChunkType::Unknown));
    ChunkMeta_.set_version(-1);
    ChunkMeta_.mutable_extensions();
}

TChunkTreeStatistics TChunk::GetStatistics() const
{
    TChunkTreeStatistics result;
    if (IsSealed()) {
        result.RowCount = MiscExt_.row_count();
        result.LogicalRowCount = MiscExt_.row_count();
        result.UncompressedDataSize = MiscExt_.uncompressed_data_size();
        result.CompressedDataSize = MiscExt_.compressed_data_size();
        result.DataWeight = MiscExt_.has_data_weight() ? MiscExt_.data_weight() : -1;
        if (IsErasure()) {
            result.ErasureDiskSpace = ChunkInfo_.disk_space();
        } else {
            result.RegularDiskSpace = ChunkInfo_.disk_space();
        }
        result.ChunkCount = 1;
        result.LogicalChunkCount = 1;
        result.Rank = 0;
        result.Sealed = IsSealed();
    } else {
        result.Sealed = false;
    }
    return result;
}

i64 TChunk::GetPartDiskSpace() const
{
    auto result = ChunkInfo_.disk_space();
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

void TChunk::Save(NCellMaster::TSaveContext& context) const
{
    TChunkTree::Save(context);

    using NYT::Save;
    Save(context, ChunkInfo_);
    Save(context, ChunkMeta_);
    Save(context, AggregatedRequisitionIndex_);
    Save(context, LocalRequisitionIndex_);
    Save(context, ReadQuorum_);
    Save(context, WriteQuorum_);
    Save(context, GetErasureCodec());
    Save(context, GetMovable());
    {
        // COMPAT(shakurov)
        SmallVector<TChunkTree*, TypicalChunkParentCount> parents;
        for (auto [chunkTree, refCount] : Parents_) {
            for (auto i = 0; i < refCount; ++i) {
                parents.push_back(chunkTree);
            }
        }
        std::sort(parents.begin(), parents.end(), TObjectRefComparer::Compare);
        Save(context, parents);
    }
    Save(context, ExpirationTime_);
    if (ReplicasData_) {
        Save(context, true);
        // NB: RemoveReplica calls do not commute and their order is not
        // deterministic (i.e. when unregistering a node we traverse certain hashtables).
        TVectorSerializer<TDefaultSerializer, TSortedTag>::Save(context, ReplicasData_->StoredReplicas);
        Save(context, ReplicasData_->CachedReplicas);
        Save(context, ReplicasData_->LastSeenReplicas);
        Save(context, ReplicasData_->CurrentLastSeenReplicaIndex);
    } else {
        Save(context, false);
    }
    Save(context, ExportCounter_);
    if (ExportCounter_ > 0) {
        YT_ASSERT(ExportDataList_);
        TPodSerializer::Save(context, *ExportDataList_);
    }
}

void TChunk::Load(NCellMaster::TLoadContext& context)
{
    TChunkTree::Load(context);

    using NYT::Load;
    Load(context, ChunkInfo_);
    Load(context, ChunkMeta_);

    Load(context, AggregatedRequisitionIndex_);
    Load(context, LocalRequisitionIndex_);

    SetReadQuorum(Load<i8>(context));
    SetWriteQuorum(Load<i8>(context));
    SetErasureCodec(Load<NErasure::ECodec>(context));
    SetMovable(Load<bool>(context));

    auto parents = Load<SmallVector<TChunkTree*, TypicalChunkParentCount>>(context);
    for (auto* parent : parents) {
        ++Parents_[parent];
    }

    ExpirationTime_ = Load<TInstant>(context);

    if (Load<bool>(context)) {
        auto* data = MutableReplicasData();
        Load(context, data->StoredReplicas);
        Load(context, data->CachedReplicas);
        Load(context, data->LastSeenReplicas);
        Load(context, data->CurrentLastSeenReplicaIndex);

        // COMPAT(babenko)
        if (context.GetVersion() < NCellMaster::EMasterReign::ErasureJournals && IsJournal()) {
            for (auto& replica : data->StoredReplicas) {
                EChunkReplicaState state;
                constexpr int ActiveChunkReplicaIndex   = 0; // the replica is currently being written
                constexpr int UnsealedChunkReplicaIndex = 1; // the replica is finished but not sealed yet
                constexpr int SealedChunkReplicaIndex   = 2; // the replica is finished and sealed
                switch (replica.GetReplicaIndex()) {
                    case ActiveChunkReplicaIndex:     state = EChunkReplicaState::Active; break;
                    case UnsealedChunkReplicaIndex:   state = EChunkReplicaState::Unsealed; break;
                    case SealedChunkReplicaIndex:     state = EChunkReplicaState::Sealed; break;
                    default:                          YT_ABORT();
                }
                replica = TNodePtrWithIndexes(replica.GetPtr(), GenericChunkReplicaIndex, replica.GetMediumIndex(), state);
            }
        }
    }
    Load(context, ExportCounter_);
    if (ExportCounter_ > 0) {
        ExportDataList_ = std::make_unique<TChunkExportDataList>();
        TPodSerializer::Load(context, *ExportDataList_);
        YT_VERIFY(std::any_of(
            ExportDataList_->begin(), ExportDataList_->end(),
            [] (auto data) { return data.RefCounter != 0; }));
    }
    if (IsConfirmed()) {
        MiscExt_ = GetProtoExtension<TMiscExt>(ChunkMeta_.extensions());
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

void TChunk::AddReplica(TNodePtrWithIndexes replica, const TMedium* medium)
{
    auto* data = MutableReplicasData();
    if (medium->GetCache()) {
        YT_VERIFY(!IsJournal());
        auto& cachedReplicas = data->CachedReplicas;
        if (!cachedReplicas) {
            cachedReplicas = std::make_unique<TCachedReplicas>();
        }
        YT_VERIFY(cachedReplicas->insert(replica).second);
    } else {
        if (IsJournal()) {
            for (auto& existingReplica : data->StoredReplicas) {
                if (existingReplica.ToGenericState() == replica.ToGenericState()) {
                    existingReplica = replica;
                    return;
                }
            }
        }
        data->StoredReplicas.push_back(replica);
        if (!medium->GetTransient()) {
            if (IsErasure()) {
                data->LastSeenReplicas[replica.GetReplicaIndex()] = replica.GetPtr()->GetId();
            } else {
                data->LastSeenReplicas[data->CurrentLastSeenReplicaIndex] = replica.GetPtr()->GetId();
                data->CurrentLastSeenReplicaIndex = (data->CurrentLastSeenReplicaIndex + 1) % LastSeenReplicaCount;
            }
        }
    }
}

void TChunk::RemoveReplica(TNodePtrWithIndexes replica, const TMedium* medium)
{
    auto* data = MutableReplicasData();
    if (medium->GetCache()) {
        auto& cachedReplicas = data->CachedReplicas;
        YT_VERIFY(cachedReplicas->erase(replica) == 1);
        if (cachedReplicas->empty()) {
            cachedReplicas.reset();
        }
    } else {
        auto doRemove = [&] (auto converter) {
            auto& storedReplicas = data->StoredReplicas;
            for (auto& existingReplica : storedReplicas) {
                if (converter(existingReplica) == converter(replica)) {
                    std::swap(existingReplica, storedReplicas.back());
                    storedReplicas.pop_back();
                    return;
                }
            }
            YT_ABORT();
        };
        if (IsJournal()) {
            doRemove([] (auto replica) { return replica.ToGenericState(); });
        } else {
            doRemove([] (auto replica) { return replica; });
        }
    }
}

TNodePtrWithIndexesList TChunk::GetReplicas() const
{
    const auto& storedReplicas = StoredReplicas();
    const auto& cachedReplicas = CachedReplicas();
    TNodePtrWithIndexesList result;
    result.reserve(storedReplicas.size() + cachedReplicas.size());
    result.insert(result.end(), storedReplicas.begin(), storedReplicas.end());
    result.insert(result.end(), cachedReplicas.begin(), cachedReplicas.end());
    return result;
}

void TChunk::ApproveReplica(TNodePtrWithIndexes replica)
{
    if (IsJournal()) {
        auto* data = MutableReplicasData();
        auto genericReplica = replica.ToGenericState();
        for (auto& existingReplica : data->StoredReplicas) {
            if (existingReplica.ToGenericState() == genericReplica) {
                existingReplica = replica;
                return;
            }
        }
        YT_ABORT();
    }
}

void TChunk::Confirm(
    TChunkInfo* chunkInfo,
    TChunkMeta* chunkMeta)
{
    // YT-3251
    if (!HasProtoExtension<TMiscExt>(chunkMeta->extensions())) {
        THROW_ERROR_EXCEPTION("Missing TMiscExt in chunk meta");
    }

    ChunkInfo_.Swap(chunkInfo);
    ChunkMeta_.Swap(chunkMeta);
    MiscExt_ = GetProtoExtension<TMiscExt>(ChunkMeta_.extensions());

    YT_VERIFY(IsConfirmed());
}

bool TChunk::IsConfirmed() const
{
    return EChunkType(ChunkMeta_.type()) != EChunkType::Unknown;
}

bool TChunk::IsAvailable() const
{
    if (!ReplicasData_) {
        // Actually it makes no sense calling IsAvailable for foreign chunks.
        return false;
    }

    const auto& storedReplicas = ReplicasData_->StoredReplicas;
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
            if (storedReplicas.size() >= GetReadQuorum()) {
                return true;
            }
            for (auto replica : storedReplicas) {
                if (replica.GetState() == EChunkReplicaState::Sealed) {
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

    return MiscExt_.sealed();
}

i64 TChunk::GetSealedRowCount() const
{
    YT_VERIFY(MiscExt_.sealed());
    return MiscExt_.row_count();
}

void TChunk::Seal(const TMiscExt& info)
{
    YT_VERIFY(IsConfirmed() && !IsSealed());

    // NB: Just a sanity check.
    YT_VERIFY(!MiscExt_.sealed());
    YT_VERIFY(MiscExt_.row_count() == 0);
    YT_VERIFY(MiscExt_.uncompressed_data_size() == 0);
    YT_VERIFY(MiscExt_.compressed_data_size() == 0);
    YT_VERIFY(ChunkInfo_.disk_space() == 0);

    MiscExt_.set_sealed(true);
    MiscExt_.set_row_count(info.row_count());
    MiscExt_.set_uncompressed_data_size(info.uncompressed_data_size());
    MiscExt_.set_compressed_data_size(info.compressed_data_size());
    SetProtoExtension(ChunkMeta_.mutable_extensions(), MiscExt_);
    ChunkInfo_.set_disk_space(info.uncompressed_data_size());  // an approximation
}

int TChunk::GetMaxReplicasPerRack(
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
        case EObjectType::ErasureJournalChunk:
            return std::max(ReadQuorum_ - 1, 1);

        default:
            YT_ABORT();
    }
}

TChunkExportData TChunk::GetExportData(int cellIndex) const
{
    if (ExportCounter_ == 0) {
        return {};
    }

    YT_ASSERT(ExportDataList_);
    return (*ExportDataList_)[cellIndex];
}

bool TChunk::IsExportedToCell(int cellIndex) const
{
    if (ExportCounter_ == 0) {
        return false;
    }

    YT_ASSERT(ExportDataList_);
    return (*ExportDataList_)[cellIndex].RefCounter != 0;
}

void TChunk::Export(int cellIndex, TChunkRequisitionRegistry* registry)
{
    if (ExportCounter_ == 0) {
        ExportDataList_ = std::make_unique<TChunkExportDataList>();
        for (auto& data : *ExportDataList_) {
            data.RefCounter = 0;
            data.ChunkRequisitionIndex = EmptyChunkRequisitionIndex;
        }
    }

    auto& data = (*ExportDataList_)[cellIndex];
    if (++data.RefCounter == 1) {
        ++ExportCounter_;

        YT_VERIFY(data.ChunkRequisitionIndex == EmptyChunkRequisitionIndex);
        registry->Ref(data.ChunkRequisitionIndex);
        // NB: an empty requisition doesn't affect the aggregated requisition
        // and thus doesn't call for updating the latter.
    }
}

void TChunk::Unexport(
    int cellIndex,
    int importRefCounter,
    TChunkRequisitionRegistry* registry,
    const NObjectServer::TObjectManagerPtr& objectManager)
{
    YT_ASSERT(ExportDataList_);
    auto& data = (*ExportDataList_)[cellIndex];
    if ((data.RefCounter -= importRefCounter) == 0) {
        registry->Unref(data.ChunkRequisitionIndex, objectManager);
        data.ChunkRequisitionIndex = EmptyChunkRequisitionIndex; // just in case

        --ExportCounter_;

        if (ExportCounter_ == 0) {
            ExportDataList_.reset();
        }

        UpdateAggregatedRequisitionIndex(registry, objectManager);
    }
}

i64 TChunk::GetMasterMemoryUsage() const
{
    return ChunkMeta().ByteSize();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
