#include "chunk.h"
#include "chunk_list.h"
#include "chunk_tree_statistics.h"
#include "medium.h"

#include <yt/server/cell_master/serialize.h>
#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/chunk_server/chunk_manager.h>

#include <yt/server/security_server/account.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/erasure/codec.h>

namespace NYT {
namespace NChunkServer {

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

TChunk::TChunk(const TChunkId& id)
    : TChunkTree(id)
    , LocalRequisitionIndex_(IsErasure()
        ? MigrationErasureChunkRequisitionIndex
        : MigrationChunkRequisitionIndex)
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

void TChunk::Save(NCellMaster::TSaveContext& context) const
{
    TChunkTree::Save(context);

    using NYT::Save;
    Save(context, ChunkInfo_);
    Save(context, ChunkMeta_);
    Save(context, LocalRequisitionIndex_);
    Save(context, ReadQuorum_);
    Save(context, WriteQuorum_);
    Save(context, GetErasureCodec());
    Save(context, GetMovable());
    Save(context, Parents_);
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
        TRangeSerializer::Save(context, TRef::FromPod(ExportDataList_));
    }
}

namespace {

// Compatibility stuff; used by Load().
struct TChunkExportDataBefore400
{
    ui32 RefCounter : 24;
    bool Vital : 1;
    ui8 ReplicationFactor : 7;
};
static_assert(sizeof(TChunkExportDataBefore400) == 4, "sizeof(TChunkExportDataBefore400) != 4");
using TChunkExportDataListBefore400 = TChunkExportDataBefore400[NObjectClient::MaxSecondaryMasterCells];

struct TChunkExportDataBefore619
{
    // Removing this ctor causes internal compiler error on gcc 4.9.2.
    TChunkExportDataBefore619()
        : RefCounter(0)
    { }

    ui32 RefCounter;
    TChunkReplication Replication;
};
static_assert(sizeof(TChunkExportDataBefore619) == 12, "sizeof(TChunkExportDataBefore619) != 12");
using TChunkExportDataListBefore619 = TChunkExportDataBefore619[NObjectClient::MaxSecondaryMasterCells];

} // namespace

void TChunk::Load(NCellMaster::TLoadContext& context)
{
    TChunkTree::Load(context);

    using NYT::Load;
    Load(context, ChunkInfo_);
    Load(context, ChunkMeta_);

    auto setLocalRequisitionIndexFromRF = [&](int replicationFactor) {
        if (replicationFactor < 3 && !IsErasure()) {
            switch (replicationFactor) {
                case 1:
                    LocalRequisitionIndex_ = MigrationErasureChunkRequisitionIndex;
                    break;
                case 2:
                    LocalRequisitionIndex_ = MigrationRF2ChunkRequisitionIndex;
                    break;
                default:
                    Y_ASSERT(LocalRequisitionIndex_ == MigrationChunkRequisitionIndex);
            }
        } else {
            // Leave LocalRequisitionIndex_ defaulted to the migration index.
            Y_ASSERT(LocalRequisitionIndex_ == (IsErasure() ? MigrationErasureChunkRequisitionIndex : MigrationChunkRequisitionIndex));
        }
    };

    // COMPAT(shakurov)
    // Previously, chunks didn't store info on which account requested which RF
    // - just the maximum of those RFs. Chunk requisition can't be computed from
    // such limited data. Thus, we have to fallback to default requisition for
    // the local cell and empty requisition for other cells. This will be
    // corrected by the next requisition update - which will happen soon since
    // it's requested on each leader change.
    if (context.GetVersion() < 400) {
        auto replicationFactor = Load<i8>(context);
        setLocalRequisitionIndexFromRF(replicationFactor);
    } else if (context.GetVersion() < 700) {
        // Discard replication and leave LocalRequisitionIndex_ defaulted to the migration index.
        auto replication = Load<TChunkReplication>(context);
        setLocalRequisitionIndexFromRF(replication[DefaultStoreMediumIndex].GetReplicationFactor());
    } else {
        LocalRequisitionIndex_ = Load<TChunkRequisitionIndex>(context);
    }

    SetReadQuorum(Load<i8>(context));
    SetWriteQuorum(Load<i8>(context));
    SetErasureCodec(Load<NErasure::ECodec>(context));
    SetMovable(Load<bool>(context));
    // COMPAT(shakurov)
    if (context.GetVersion() < 400) {
        // Discard vitality and leave LocalRequisitionIndex_ defaulted to the migration index.
        Load<bool>(context);
    }

    Load(context, Parents_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 616) {
        if (Load<bool>(context)) {
            auto* data = MutableReplicasData();
            Load(context, data->StoredReplicas);
            Load(context, data->CachedReplicas);
            Load(context, data->LastSeenReplicas);
            Load(context, data->CurrentLastSeenReplicaIndex);
        }
    } else {
        auto* data = MutableReplicasData();
        auto storedReplicas = Load<std::unique_ptr<TStoredReplicas>>(context);
        if (storedReplicas) {
            data->StoredReplicas = std::move(*storedReplicas);
        }
        Load(context, data->CachedReplicas);
        // COMPAT(babenko)
        if (context.GetVersion() >= 603) {
            Load(context, data->LastSeenReplicas);
            Load(context, data->CurrentLastSeenReplicaIndex);
        } else {
            for (auto replica : StoredReplicas()) {
                if (IsErasure()) {
                    data->LastSeenReplicas[replica.GetReplicaIndex()] = replica.GetPtr()->GetId();
                } else {
                    data->LastSeenReplicas[data->CurrentLastSeenReplicaIndex] = replica.GetPtr()->GetId();
                    data->CurrentLastSeenReplicaIndex = (data->CurrentLastSeenReplicaIndex + 1) % LastSeenReplicaCount;
                }
            }
        }
        if (data->StoredReplicas.empty() && !data->CachedReplicas) {
            ReplicasData_.reset();
        }
    }
    Load(context, ExportCounter_);
    if (ExportCounter_ > 0) {
        // COMPAT(shakurov)
        if (context.GetVersion() < 400) {
            TChunkExportDataListBefore400 oldExportDataList = {};
            TRangeSerializer::Load(context, TMutableRef::FromPod(oldExportDataList));
            for (auto i = 0; i < NObjectClient::MaxSecondaryMasterCells; ++i) {
                auto& exportData = ExportDataList_[i];
                exportData.RefCounter = oldExportDataList[i].RefCounter;
                // Drop RF and vitality.
                exportData.ChunkRequisitionIndex = EmptyChunkRequisitionIndex;
            }
        } else if (context.GetVersion() < 700) {
            TChunkExportDataListBefore619 oldExportDataList = {};
            TRangeSerializer::Load(context, TMutableRef::FromPod(oldExportDataList));
            for (int i = 0; i < NObjectClient::MaxSecondaryMasterCells; ++i) {
                auto& exportData = ExportDataList_[i];
                exportData.RefCounter = oldExportDataList[i].RefCounter;
                // Drop TChunkReplication.
                exportData.ChunkRequisitionIndex = EmptyChunkRequisitionIndex;
            }
        } else {
            TRangeSerializer::Load(context, TMutableRef::FromPod(ExportDataList_));
        }
    }
    if (IsConfirmed()) {
        MiscExt_ = GetProtoExtension<TMiscExt>(ChunkMeta_.extensions());
    }
}

void TChunk::AddParent(TChunkList* parent)
{
    Parents_.push_back(parent);
}

void TChunk::RemoveParent(TChunkList* parent)
{
    auto it = std::find(Parents_.begin(), Parents_.end(), parent);
    YCHECK(it != Parents_.end());
    Parents_.erase(it);
}

void TChunk::AddReplica(TNodePtrWithIndexes replica, const TMedium* medium)
{
    auto* data = MutableReplicasData();
    if (medium->GetCache()) {
        Y_ASSERT(!IsJournal());
        auto& cachedReplicas = data->CachedReplicas;
        if (!cachedReplicas) {
            cachedReplicas = std::make_unique<TCachedReplicas>();
        }
        YCHECK(cachedReplicas->insert(replica).second);
    } else {
        if (IsJournal()) {
            for (auto& existingReplica : data->StoredReplicas) {
                if (existingReplica.GetPtr() == replica.GetPtr()) {
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
        Y_ASSERT(cachedReplicas);
        YCHECK(cachedReplicas->erase(replica) == 1);
        if (cachedReplicas->empty()) {
            cachedReplicas.reset();
        }
    } else {
        auto& storedReplicas = data->StoredReplicas;
        for (auto it = storedReplicas.begin(); it != storedReplicas.end(); ++it) {
            auto& existingReplica = *it;
            if (existingReplica == replica ||
                IsJournal() && existingReplica.GetPtr() == replica.GetPtr())
            {
                std::swap(existingReplica, storedReplicas.back());
                storedReplicas.pop_back();
                return;
            }
        }
        Y_UNREACHABLE();
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
        for (auto& existingReplica : data->StoredReplicas) {
            if (existingReplica.GetPtr() == replica.GetPtr()) {
                existingReplica = replica;
                return;
            }
        }
        Y_UNREACHABLE();
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

    YCHECK(IsConfirmed());
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

        case EObjectType::ErasureChunk: {
            auto* codec = NErasure::GetCodec(GetErasureCodec());
            int dataPartCount = codec->GetDataPartCount();
            NErasure::TPartIndexSet missingIndexSet((1 << dataPartCount) - 1);
            for (auto replica : storedReplicas) {
                missingIndexSet.reset(replica.GetReplicaIndex());
            }
            return missingIndexSet.none();
        }

        case EObjectType::JournalChunk:
            if (storedReplicas.size() >= GetReadQuorum()) {
                return true;
            }
            for (auto replica : storedReplicas) {
                if (replica.GetReplicaIndex() == SealedChunkReplicaIndex) {
                    return true;
                }
            }
            return false;

        default:
            Y_UNREACHABLE();
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
    YCHECK(MiscExt_.sealed());
    return MiscExt_.row_count();
}

void TChunk::Seal(const TMiscExt& info)
{
    YCHECK(IsConfirmed() && !IsSealed());

    // NB: Just a sanity check.
    YCHECK(!MiscExt_.sealed());
    YCHECK(MiscExt_.row_count() == 0);
    YCHECK(MiscExt_.uncompressed_data_size() == 0);
    YCHECK(MiscExt_.compressed_data_size() == 0);
    YCHECK(ChunkInfo_.disk_space() == 0);

    MiscExt_.set_sealed(true);
    MiscExt_.set_row_count(info.row_count());
    MiscExt_.set_uncompressed_data_size(info.uncompressed_data_size());
    MiscExt_.set_compressed_data_size(info.compressed_data_size());
    SetProtoExtension(ChunkMeta_.mutable_extensions(), MiscExt_);
    ChunkInfo_.set_disk_space(info.uncompressed_data_size());  // an approximation
}

TNullable<int> TChunk::GetMaxReplicasPerRack(
    int mediumIndex,
    TNullable<int> replicationFactorOverride,
    const TChunkRequisitionRegistry* registry) const
{
    switch (GetType()) {
        case EObjectType::Chunk: {
            if (replicationFactorOverride) {
                return *replicationFactorOverride;
            }
            auto replicationFactor = ComputeReplicationFactor(mediumIndex, registry);
            if (replicationFactor) {
                return std::max(*replicationFactor - 1, 1);
            }
            return Null;
        }

        case EObjectType::ErasureChunk:
            return NErasure::GetCodec(GetErasureCodec())->GetGuaranteedRepairablePartCount();

        case EObjectType::JournalChunk: {
            int minQuorum = std::min(ReadQuorum_, WriteQuorum_);
            return std::max(minQuorum - 1, 1);
        }

        default:
            Y_UNREACHABLE();
    }
}

const TChunkExportData& TChunk::GetExportData(int cellIndex) const
{
    return ExportDataList_[cellIndex];
}

void TChunk::Export(int cellIndex)
{
    auto& data = ExportDataList_[cellIndex];
    if (++data.RefCounter == 1) {
        ++ExportCounter_;
    }
}

void TChunk::Unexport(
    int cellIndex,
    int importRefCounter,
    TChunkRequisitionRegistry* registry,
    const NObjectServer::TObjectManagerPtr& objectManager)
{
    auto& data = ExportDataList_[cellIndex];
    if ((data.RefCounter -= importRefCounter) == 0) {
        // NB: Reset the entry to the neutral state as it affects ComputeReplication() etc.
        registry->Unref(data.ChunkRequisitionIndex, objectManager);
        data.ChunkRequisitionIndex = EmptyChunkRequisitionIndex;
        registry->Ref(EmptyChunkRequisitionIndex);
        --ExportCounter_;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

Y_DECLARE_PODTYPE(NYT::NChunkServer::TChunkExportDataListBefore400);
Y_DECLARE_PODTYPE(NYT::NChunkServer::TChunkExportDataListBefore619);
