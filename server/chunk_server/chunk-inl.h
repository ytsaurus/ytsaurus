#pragma once
#ifndef CHUNK_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk.h"
#endif

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

inline const TChunk::TCachedReplicas& TChunk::CachedReplicas() const
{
    const auto& data = ReplicasData();
    return data.CachedReplicas ? *data.CachedReplicas : EmptyCachedReplicas;
}

inline const TChunk::TStoredReplicas& TChunk::StoredReplicas() const
{
    const auto& data = ReplicasData();
    return data.StoredReplicas;
}

inline const TChunk::TLastSeenReplicas& TChunk::LastSeenReplicas() const
{
    const auto& data = ReplicasData();
    return data.LastSeenReplicas;
}

inline const TChunk::TReplicasData& TChunk::ReplicasData() const
{
    return ReplicasData_ ? *ReplicasData_ : EmptyReplicasData;
}

inline TChunk::TReplicasData* TChunk::MutableReplicasData()
{
    if (!ReplicasData_) {
        ReplicasData_ = std::make_unique<TReplicasData>();
        std::fill(ReplicasData_->LastSeenReplicas.begin(), ReplicasData_->LastSeenReplicas.end(), InvalidNodeId);
    }
    return ReplicasData_.get();
}

inline TChunkDynamicData* TChunk::GetDynamicData() const
{
    return GetTypedDynamicData<TChunkDynamicData>();
}

inline bool TChunk::GetScanFlag(EChunkScanKind kind, NObjectServer::TEpoch epoch) const
{
    auto* data = GetDynamicData();
    return data->ScanEpoch == epoch ? Any(data->ScanFlags & kind) : false;
}

inline void TChunk::SetScanFlag(EChunkScanKind kind, NObjectServer::TEpoch epoch)
{
    auto* data = GetDynamicData();
    if (epoch != data->ScanEpoch) {
        data->ScanFlags = EChunkScanKind::None;
        data->ScanEpoch = epoch;
    }
    data->ScanFlags |= kind;
}

inline void TChunk::ClearScanFlag(EChunkScanKind kind, NObjectServer::TEpoch epoch)
{
    auto* data = GetDynamicData();
    if (epoch != data->ScanEpoch) {
        data->ScanFlags = EChunkScanKind::None;
        data->ScanEpoch = epoch;
    }
    data->ScanFlags &= ~kind;
}

inline TChunk* TChunk::GetNextScannedChunk(EChunkScanKind kind) const
{
    auto* data = GetDynamicData();
    auto& node = kind == EChunkScanKind::Seal ? data->JournalLinkedListNode : data->AllLinkedListNode;
    return node.Next;
}

inline TChunkRepairQueueIterator TChunk::GetRepairQueueIterator(int mediumIndex) const
{
    return GetDynamicData()->RepairQueueIterators[mediumIndex];
}

inline void TChunk::SetRepairQueueIterator(int mediumIndex, TChunkRepairQueueIterator value)
{
    GetDynamicData()->RepairQueueIterators[mediumIndex] = value;
}

inline bool TChunk::IsJobScheduled() const
{
    return GetJob().operator bool();
}

inline TJobPtr TChunk::GetJob() const
{
    return GetDynamicData()->Job;
}

inline void TChunk::SetJob(TJobPtr job)
{
    GetDynamicData()->Job = std::move(job);
}

inline void TChunk::RefUsedRequisitions(TChunkRequisitionRegistry* registry) const
{
    registry->Ref(LocalRequisitionIndex_);
    // Don't check TChunk::ExportCounter_ or TChunkExportData::RefCounter here:
    // for those cells into which the chunk isn't exported, EmptyChunkRequisitionIndex is used.
    // And we should Ref() that, too.
    for (const auto& exportData : ExportDataList_) {
        registry->Ref(exportData.ChunkRequisitionIndex);
    }
}

inline void TChunk::UnrefUsedRequisitions(
    TChunkRequisitionRegistry* registry,
    const NObjectServer::TObjectManagerPtr& objectManager) const
{
    registry->Unref(LocalRequisitionIndex_, objectManager);
    for (const auto& exportData : ExportDataList_) {
        registry->Unref(exportData.ChunkRequisitionIndex, objectManager);
    }
}

inline TChunkRequisitionIndex TChunk::GetLocalRequisitionIndex() const
{
    return LocalRequisitionIndex_;
}

inline void TChunk::SetLocalRequisitionIndex(
    TChunkRequisitionIndex requisitionIndex,
    TChunkRequisitionRegistry* registry,
    const NObjectServer::TObjectManagerPtr& objectManager)
{
    registry->Unref(LocalRequisitionIndex_, objectManager);
    LocalRequisitionIndex_ = requisitionIndex;
    registry->Ref(LocalRequisitionIndex_);
}

inline TChunkRequisitionIndex TChunk::GetExternalRequisitionIndex(int cellIndex) const
{
    const auto& data = ExportDataList_[cellIndex];
    YCHECK(data.RefCounter != 0);
    return data.ChunkRequisitionIndex;
}

inline void TChunk::SetExternalRequisitionIndex(
    int cellIndex,
    TChunkRequisitionIndex requisitionIndex,
    TChunkRequisitionRegistry* registry,
    const NObjectServer::TObjectManagerPtr& objectManager)
{
    auto& exportData = ExportDataList_[cellIndex];
    registry->Unref(exportData.ChunkRequisitionIndex, objectManager);
    exportData.ChunkRequisitionIndex = requisitionIndex;
    registry->Ref(exportData.ChunkRequisitionIndex);
}

inline TChunkRequisition TChunk::ComputeRequisition(const TChunkRequisitionRegistry* registry) const
{
    auto result = registry->GetRequisition(LocalRequisitionIndex_);

    // Shortcut for non-exported chunk.
    if (ExportCounter_ == 0) {
        return result;
    }

    for (const auto& data : ExportDataList_) {
        if (data.RefCounter != 0) {
            result |= registry->GetRequisition(data.ChunkRequisitionIndex);
        }
    }
    return result;
}

inline TNullable<TChunkReplication> TChunk::ComputeReplication(const TChunkRequisitionRegistry* registry) const
{
    auto result = registry->GetReplication(LocalRequisitionIndex_);

    auto nonEmptyRequisitionCount = 0;
    if (LocalRequisitionIndex_ != EmptyChunkRequisitionIndex) {
        ++nonEmptyRequisitionCount;
    }

    // Shortcut for non-exported chunk.
    if (ExportCounter_ == 0) {
        return MakeNullable(nonEmptyRequisitionCount != 0, result);
    }

    for (const auto& data : ExportDataList_) {
        if (data.RefCounter != 0) {
            result |= registry->GetReplication(data.ChunkRequisitionIndex);
            if (data.ChunkRequisitionIndex != EmptyChunkRequisitionIndex) {
                ++nonEmptyRequisitionCount;
            }
        }
    }

    return MakeNullable(nonEmptyRequisitionCount != 0, result);
}

inline TNullable<int> TChunk::ComputeReplicationFactor(int mediumIndex, const TChunkRequisitionRegistry* registry) const
{
    auto replication = ComputeReplication(registry);
    if (!replication) {
        return Null;
    }
    return (*replication)[mediumIndex].GetReplicationFactor();
}

inline TNullable<TPerMediumIntArray> TChunk::ComputeReplicationFactors(const TChunkRequisitionRegistry* registry) const
{
    TPerMediumIntArray result;

    auto replication = ComputeReplication(registry);
    if (!replication) {
        return Null;
    }

    auto resultIt = std::begin(result);
    for (const auto& policy : *replication) {
        *resultIt++ = policy.GetReplicationFactor();
    }

    return result;
}

inline int TChunk::GetReadQuorum() const
{
    return ReadQuorum_;
}

inline void TChunk::SetReadQuorum(int value)
{
    ReadQuorum_ = value;
}

inline int TChunk::GetWriteQuorum() const
{
    return WriteQuorum_;
}

inline void TChunk::SetWriteQuorum(int value)
{
    WriteQuorum_ = value;
}

inline NErasure::ECodec TChunk::GetErasureCodec() const
{
    return ErasureCodec_;
}

inline void TChunk::SetErasureCodec(NErasure::ECodec value)
{
    ErasureCodec_ = value;
}

inline bool TChunk::IsErasure() const
{
    return GetType() == NObjectClient::EObjectType::ErasureChunk;
}

inline bool TChunk::IsJournal() const
{
    return GetType() == NObjectClient::EObjectType::JournalChunk;
}

inline bool TChunk::IsRegular() const
{
    return GetType() == NObjectClient::EObjectType::Chunk;
}

inline bool TChunk::IsDiskSizeFinal() const
{
    return IsJournal() ? IsSealed() : IsConfirmed();
}

inline int TChunk::ExportCounter() const
{
    return ExportCounter_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
