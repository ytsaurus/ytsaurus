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

inline TChunkRepairQueueIterator TChunk::GetRepairQueueIterator(int mediumIndex, EChunkRepairQueue queue) const
{
    switch (queue) {
        case EChunkRepairQueue::Missing:
            return GetDynamicData()->MissingPartRepairQueueIterators[mediumIndex];
        case EChunkRepairQueue::Decommissioned:
            return GetDynamicData()->DecommissionedPartRepairQueueIterators[mediumIndex];
        default:
            Y_UNREACHABLE();
    }
}

inline void TChunk::SetRepairQueueIterator(int mediumIndex, EChunkRepairQueue queue, TChunkRepairQueueIterator value)
{
    switch (queue) {
        case EChunkRepairQueue::Missing:
            GetDynamicData()->MissingPartRepairQueueIterators[mediumIndex] = value;
            break;
        case EChunkRepairQueue::Decommissioned:
            GetDynamicData()->DecommissionedPartRepairQueueIterators[mediumIndex] = value;
            break;
        default:
            Y_UNREACHABLE();
    }
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
    registry->Ref(AggregatedRequisitionIndex_);
    registry->Ref(LocalRequisitionIndex_);

    if (ExportCounter_ == 0) {
        return;
    }

    YCHECK(ExportDataList_);

    for (auto i = 0; i < NObjectClient::MaxSecondaryMasterCells; ++i) {
        const auto& data = ExportDataList_[i];
        if (data.RefCounter != 0) {
            registry->Ref(data.ChunkRequisitionIndex);
        }
    }
}

inline void TChunk::UnrefUsedRequisitions(
    TChunkRequisitionRegistry* registry,
    const NObjectServer::TObjectManagerPtr& objectManager) const
{
    registry->Unref(AggregatedRequisitionIndex_, objectManager);
    registry->Unref(LocalRequisitionIndex_, objectManager);

    if (ExportCounter_ == 0) {
        return;
    }

    YCHECK(ExportDataList_);

    for (auto i = 0; i < NObjectClient::MaxSecondaryMasterCells; ++i) {
        const auto& data = ExportDataList_[i];
        if (data.RefCounter != 0) {
            registry->Unref(data.ChunkRequisitionIndex, objectManager);
        }
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

    UpdateAggregatedRequisitionIndex(registry, objectManager);
}

inline TChunkRequisitionIndex TChunk::GetExternalRequisitionIndex(int cellIndex) const
{
    YCHECK(ExportDataList_);
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
    YCHECK(ExportDataList_);
    auto& data = ExportDataList_[cellIndex];
    YCHECK(data.RefCounter != 0);
    registry->Unref(data.ChunkRequisitionIndex, objectManager);
    data.ChunkRequisitionIndex = requisitionIndex;
    registry->Ref(data.ChunkRequisitionIndex);

    UpdateAggregatedRequisitionIndex(registry, objectManager);
}

inline void TChunk::UpdateAggregatedRequisitionIndex(
    TChunkRequisitionRegistry* registry,
    const NObjectServer::TObjectManagerPtr& objectManager)
{
    auto requisition = ComputeAggregatedRequisition(registry);
    if (requisition.GetEntryCount() == 0) {
        // This doesn't mean the chunk is no longer needed; this may be a
        // temporary contingency. The aggregated requisition should never
        // be made empty as this may confuse the replicator.
        return;
    }

    auto newIndex = registry->GetOrCreate(requisition, objectManager);
    if (newIndex != AggregatedRequisitionIndex_) {
        registry->Unref(AggregatedRequisitionIndex_, objectManager);
        AggregatedRequisitionIndex_ = newIndex;
        registry->Ref(AggregatedRequisitionIndex_);
    }
}

inline TChunkRequisition TChunk::ComputeAggregatedRequisition(const TChunkRequisitionRegistry* registry)
{
    auto result = registry->GetRequisition(LocalRequisitionIndex_);

    // Shortcut for non-exported chunk.
    if (ExportCounter_ == 0) {
        return result;
    }

    YCHECK(ExportDataList_);

    for (auto i = 0; i < NObjectClient::MaxSecondaryMasterCells; ++i) {
        const auto& data = ExportDataList_[i];
        if (data.RefCounter != 0) {
            result |= registry->GetRequisition(data.ChunkRequisitionIndex);
        }
    }
    return result;
}

inline const TChunkRequisition& TChunk::GetAggregatedRequisition(const TChunkRequisitionRegistry* registry) const
{
    YCHECK(AggregatedRequisitionIndex_ != EmptyChunkRequisitionIndex);
    return registry->GetRequisition(AggregatedRequisitionIndex_);
}

inline TChunkReplication TChunk::GetAggregatedReplication(const TChunkRequisitionRegistry* registry) const
{
    YCHECK(AggregatedRequisitionIndex_ != EmptyChunkRequisitionIndex);
    return registry->GetReplication(AggregatedRequisitionIndex_);
}

inline int TChunk::GetAggregatedReplicationFactor(int mediumIndex, const TChunkRequisitionRegistry* registry) const
{
    return GetAggregatedReplication(registry)[mediumIndex].GetReplicationFactor();
}

inline TPerMediumIntArray TChunk::GetAggregatedReplicationFactors(const TChunkRequisitionRegistry* registry) const
{
    auto replication = GetAggregatedReplication(registry);

    TPerMediumIntArray result;
    auto resultIt = std::begin(result);
    for (const auto& policy : replication) {
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
