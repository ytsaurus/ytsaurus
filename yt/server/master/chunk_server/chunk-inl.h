#pragma once
#ifndef CHUNK_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk.h"
// For the sake of sane code completion.
#include "chunk.h"
#endif

#include <yt/client/chunk_client/chunk_replica.h>

namespace NYT::NChunkServer {

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
    auto* iteratorMap = SelectRepairQueueIteratorMap(queue);
    auto it = iteratorMap->find(mediumIndex);
    return it == iteratorMap->end()
        ? TChunkRepairQueueIterator()
        : it->second;
}

inline void TChunk::SetRepairQueueIterator(int mediumIndex, EChunkRepairQueue queue, TChunkRepairQueueIterator value)
{
    auto* iteratorMap = SelectRepairQueueIteratorMap(queue);
    if (value == TChunkRepairQueueIterator()) {
        iteratorMap->erase(mediumIndex);
    } else {
        (*iteratorMap)[mediumIndex] = value;
    }
}

inline TChunkDynamicData::TMediumToRepairQueueIterator* TChunk::SelectRepairQueueIteratorMap(EChunkRepairQueue queue) const
{
    switch (queue) {
        case EChunkRepairQueue::Missing:
            return &GetDynamicData()->MissingPartRepairQueueIterators;

        case EChunkRepairQueue::Decommissioned:
            return &GetDynamicData()->DecommissionedPartRepairQueueIterators;
        default:
            YT_ABORT();
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

    YT_ASSERT(ExportDataList_);
    for (auto data : *ExportDataList_) {
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

    YT_ASSERT(ExportDataList_);
    for (auto data : *ExportDataList_) {
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
    YT_ASSERT(ExportDataList_);
    auto data = (*ExportDataList_)[cellIndex];
    YT_VERIFY(data.RefCounter != 0);
    return data.ChunkRequisitionIndex;
}

inline void TChunk::SetExternalRequisitionIndex(
    int cellIndex,
    TChunkRequisitionIndex requisitionIndex,
    TChunkRequisitionRegistry* registry,
    const NObjectServer::TObjectManagerPtr& objectManager)
{
    YT_ASSERT(ExportDataList_);
    auto& data = (*ExportDataList_)[cellIndex];
    YT_VERIFY(data.RefCounter != 0);
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

    YT_ASSERT(ExportDataList_);
    for (auto data : *ExportDataList_) {
        if (data.RefCounter != 0) {
            result |= registry->GetRequisition(data.ChunkRequisitionIndex);
        }
    }

    return result;
}

inline const TChunkRequisition& TChunk::GetAggregatedRequisition(const TChunkRequisitionRegistry* registry) const
{
    YT_VERIFY(AggregatedRequisitionIndex_ != EmptyChunkRequisitionIndex);
    return registry->GetRequisition(AggregatedRequisitionIndex_);
}

inline const TChunkReplication& TChunk::GetAggregatedReplication(const TChunkRequisitionRegistry* registry) const
{
    YT_VERIFY(AggregatedRequisitionIndex_ != EmptyChunkRequisitionIndex);
    return registry->GetReplication(AggregatedRequisitionIndex_);
}

inline int TChunk::GetAggregatedReplicationFactor(int mediumIndex, const TChunkRequisitionRegistry* registry) const
{
    return GetAggregatedReplication(registry).Get(mediumIndex).GetReplicationFactor();
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
    return NChunkClient::IsErasureChunkType(GetType());
}

inline bool TChunk::IsJournal() const
{
    return NChunkClient::IsJournalChunkType(GetType());
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

} // namespace NYT::NChunkServer
