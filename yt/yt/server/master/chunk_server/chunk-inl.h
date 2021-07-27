#pragma once
#ifndef CHUNK_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk.h"
// For the sake of sane code completion.
#include "chunk.h"
#endif

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/library/erasure/impl/codec.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

inline const TChunk::TCachedReplicas& TChunk::CachedReplicas() const
{
    const auto& data = ReplicasData();
    return data.CachedReplicas ? *data.CachedReplicas : EmptyCachedReplicas;
}

inline TRange<TNodePtrWithIndexes> TChunk::StoredReplicas() const
{
    const auto& data = ReplicasData();
    return data.GetStoredReplicas();
}

inline TRange<TNodeId> TChunk::LastSeenReplicas() const
{
    const auto& data = ReplicasData();
    return data.GetLastSeenReplicas();
}

inline const TChunk::TReplicasDataBase& TChunk::ReplicasData() const
{
    if (ReplicasData_) {
        return *ReplicasData_;
    }

    return EmptyChunkReplicasData;
}

inline TChunk::TReplicasDataBase* TChunk::MutableReplicasData()
{
    if (!ReplicasData_) {
        if (IsErasure()) {
            ReplicasData_ = TPoolAllocator::New<TErasureChunkReplicasData>();
        } else {
            ReplicasData_ = TPoolAllocator::New<TRegularChunkReplicasData>();
        }
        ReplicasData_->Initialize();
    }
    return ReplicasData_.get();
}

inline TChunkDynamicData* TChunk::GetDynamicData() const
{
    return GetTypedDynamicData<TChunkDynamicData>();
}

inline void TChunk::MaybeResetObsoleteEpochData(NObjectServer::TEpoch epoch)
{
    auto* data = GetDynamicData();
    if (epoch != data->Epoch) {
        data->EpochPartLossTime = {};
        data->EpochScanFlags = EChunkScanKind::None;
        data->Epoch = epoch;
    }
}

inline bool TChunk::GetScanFlag(EChunkScanKind kind, NObjectServer::TEpoch epoch) const
{
    auto* data = GetDynamicData();
    return data->Epoch == epoch ? Any(data->EpochScanFlags & kind) : false;
}

inline void TChunk::SetScanFlag(EChunkScanKind kind, NObjectServer::TEpoch epoch)
{
    MaybeResetObsoleteEpochData(epoch);
    auto* data = GetDynamicData();
    data->EpochScanFlags |= kind;
}

inline void TChunk::ClearScanFlag(EChunkScanKind kind, NObjectServer::TEpoch epoch)
{
    MaybeResetObsoleteEpochData(epoch);
    auto* data = GetDynamicData();
    data->EpochScanFlags &= ~kind;
}

inline TChunk* TChunk::GetNextScannedChunk() const
{
    auto* data = GetDynamicData();
    auto& node = data->LinkedListNode;
    return node.Next;
}

inline std::optional<NProfiling::TCpuInstant> TChunk::GetPartLossTime(NObjectServer::TEpoch epoch) const
{
    auto* data = GetDynamicData();
    if (data->Epoch == epoch && data->EpochPartLossTime != NProfiling::TCpuInstant{}) {
        return data->EpochPartLossTime;
    } else {
        return std::nullopt;
    }
}

inline void TChunk::SetPartLossTime(NProfiling::TCpuInstant partLossTime, NObjectServer::TEpoch epoch)
{
    MaybeResetObsoleteEpochData(epoch);
    auto* data = GetDynamicData();
    data->EpochPartLossTime = partLossTime;
}

inline void TChunk::ResetPartLossTime(NObjectServer::TEpoch epoch)
{
    MaybeResetObsoleteEpochData(epoch);
    auto* data = GetDynamicData();
    data->EpochPartLossTime = NProfiling::TCpuInstant{};
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

inline const TChunkDynamicData::TJobSet& TChunk::GetJobs() const
{
    return GetDynamicData()->Jobs;
}

inline bool TChunk::HasJobs() const
{
    return !GetJobs().empty();
}

inline void TChunk::AddJob(TJobPtr job)
{
    GetDynamicData()->Jobs.emplace_back(std::move(job));
}

inline void TChunk::RemoveJob(const TJobPtr& job)
{
    auto& jobs = GetDynamicData()->Jobs;
    auto jobIt = std::find(jobs.begin(), jobs.end(), job);
    // TODO(gritukan): Is it possible that job is not in job set?
    if (jobIt != jobs.end()) {
        jobs.erase(jobIt);
        jobs.shrink_to_small();
    }
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

inline int TChunk::GetAggregatedPhysicalReplicationFactor(const TChunkRequisitionRegistry* registry) const
{
    YT_VERIFY(AggregatedRequisitionIndex_ != EmptyChunkRequisitionIndex);
    const auto& physicalReplication = registry->GetPhysicalReplication(AggregatedRequisitionIndex_);

    if (IsErasure()) {
        auto* codec = NErasure::GetCodec(GetErasureCodec());
        return physicalReplication.MediumCount * codec->GetTotalPartCount();
    } else {
        return physicalReplication.ReplicaCount;
    }
}

inline i64 TChunk::GetReplicaLagLimit() const
{
    return 1LL << LogReplicaLagLimit_;
}

inline void TChunk::SetReplicaLagLimit(i64 value)
{
    YT_VERIFY(value >= 0);

    LogReplicaLagLimit_ = 0;
    while ((1LL << LogReplicaLagLimit_) < value) {
        ++LogReplicaLagLimit_;
    }
}

inline std::optional<i64> TChunk::GetFirstOverlayedRowIndex() const
{
    return FirstOverlayedRowIndex_ == -1
        ? std::nullopt
        : std::make_optional(FirstOverlayedRowIndex_);
}

inline void TChunk::SetFirstOverlayedRowIndex(std::optional<i64> value)
{
    FirstOverlayedRowIndex_ = value ? *value : -1;
}

inline bool TChunk::IsErasure() const
{
    return NChunkClient::IsErasureChunkType(GetType());
}

inline bool TChunk::IsJournal() const
{
    return NChunkClient::IsJournalChunkType(GetType());
}

inline bool TChunk::IsBlob() const
{
    return NChunkClient::IsBlobChunkType(GetType());
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
