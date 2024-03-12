#ifndef CHUNK_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk.h"
// For the sake of sane code completion.
#include "chunk.h"
#endif

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/library/erasure/impl/codec.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

inline TRange<TChunkLocationPtrWithReplicaInfo> TChunk::StoredReplicas() const
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
            ReplicasData_ = std::make_unique<TErasureChunkReplicasData>();
        } else {
            ReplicasData_ = std::make_unique<TRegularChunkReplicasData>();
        }
        ReplicasData_->Initialize();
    }
    return ReplicasData_.get();
}

inline TChunkDynamicData* TChunk::GetDynamicData() const
{
    return GetTypedDynamicData<TChunkDynamicData>();
}

inline void TChunk::MaybeResetObsoleteEpochData()
{
    auto* data = GetDynamicData();
    auto currentEpoch = NObjectServer::GetCurrentEpoch();
    if (currentEpoch != data->Epoch) {
        data->EpochScanFlags &= DelegatedScanKinds;
        data->Epoch = currentEpoch;
    }

    auto currentIncumbencyEpoch = GetIncumbencyEpoch(ShardIndex_);
    if (currentIncumbencyEpoch != data->IncumbencyEpoch) {
        data->EpochPartLossTime = {};
        data->EpochScanFlags &= ~DelegatedScanKinds;
        data->IncumbencyEpoch = currentIncumbencyEpoch;
    }
}

inline bool TChunk::GetScanFlag(EChunkScanKind kind) const
{
    auto* data = GetDynamicData();
    if (Any(DelegatedScanKinds & kind)) {
        auto currentIncumbencyEpoch = GetIncumbencyEpoch(ShardIndex_);
        return data->IncumbencyEpoch == currentIncumbencyEpoch ? Any(data->EpochScanFlags & kind) : false;
    } else {
        YT_ASSERT(None(DelegatedScanKinds & kind));
        auto currentEpoch = NObjectServer::GetCurrentEpoch();
        return data->Epoch == currentEpoch ? Any(data->EpochScanFlags & kind) : false;
    }
}

inline void TChunk::SetScanFlag(EChunkScanKind kind)
{
    MaybeResetObsoleteEpochData();
    auto* data = GetDynamicData();
    data->EpochScanFlags |= kind;
}

inline void TChunk::ClearScanFlag(EChunkScanKind kind)
{
    MaybeResetObsoleteEpochData();
    auto* data = GetDynamicData();
    data->EpochScanFlags &= ~kind;
}

inline TChunk* TChunk::GetNextScannedChunk() const
{
    auto* data = GetDynamicData();
    auto& node = data->LinkedListNode;
    return node.Next;
}

inline std::optional<NProfiling::TCpuInstant> TChunk::GetPartLossTime() const
{
    auto* data = GetDynamicData();
    auto currentIncumbencyEpoch = GetIncumbencyEpoch(ShardIndex_);
    if (data->IncumbencyEpoch == currentIncumbencyEpoch && data->EpochPartLossTime != NProfiling::TCpuInstant{}) {
        return data->EpochPartLossTime;
    } else {
        return std::nullopt;
    }
}

inline void TChunk::SetPartLossTime(NProfiling::TCpuInstant partLossTime)
{
    MaybeResetObsoleteEpochData();
    auto* data = GetDynamicData();
    data->EpochPartLossTime = partLossTime;
}

inline void TChunk::ResetPartLossTime()
{
    MaybeResetObsoleteEpochData();
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

inline TChunkRequisitionIndex TChunk::GetLocalRequisitionIndex() const
{
    return LocalRequisitionIndex_;
}

inline void TChunk::SetLocalRequisitionIndex(
    TChunkRequisitionIndex requisitionIndex,
    TChunkRequisitionRegistry* registry,
    const NObjectServer::IObjectManagerPtr& objectManager)
{
    registry->Unref(LocalRequisitionIndex_, objectManager);
    LocalRequisitionIndex_ = requisitionIndex;
    registry->Ref(LocalRequisitionIndex_);

    UpdateAggregatedRequisitionIndex(registry, objectManager);
}

inline TChunkRequisitionIndex TChunk::GetExternalRequisitionIndex(
    NObjectServer::TCellTag cellTag) const
{
    YT_VERIFY(IsExported());
    auto it = GetIteratorOrCrash(*PerCellExportData_, cellTag);
    const auto& data = it->second;
    YT_VERIFY(data.RefCounter != 0);
    return data.ChunkRequisitionIndex;
}

inline void TChunk::SetExternalRequisitionIndex(
    NObjectServer::TCellTag cellTag,
    TChunkRequisitionIndex requisitionIndex,
    TChunkRequisitionRegistry* registry,
    const NObjectServer::IObjectManagerPtr& objectManager)
{
    YT_VERIFY(IsExported());
    auto it = GetIteratorOrCrash(*PerCellExportData_, cellTag);
    auto& data = it->second;
    YT_VERIFY(data.RefCounter != 0);
    registry->Unref(data.ChunkRequisitionIndex, objectManager);
    data.ChunkRequisitionIndex = requisitionIndex;
    registry->Ref(data.ChunkRequisitionIndex);

    UpdateAggregatedRequisitionIndex(registry, objectManager);
}

inline void TChunk::UpdateAggregatedRequisitionIndex(
    TChunkRequisitionRegistry* registry,
    const NObjectServer::IObjectManagerPtr& objectManager)
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

inline const TChunkRequisition& TChunk::GetAggregatedRequisition(const TChunkRequisitionRegistry* registry) const
{
    YT_VERIFY(AggregatedRequisitionIndex_ != EmptyChunkRequisitionIndex);
    return registry->GetRequisition(AggregatedRequisitionIndex_);
}

inline TChunkRequisitionIndex TChunk::GetAggregatedRequisitionIndex() const
{
    YT_VERIFY(AggregatedRequisitionIndex_ != EmptyChunkRequisitionIndex);
    return AggregatedRequisitionIndex_;
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

inline bool TChunk::IsExported() const
{
    YT_ASSERT(!PerCellExportData_ ||
        !PerCellExportData_->empty() &&
        std::all_of(
            PerCellExportData_->begin(),
            PerCellExportData_->end(),
            [] (auto pair) { return pair.second.RefCounter != 0; }));

    return static_cast<bool>(PerCellExportData_);
}

inline void TChunk::OnRefresh()
{
    auto* data = GetDynamicData();
    data->LastRefreshIncumbencyEpoch = GetIncumbencyEpoch(ShardIndex_);
}

inline bool TChunk::IsRefreshActual() const
{
    if (auto* data = GetDynamicData()) {
        return data->LastRefreshIncumbencyEpoch == GetIncumbencyEpoch(ShardIndex_);
    } else {
        YT_VERIFY(!NObjectServer::IsObjectAlive(this));
        return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
