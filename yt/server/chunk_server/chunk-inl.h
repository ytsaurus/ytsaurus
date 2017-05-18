#pragma once
#ifndef CHUNK_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk.h"
#endif

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

inline const TChunk::TCachedReplicas& TChunk::CachedReplicas() const
{
    return CachedReplicas_ ? *CachedReplicas_ : EmptyCachedReplicas;
}

inline const TChunk::TStoredReplicas& TChunk::StoredReplicas() const
{
    return StoredReplicas_ ? *StoredReplicas_ : EmptyStoredReplicas;
}

inline const TChunk::TLastSeenReplicas& TChunk::LastSeenReplicas() const
{
    return LastSeenReplicas_;
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

inline const TNullable<TChunkRepairQueueIterator>& TChunk::GetRepairQueueIterator(int mediumIndex) const
{
    return GetDynamicData()->RepairQueueIterators[mediumIndex];
}

inline void TChunk::SetRepairQueueIterator(int mediumIndex, const TNullable<TChunkRepairQueueIterator>& value)
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

inline bool TChunk::ComputeVital() const
{
    // NB: Shortcut for non-exported chunk.
    if (ExportCounter_ == 0) {
        return GetLocalVital();
    }

    if (GetLocalVital()) {
        return true;
    }
    for (const auto& data : ExportDataList_) {
        if (data.Properties.GetVital()) {
            return true;
        }
    }
    return false;
}

inline bool TChunk::GetLocalVital() const
{
    return LocalProperties_.GetVital();
}

inline void TChunk::SetLocalVital(bool value)
{
    LocalProperties_.SetVital(value);
}

inline TChunkProperties TChunk::ComputeProperties() const
{
    auto result = LocalProperties_;

    // Shortcut for non-exported chunk.
    if (ExportCounter_ == 0) {
        return result;
    }

    for (const auto& data : ExportDataList_) {
        result |= data.Properties;
    }
    return result;
}

inline TPerMediumIntArray TChunk::ComputeReplicationFactors() const
{
    TPerMediumIntArray result;

    auto props = ComputeProperties();
    auto resultIt = std::begin(result);
    for (const auto& mediumProps : props) {
        *resultIt++ = mediumProps.GetReplicationFactor();
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
