#pragma once
#ifndef CHUNK_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk.h"
#endif

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

inline TChunkDynamicData* TChunk::GetDynamicData() const
{
    return GetTypedDynamicData<TChunkDynamicData>();
}

inline bool TChunk::GetMovable() const
{
    return Movable_;
}

inline void TChunk::SetMovable(bool value)
{
    Movable_ = value;
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

inline const TNullable<TChunkRepairQueueIterator>& TChunk::GetRepairQueueIterator() const
{
    return GetDynamicData()->RepairQueueIterator;
}

inline void TChunk::SetRepairQueueIterator(const TNullable<TChunkRepairQueueIterator>& value)
{
    GetDynamicData()->RepairQueueIterator = value;
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

inline void TChunk::Reset()
{
    auto* data = GetDynamicData();
    data->ScanFlags = EChunkScanKind::None;
    data->RepairQueueIterator.Reset();
    data->Job.Reset();
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
    // NB: Shortcut for non-exported chunk.
    if (ExportCounter_ == 0) {
        return GetLocalProperties();
    }

    TChunkProperties combinedProps = GetLocalProperties();
    for (const auto& data : ExportDataList_) {
        combinedProps |= data.Properties;
    }
    return combinedProps;
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

inline int TChunk::GetLocalReplicationFactor(int mediumIndex) const
{
    return LocalProperties_[mediumIndex].GetReplicationFactor();
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
