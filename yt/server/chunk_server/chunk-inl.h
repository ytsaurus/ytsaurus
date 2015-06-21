#ifndef CHUNK_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk.h"
#endif
#undef CHUNK_INL_H_

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

inline TChunkDynamicData* TChunk::GetDynamicData() const
{
    return GetTypedDynamicData<TChunkDynamicData>();
}

inline bool TChunk::GetMovable() const
{
    return Flags_.Movable;
}

inline void TChunk::SetMovable(bool value)
{
    Flags_.Movable = value;
}

inline bool TChunk::GetVital() const
{
    return Flags_.Vital;
}

inline void TChunk::SetVital(bool value)
{
    Flags_.Vital = value;
}

inline bool TChunk::GetRefreshScheduled() const
{
    return GetDynamicData()->Flags.RefreshScheduled;
}

inline void TChunk::SetRefreshScheduled(bool value)
{
    GetDynamicData()->Flags.RefreshScheduled = value;
}

inline bool TChunk::GetPropertiesUpdateScheduled() const
{
    return GetDynamicData()->Flags.PropertiesUpdateScheduled;
}

inline void TChunk::SetPropertiesUpdateScheduled(bool value)
{
    GetDynamicData()->Flags.PropertiesUpdateScheduled = value;
}

inline bool TChunk::GetSealScheduled() const
{
    return GetDynamicData()->Flags.SealScheduled;
}

inline void TChunk::SetSealScheduled(bool value)
{
    GetDynamicData()->Flags.SealScheduled = value;
}

inline const TNullable<TChunkRepairQueueIterator>& TChunk::GetRepairQueueIterator() const
{
    return GetDynamicData()->RepairQueueIterator;
}

inline void TChunk::SetRepairQueueIterator(const TNullable<TChunkRepairQueueIterator>& value)
{
    GetDynamicData()->RepairQueueIterator = value;
}

inline void TChunk::Reset()
{
    auto* data = GetDynamicData();
    data->Flags = {};
    data->RepairQueueIterator.Reset();
}

inline int TChunk::GetReplicationFactor() const
{
    return ReplicationFactor_;
}

inline void TChunk::SetReplicationFactor(int value)
{
    ReplicationFactor_ = value;
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
