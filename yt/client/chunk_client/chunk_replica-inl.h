#pragma once
#ifndef CHUNK_REPLICA_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_replica.h"
// For the sake of sane code completion.
#include "chunk_replica.h"
#endif

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TChunkReplica::TChunkReplica()
    : Value(NNodeTrackerClient::InvalidNodeId | (0 << 24))
{ }

Y_FORCE_INLINE TChunkReplica::TChunkReplica(ui32 value)
    : Value(value)
{ }

Y_FORCE_INLINE TChunkReplica::TChunkReplica(int nodeId, int replicaIndex, int mediumIndex)
    : Value(nodeId | (replicaIndex << 24) | (mediumIndex << 29))
{
    static_assert(
        ChunkReplicaIndexBound * MediumIndexBound <= 0x100,
        "Replica and medium indexes must fit into a single byte.");

    Y_ASSERT(nodeId >= 0 && nodeId <= static_cast<int>(NNodeTrackerClient::MaxNodeId));
    Y_ASSERT(replicaIndex >= 0 && replicaIndex < ChunkReplicaIndexBound);
    Y_ASSERT(mediumIndex >= 0 && mediumIndex < MediumIndexBound);
}

Y_FORCE_INLINE int TChunkReplica::GetNodeId() const
{
    return Value & 0x00ffffff;
}

Y_FORCE_INLINE int TChunkReplica::GetReplicaIndex() const
{
    return (Value & 0x1f000000) >> 24;
}

Y_FORCE_INLINE int TChunkReplica::GetMediumIndex() const
{
    return Value >> 29;
}

Y_FORCE_INLINE void ToProto(ui32* value, TChunkReplica replica)
{
    *value = replica.Value;
}

Y_FORCE_INLINE void FromProto(TChunkReplica* replica, ui32 value)
{
    replica->Value = value;
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TChunkIdWithIndex::TChunkIdWithIndex()
    : ReplicaIndex(GenericChunkReplicaIndex)
{ }

Y_FORCE_INLINE TChunkIdWithIndex::TChunkIdWithIndex(const TChunkId& id, int replicaIndex)
    : Id(id)
    , ReplicaIndex(replicaIndex)
{ }

Y_FORCE_INLINE bool operator==(const TChunkIdWithIndex& lhs, const TChunkIdWithIndex& rhs)
{
    return lhs.Id == rhs.Id && lhs.ReplicaIndex == rhs.ReplicaIndex;
}

Y_FORCE_INLINE bool operator!=(const TChunkIdWithIndex& lhs, const TChunkIdWithIndex& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TChunkIdWithIndexes::TChunkIdWithIndexes()
    : TChunkIdWithIndex()
    , MediumIndex(DefaultStoreMediumIndex)
{ }

Y_FORCE_INLINE TChunkIdWithIndexes::TChunkIdWithIndexes(const TChunkIdWithIndex& chunkIdWithIndex, int mediumIndex)
    : TChunkIdWithIndex(chunkIdWithIndex)
    , MediumIndex(mediumIndex)
{ }

Y_FORCE_INLINE TChunkIdWithIndexes::TChunkIdWithIndexes(const TChunkId& id, int replicaIndex, int mediumIndex)
    : TChunkIdWithIndex(id, replicaIndex)
    , MediumIndex(mediumIndex)
{ }

Y_FORCE_INLINE bool operator==(const TChunkIdWithIndexes& lhs, const TChunkIdWithIndexes& rhs)
{
    return static_cast<const TChunkIdWithIndex&>(lhs) == static_cast<const TChunkIdWithIndex&>(rhs) &&
        lhs.MediumIndex == rhs.MediumIndex;
}

Y_FORCE_INLINE bool operator!=(const TChunkIdWithIndexes& lhs, const TChunkIdWithIndexes& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
