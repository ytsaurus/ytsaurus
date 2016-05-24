#pragma once

#ifndef CHUNK_REPLICA_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_replica.h"
#endif

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TChunkReplica::TChunkReplica()
    : Value(NNodeTrackerClient::InvalidNodeId | (0 << 28))
{ }

Y_FORCE_INLINE TChunkReplica::TChunkReplica(ui32 value)
    : Value(value)
{ }

Y_FORCE_INLINE TChunkReplica::TChunkReplica(int nodeId, int index)
    : Value(nodeId | (index << 28))
{
    Y_ASSERT(nodeId >= 0 && nodeId <= NNodeTrackerClient::MaxNodeId);
    Y_ASSERT(index >= 0 && index < ChunkReplicaIndexBound);
}

Y_FORCE_INLINE int TChunkReplica::GetNodeId() const
{
    return Value & 0x0fffffff;
}

Y_FORCE_INLINE int TChunkReplica::GetIndex() const
{
    return Value >> 28;
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
    : Index(0)
{ }

Y_FORCE_INLINE TChunkIdWithIndex::TChunkIdWithIndex(const TChunkId& id, int index)
    : Id(id)
    , Index(index)
{ }

Y_FORCE_INLINE bool operator == (const TChunkIdWithIndex& lhs, const TChunkIdWithIndex& rhs)
{
    return lhs.Id == rhs.Id && lhs.Index == rhs.Index;
}

Y_FORCE_INLINE bool operator != (const TChunkIdWithIndex& lhs, const TChunkIdWithIndex& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
