#ifndef CHUNK_REPLICA_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_replica.h"
#endif

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

FORCED_INLINE TChunkReplica::TChunkReplica()
    : Value(NNodeTrackerClient::InvalidNodeId | (0 << 28))
{ }

FORCED_INLINE TChunkReplica::TChunkReplica(ui32 value)
    : Value(value)
{ }

FORCED_INLINE TChunkReplica::TChunkReplica(int nodeId, int index)
    : Value(nodeId | (index << 28))
{
    YASSERT(nodeId >= 0 && nodeId <= NNodeTrackerClient::MaxNodeId);
    YASSERT(index >= 0 && index < ChunkReplicaIndexBound);
}

FORCED_INLINE int TChunkReplica::GetNodeId() const
{
    return Value & 0x0fffffff;
}

FORCED_INLINE int TChunkReplica::GetIndex() const
{
    return Value >> 28;
}

FORCED_INLINE void ToProto(ui32* value, TChunkReplica replica)
{
    *value = replica.Value;
}

FORCED_INLINE void FromProto(TChunkReplica* replica, ui32 value)
{
    replica->Value = value;
}

////////////////////////////////////////////////////////////////////////////////

FORCED_INLINE TChunkIdWithIndex::TChunkIdWithIndex()
    : Index(0)
{ }

FORCED_INLINE TChunkIdWithIndex::TChunkIdWithIndex(const TChunkId& id, int index)
    : Id(id)
    , Index(index)
{ }

FORCED_INLINE bool operator == (const TChunkIdWithIndex& lhs, const TChunkIdWithIndex& rhs)
{
    return lhs.Id == rhs.Id && lhs.Index == rhs.Index;
}

FORCED_INLINE bool operator != (const TChunkIdWithIndex& lhs, const TChunkIdWithIndex& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
