#pragma once

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/chunk_server/public.h>

namespace NYT::NChunkServer::NReplicator {

////////////////////////////////////////////////////////////////////////////////

struct TNode;
struct TMedium;
struct TDataCenter;

DECLARE_REFCOUNTED_STRUCT(IChunkReplicaAllocator)
DECLARE_REFCOUNTED_STRUCT(IJobTracker)
DECLARE_REFCOUNTED_STRUCT(IReplicatorState)

using TNodePtrWithIndexes = TPtrWithIndexes<TNode>;
using TNodePtrWithIndexesList = SmallVector<TNodePtrWithIndexes, NChunkClient::TypicalReplicaCount>;

using TMediumIndex = int;

using NNodeTrackerServer::TDataCenterId;

////////////////////////////////////////////////////////////////////////////////

//! Serializes node id, replica index, medium index.
void ToProto(ui64* protoValue, TNodePtrWithIndexes value);
//! Serializes node id, replica index.
void ToProto(ui32* protoValue, TNodePtrWithIndexes value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
