#pragma once

#include "public.h"

#include <core/logging/log.h>
#include <core/profiling/profiler.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ChunkServerLogger;
extern const NProfiling::TProfiler ChunkServerProfiler;

DECLARE_REFCOUNTED_STRUCT(IChunkVisitor)
DECLARE_REFCOUNTED_STRUCT(IChunkTraverserCallbacks)
DECLARE_REFCOUNTED_STRUCT(IChunkTreeBalancerCallbacks)

DEFINE_ENUM(EAddReplicaReason,
    (IncrementalHeartbeat)
    (FullHeartbeat)
    (Confirmation)
);

DEFINE_ENUM(ERemoveReplicaReason,
    (None)
    (IncrementalHeartbeat)
    (FailedToApprove)
    (ChunkIsDead)
    (NodeRemoved)
);

DEFINE_BIT_ENUM(EJobUnregisterFlags,
    ((None)                  (0x0000))
    ((UnregisterFromChunk)   (0x0001))
    ((UnregisterFromNode)    (0x0002))
    ((ScheduleChunkRefresh)  (0x0004))
    ((All)                   (0xffff))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
