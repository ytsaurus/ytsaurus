#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqUpdateChunkPresence;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ChunkServerLogger;
extern const NProfiling::TProfiler ChunkServerProfiler;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IChunkVisitor)
DECLARE_REFCOUNTED_STRUCT(IChunkTraverserCallbacks)
DECLARE_REFCOUNTED_STRUCT(IChunkTreeBalancerCallbacks)

class TChunkScanner;

DEFINE_ENUM(EAddReplicaReason,
    (IncrementalHeartbeat)
    (FullHeartbeat)
    (Confirmation)
);

DEFINE_ENUM(ERemoveReplicaReason,
    (None)
    (IncrementalHeartbeat)
    (ApproveTimeout)
    (ChunkDestroyed)
    (NodeDisposed)
);

DEFINE_BIT_ENUM(EJobUnregisterFlags,
    ((None)                  (0x0000))
    ((UnregisterFromNode)    (0x0001))
    ((ScheduleChunkRefresh)  (0x0002))
    ((All)                   (0xffff))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
