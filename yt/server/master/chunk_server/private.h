#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NChunkServer {

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
