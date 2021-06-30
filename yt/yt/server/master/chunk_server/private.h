#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/profiler.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqUpdateChunkPresence;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ChunkServerLogger;
extern const NProfiling::TProfiler ChunkServerProfilerRegistry;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IChunkVisitor)
DECLARE_REFCOUNTED_STRUCT(IChunkTraverserContext)
DECLARE_REFCOUNTED_STRUCT(IChunkTreeBalancerCallbacks)
DECLARE_REFCOUNTED_STRUCT(IChunkReplacerCallbacks)
DECLARE_REFCOUNTED_CLASS(TExpirationTracker)
DECLARE_REFCOUNTED_CLASS(TJobRegistry)

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


// This enum is used only for text representation of table chunk formats in deprecated
// TableChunkFormat and TableChunkFormatStatistics attributes.
// Keep in sync with NChunkClient::EChunkFormat.
DEFINE_ENUM(ETableChunkFormat,
    ((Old)                  (1))
    ((VersionedSimple)      (2))
    ((Schemaful)            (3))
    ((SchemalessHorizontal) (4))
    ((VersionedColumnar)    (5))
    ((UnversionedColumnar)  (6))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
