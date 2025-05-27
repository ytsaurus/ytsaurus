#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqUpdateChunkPresence;
class TSequoiaReplicaInfo;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, ChunkServerLogger, "ChunkServer");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, ChunkServerProfiler, "/chunk_server");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, ChunkServerHistogramProfiler, "/chunk_server/histograms");

YT_DEFINE_GLOBAL(const NProfiling::TProfiler, ChunkServiceProfiler, "/chunk_service");

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IChunkVisitor)
DECLARE_REFCOUNTED_STRUCT(IChunkTraverserContext)
DECLARE_REFCOUNTED_STRUCT(IChunkTreeBalancerCallbacks)
DECLARE_REFCOUNTED_STRUCT(IChunkReplacerCallbacks)
DECLARE_REFCOUNTED_STRUCT(IDataNodeTrackerInternal)
DECLARE_REFCOUNTED_STRUCT(IChunkReplicaFetcher)

DECLARE_REFCOUNTED_CLASS(TJobRegistry)

template <class TPayload>
class TChunkScanQueueWithPayload;
using TChunkScanQueue = TChunkScanQueueWithPayload<void>;

template <class TPayload>
class TChunkScannerWithPayload;
using TChunkScanner = TChunkScannerWithPayload<void>;

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

DEFINE_ENUM(EChunkMergerStatus,
    ((NotInMergePipeline)           (0))
    ((AwaitingMerge)                (1))
    ((InMergePipeline)              (2))
);

// Only used for producing text representation of table chunk formats in
// deprecated TableChunkFormat and TableChunkFormatStatistics attributes.
// Keep in sync with NChunkClient::EChunkFormat.
TStringBuf SerializeChunkFormatAsTableChunkFormat(NChunkClient::EChunkFormat chunkFormat);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
