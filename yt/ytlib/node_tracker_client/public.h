#pragma once

#include <yt/client/node_tracker_client/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/misc/public.h>

namespace NYT {
namespace NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqRegisterNode;
class TRspRegisterNode;

class TReqIncrementalHeartbeat;
class TRspIncrementalHeartbeat;

class TReqFullHeartbeat;
class TRspFullHeartbeat;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

class TNodeDirectoryBuilder;

DECLARE_REFCOUNTED_CLASS(TNodeDirectorySynchronizer)

DECLARE_REFCOUNTED_CLASS(TNodeDirectorySynchronizerConfig)

DECLARE_REFCOUNTED_STRUCT(INodeChannelFactory)

DEFINE_ENUM(EMemoryCategory,
    ((Footprint)                   (0))
    ((BlockCache)                  (1))
    ((ChunkMeta)                   (2))
    ((UserJobs)                    (3))
    ((TabletStatic)                (4))
    ((TabletDynamic)               (5))
    ((BlobSession)                 (6))
    ((CachedVersionedChunkMeta)    (7))
    ((SystemJobs)                  (8))
    ((Query)                       (9))
);

using TNodeMemoryTracker = TMemoryUsageTracker<EMemoryCategory>;
using TNodeMemoryTrackerPtr = TIntrusivePtr<TNodeMemoryTracker>;

using TNodeMemoryTrackerGuard = TMemoryUsageTrackerGuard<EMemoryCategory>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerClient
} // namespace NYT
