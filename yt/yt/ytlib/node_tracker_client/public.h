#pragma once

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/misc/public.h>

namespace NYT::NNodeTrackerClient {

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

DECLARE_REFCOUNTED_STRUCT(INodeStatusDirectory)

DEFINE_ENUM(EMemoryCategory,
    ((Footprint)                   (0))
    ((BlockCache)                  (1))
    ((ChunkMeta)                   (2))
    ((ChunkBlockMeta)             (10))
    ((UserJobs)                    (3))
    ((TabletStatic)                (4))
    ((TabletDynamic)               (5))
    ((BlobSession)                 (6))
    ((VersionedChunkMeta)          (7))
    ((SystemJobs)                  (8))
    ((Query)                       (9))
    ((TmpfsLayers)                (11))
    ((MasterCache)                (12))
    ((LookupRowsCache)            (13))
);

DEFINE_ENUM(EMemoryLimitType,
    ((None)                        (0))
    ((Static)                      (1))
    ((Dynamic)                     (2))
);

DEFINE_ENUM(ENodeState,
    // Used internally.
    ((Unknown)    (-1))
    // Not registered.
    ((Offline)     (0))
    // Registered but did not report some of the heartbeats.
    ((Registered)  (1))
    // Registered and reported all the expected types of heartbeats
    // at least once.
    ((Online)      (2))
    // Unregistered and placed into disposal queue.
    ((Unregistered)(3))
    // Indicates that state varies across cells.
    ((Mixed)       (4))
);

DEFINE_ENUM(ENodeRole,
    ((MasterCache)       (0))
    ((TimestampProvider) (1))
);

DEFINE_ENUM(ENodeFlavor,
    // COMPAT(gritukan)
    ((Cluster)      (0))
    // Node that is used to store chunks.
    ((Data)         (1))
    // Node that is used to execute jobs.
    ((Exec)         (2))
    // Node that is used to host dynamic tables tablets.
    ((Tablet)       (3))
    // Node that is used to host chaos cells.
    ((Chaos)        (4))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
