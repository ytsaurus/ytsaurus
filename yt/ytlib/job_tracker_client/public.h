#pragma once

#include <yt/core/misc/enum.h>
#include <yt/core/misc/guid.h>
#include <yt/core/misc/public.h>

namespace NYT {
namespace NJobTrackerClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TJobSpec;
class TReqHeartbeat;
class TRspHeartbeat;
class TJobResult;
class TJobStatus;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using TJobId = TGuid;
extern const TJobId NullJobId;

using TOperationId = TGuid;
extern const TOperationId NullOperationId;

////////////////////////////////////////////////////////////////////////////////

// NB: Please keep the range of values small as this type
// is used as a key of TEnumIndexedVector.
DEFINE_ENUM(EJobType,
    // Scheduler jobs
    ((SchedulerFirst)    (  0)) // Sentinel.
    ((Map)               (  1))
    ((PartitionMap)      (  2))
    ((SortedMerge)       (  3))
    ((OrderedMerge)      (  4))
    ((UnorderedMerge)    (  5))
    ((Partition)         (  6))
    ((SimpleSort)        (  7))
    ((FinalSort)         (  8))
    ((SortedReduce)      (  9))
    ((PartitionReduce)   ( 10))
    ((ReduceCombiner)    ( 11))
    ((RemoteCopy)        ( 12))
    ((IntermediateSort)  ( 13))
    ((OrderedMap)        ( 14))
    ((JoinReduce)        ( 15))
    ((SchedulerUnknown)  ( 97)) // Used by node to report aborted jobs for which spec request has failed
    ((Source)            ( 97)) // Fake job types for data flow graph
    ((Sink)              ( 98)) // denoting inputs and outputs.
    ((SchedulerLast)     ( 99)) // Sentinel.

    // Master jobs
    ((ReplicatorFirst)   (100)) // Sentinel.
    ((ReplicateChunk)    (100))
    ((RemoveChunk)       (101))
    ((RepairChunk)       (102))
    ((SealChunk)         (103))
    ((ReplicatorLast)    (103)) // Sentinel.
);

// NB: Please keep the range of values small as this type
// is used as a key of TEnumIndexedVector.
DEFINE_ENUM(EJobState,
    ((Waiting)    (0))
    ((Running)    (1))
    ((Aborting)   (2))
    ((Completed)  (3))
    ((Failed)     (4))
    ((Aborted)    (5))
    // This sentinel is only used in TJob::GetStatisticsSuffix.
    ((Lost)       (7))
    // Initial state of newly created job.
    ((None)       (8))
);

DEFINE_ENUM(EJobPhase,
    ((Missing)                      (100))

    ((Created)                      (  0))
    ((PreparingNodeDirectory)       (  5))
    ((DownloadingArtifacts)         ( 10))
    ((PreparingSandboxDirectories)  ( 15))
    ((PreparingArtifacts)           ( 20))
    ((PreparingRootVolume)          ( 25))
    ((PreparingProxy)               ( 30))
    ((Running)                      ( 40))
    ((FinalizingProxy)              ( 50))
    ((WaitingAbort)                 ( 60))
    ((Cleanup)                      ( 70))
    ((Finished)                     ( 80))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobTrackerClient
} // namespace NYT
