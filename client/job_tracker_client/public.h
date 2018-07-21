#pragma once

#include <yt/core/misc/enum.h>
#include <yt/core/misc/guid.h>

namespace NYT {
namespace NJobTrackerClient {

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
    ((Vanilla)           ( 16))
    ((SchedulerUnknown)  ( 98)) // Used by node to report aborted jobs for which spec request has failed
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
////////////////////////////////////////////////////////////////////////////////

} // namespace NJobTrackerClient
} // namespace NYT
