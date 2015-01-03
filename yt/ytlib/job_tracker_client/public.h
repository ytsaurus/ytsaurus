#pragma once

#include <core/misc/common.h>
#include <core/misc/enum.h>
#include <core/misc/guid.h>

namespace NYT {
namespace NJobTrackerClient {

///////////////////////////////////////////////////////////////////////////////

typedef TGuid TJobId;

DEFINE_ENUM(EJobType,
    // Scheduler jobs
    ((SchedulerFirst)   (  0)) // sentinel
    ((Map)              (  1))
    ((PartitionMap)     (  2))
    ((SortedMerge)      (  3))
    ((OrderedMerge)     (  4))
    ((UnorderedMerge)   (  5))
    ((Partition)        (  6))
    ((SimpleSort)       (  7))
    ((PartitionSort)    (  8))
    ((SortedReduce)     (  9))
    ((PartitionReduce)  ( 10))
    ((ReduceCombiner)   ( 11))
    ((RemoteCopy)       ( 12))
    ((SchedulerLast)    ( 99)) // sentinel

    // Master jobs
    ((MasterFirst)      (100)) // sentinel
    ((Foreign)          (101))
    ((ReplicateChunk)   (102))
    ((RemoveChunk)      (103))
    ((RepairChunk)      (104))
    ((SealChunk)        (105))
    ((MasterLast)       (199)) // sentinel
);

DEFINE_ENUM(EJobState,
    ((Waiting)  (0))
    ((Running)  (1))
    ((Aborting) (2))
    ((Completed)(3))
    ((Failed)   (4))
    ((Aborted)  (5))
);

DEFINE_ENUM(EJobPhase,
    ((Created)         (  0))
    ((PreparingConfig) (  1))
    ((PreparingProxy)  (  2))
    ((PreparingSandbox)( 10))
    ((PreparingFiles)  ( 20))
    ((Running)         ( 50))
    ((Cleanup)         ( 80))
    ((Finished)        (100))
);

///////////////////////////////////////////////////////////////////////////////

} // namespace NJobTrackerClient
} // namespace NYT
