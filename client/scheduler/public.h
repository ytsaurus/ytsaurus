#pragma once

#include <yt/core/misc/public.h>

#include <yt/client/job_tracker_client/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

using NJobTrackerClient::TJobId;
using NJobTrackerClient::TOperationId;

DEFINE_ENUM(EOperationType,
    (Map)
    (Merge)
    (Erase)
    (Sort)
    (Reduce)
    (MapReduce)
    (RemoteCopy)
    (JoinReduce)
    (Vanilla)
);

DEFINE_ENUM(EOperationState,
    (None)
    (Starting)
    (Orphaned)
    (WaitingForAgent)
    (Initializing)
    (Preparing)
    (Materializing)
    (Reviving)
    (RevivingJobs)
    (Pending)
    (Running)
    (Completing)
    (Completed)
    (Aborting)
    (Aborted)
    (Failing)
    (Failed)
);

DEFINE_ENUM(EErrorCode,
    ((NoSuchOperation)              (200))
    ((InvalidOperationState)        (201))
    ((TooManyOperations)            (202))
    ((NoSuchJob)                    (203))
    ((OperationFailedOnJobRestart)  (210))
);

DEFINE_ENUM(EUnavailableChunkAction,
    (Fail)
    (Skip)
    (Wait)
);

DEFINE_ENUM(ESchemaInferenceMode,
    (Auto)
    (FromInput)
    (FromOutput)
);

DEFINE_ENUM(EAbortReason,
    ((None)                            (  0))
    ((Scheduler)                       (  1))
    ((FailedChunks)                    (  2))
    ((ResourceOverdraft)               (  3))
    ((Other)                           (  4))
    ((Preemption)                      (  5))
    ((UserRequest)                     (  6))
    ((NodeOffline)                     (  7))
    ((WaitingTimeout)                  (  8))
    ((AccountLimitExceeded)            (  9))
    ((GetSpecFailed)                   ( 10))
    ((Unknown)                         ( 11))
    ((RevivalConfirmationTimeout)      ( 12))
    ((IntermediateChunkLimitExceeded)  ( 13))
    ((NodeBanned)                      ( 14))
    ((SchedulingFirst)                 (100))
    ((SchedulingTimeout)               (101))
    ((SchedulingResourceOvercommit)    (102))
    ((SchedulingOperationSuspended)    (103))
    ((SchedulingJobSpecThrottling)     (104))
    ((SchedulingOther)                 (105))
    ((SchedulingLast)                  (199))
);

DEFINE_ENUM(EInterruptReason,
    ((None)        (0))
    ((Preemption)  (1))
    ((UserRequest) (2))
    ((JobSplit)    (3))
    ((Unknown)     (4))
);

DEFINE_ENUM(ESchedulingMode,
    (Fifo)
    (FairShare)
);

DEFINE_ENUM(EFifoSortParameter,
    (Weight)
    (StartTime)
    (PendingJobCount)
);

DEFINE_ENUM(EAutoMergeMode,
    (Disabled)
    (Relaxed)
    (Economy)
    (Manual)
)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
