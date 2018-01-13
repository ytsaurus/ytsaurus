#pragma once

#include <yt/ytlib/job_tracker_client/public.h>

#include <yt/core/misc/guid.h>
#include <yt/core/misc/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

using NJobTrackerClient::TJobId;
using NJobTrackerClient::TOperationId;
using NJobTrackerClient::EJobType;
using NJobTrackerClient::EJobState;

static constexpr int MaxSchedulingTagRuleCount = 100;

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TSchedulerJobSpecExt;
class TSchedulerJobResultExt;
class TTableInputSpec;
class TJobResources;
class TJobMetrics;
class TTreeTaggedJobMetrics;
class TOperationJobMetrics;
class TReqHeartbeat;
class TRspHeartbeat;
class TReqGetOperationInfo;
class TRspGetOperationInfo;
class TReqGetJobInfo;
class TRspGetJobInfo;
class TOutputResult;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EOperationType,
    (Map)
    (Merge)
    (Erase)
    (Sort)
    (Reduce)
    (MapReduce)
    (RemoteCopy)
    (JoinReduce)
);

DEFINE_ENUM(EOperationState,
    (None)
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
    ((NoSuchOperation)      (200))
    ((InvalidOperationState)(201))
    ((TooManyOperations)    (202))
    ((NoSuchJob)            (203))
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

DEFINE_ENUM(EJobFinalState,
    (Failed)
    (Aborted)
    (Completed)
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

class TSchedulerServiceProxy;

DECLARE_REFCOUNTED_CLASS(TJobIOConfig)

DECLARE_REFCOUNTED_CLASS(TTestingOperationOptions)

DECLARE_REFCOUNTED_CLASS(TAutoMergeConfig)

DECLARE_REFCOUNTED_CLASS(TSchedulingTagRuleConfig)

DECLARE_REFCOUNTED_CLASS(TResourceLimitsConfig)

DECLARE_REFCOUNTED_CLASS(TOperationSpecBase)

DECLARE_REFCOUNTED_CLASS(TUserJobSpec)

DECLARE_REFCOUNTED_CLASS(TUnorderedOperationSpecBase)

DECLARE_REFCOUNTED_CLASS(TMapOperationSpec)

DECLARE_REFCOUNTED_CLASS(TUnorderedMergeOperationSpec)

DECLARE_REFCOUNTED_CLASS(TSimpleOperationSpecBase)

DECLARE_REFCOUNTED_CLASS(TMergeOperationSpec)

DECLARE_REFCOUNTED_CLASS(TOrderedMergeOperationSpec)

DECLARE_REFCOUNTED_CLASS(TSortedMergeOperationSpec)

DECLARE_REFCOUNTED_CLASS(TEraseOperationSpec)

DECLARE_REFCOUNTED_CLASS(TReduceOperationSpecBase)

DECLARE_REFCOUNTED_CLASS(TReduceOperationSpec)

DECLARE_REFCOUNTED_CLASS(TJoinReduceOperationSpec)

DECLARE_REFCOUNTED_CLASS(TSortOperationSpecBase)

DECLARE_REFCOUNTED_CLASS(TSortOperationSpec)

DECLARE_REFCOUNTED_CLASS(TMapReduceOperationSpec)

DECLARE_REFCOUNTED_CLASS(TRemoteCopyOperationSpec)

DECLARE_REFCOUNTED_CLASS(TPoolConfig)

DECLARE_REFCOUNTED_CLASS(TExtendedSchedulableConfig)

DECLARE_REFCOUNTED_CLASS(TStrategyOperationSpec)

DECLARE_REFCOUNTED_CLASS(TOperationStrategyRuntimeParams)

DECLARE_REFCOUNTED_CLASS(TOperationRuntimeParams)

DECLARE_REFCOUNTED_CLASS(TSchedulerConnectionConfig)

class TJobResources;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESchedulingDelayType,
    (Sync)
    (Async)
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EOperationCypressStorageMode,
    (SimpleHashBuckets)
    (HashBuckets)
    (Compatible)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
