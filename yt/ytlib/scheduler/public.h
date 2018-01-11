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
class TUserJobSpec;

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
    (Scheduler)
    (FailedChunks)
    (ResourceOverdraft)
    (Other)
    (Preemption)
    (UserRequest)
    (NodeOffline)
    (WaitingTimeout)
    (AccountLimitExceeded)
    (GetSpecFailed)
    (Unknown)
    (RevivalConfirmationTimeout)
    (SchedulingFirst)
    (SchedulingTimeout)
    (SchedulingResourceOvercommit)
    (SchedulingOperationSuspended)
    (SchedulingJobSpecThrottling)
    (SchedulingOther)
    (SchedulingLast)
    (IntermediateChunkLimitExceeded)
);

DEFINE_ENUM(EInterruptReason,
    (None)
    (Preemption)
    (UserRequest)
    (JobSplit)
    (Unknown)
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
