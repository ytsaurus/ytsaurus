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
    (SchedulingTimeout)
    (SchedulingResourceOvercommit)
    (SchedulingOperationSuspended)
    (UserRequest)
    (NodeOffline)
    (WaitingTimeout)
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

class TSchedulerServiceProxy;

class TJobIOConfig;
typedef TIntrusivePtr<TJobIOConfig> TJobIOConfigPtr;

class TResourceLimitsConfig;
typedef TIntrusivePtr<TResourceLimitsConfig> TResourceLimitsConfigPtr;

class TTestingOperationOptions;
typedef TIntrusivePtr<TTestingOperationOptions> TTestingOperationOptionsPtr;

class TOperationSpecBase;
typedef TIntrusivePtr<TOperationSpecBase> TOperationSpecBasePtr;

class TUserJobSpec;
typedef TIntrusivePtr<TUserJobSpec> TUserJobSpecPtr;

class TUnorderedOperationSpecBase;
typedef TIntrusivePtr<TUnorderedOperationSpecBase> TUnorderedOperationSpecBasePtr;

class  TMapOperationSpec;
typedef TIntrusivePtr<TMapOperationSpec> TMapOperationSpecPtr;

class TUnorderedMergeOperationSpec;
typedef TIntrusivePtr<TUnorderedMergeOperationSpec> TUnorderedMergeOperationSpecPtr;

class TSimpleOperationSpecBase;
typedef TIntrusivePtr<TSimpleOperationSpecBase> TSimpleOperationSpecBasePtr;

class TMergeOperationSpec;
typedef TIntrusivePtr<TMergeOperationSpec> TMergeOperationSpecPtr;

class TOrderedMergeOperationSpec;
typedef TIntrusivePtr<TOrderedMergeOperationSpec> TOrderedMergeOperationSpecPtr;

class TSortedMergeOperationSpec;
typedef TIntrusivePtr<TSortedMergeOperationSpec> TSortedMergeOperationSpecPtr;

class TEraseOperationSpec;
typedef TIntrusivePtr<TEraseOperationSpec> TEraseOperationSpecPtr;

class TReduceOperationSpecBase;
typedef TIntrusivePtr<TReduceOperationSpecBase> TReduceOperationSpecBasePtr;

class TReduceOperationSpec;
typedef TIntrusivePtr<TReduceOperationSpec> TReduceOperationSpecPtr;

class TJoinReduceOperationSpec;
typedef TIntrusivePtr<TJoinReduceOperationSpec> TJoinReduceOperationSpecPtr;

class TSortOperationSpecBase;
typedef TIntrusivePtr<TSortOperationSpecBase> TSortOperationSpecBasePtr;

class TSortOperationSpec;
typedef TIntrusivePtr<TSortOperationSpec> TSortOperationSpecPtr;

class TMapReduceOperationSpec;
typedef TIntrusivePtr<TMapReduceOperationSpec> TMapReduceOperationSpecPtr;

class  TRemoteCopyOperationSpec;
typedef TIntrusivePtr<TRemoteCopyOperationSpec> TRemoteCopyOperationSpecPtr;

class TPoolConfig;
typedef TIntrusivePtr<TPoolConfig> TPoolConfigPtr;

class TStrategyOperationSpec;
typedef TIntrusivePtr<TStrategyOperationSpec> TStrategyOperationSpecPtr;

class TOperationRuntimeParams;
typedef TIntrusivePtr<TOperationRuntimeParams> TOperationRuntimeParamsPtr;

DECLARE_REFCOUNTED_CLASS(TSchedulerConnectionConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
