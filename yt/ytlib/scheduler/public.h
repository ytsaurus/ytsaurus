#pragma once

#include <core/misc/common.h>
#include <core/misc/guid.h>

#include <ytlib/job_tracker_client/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

using NJobTrackerClient::TJobId;
using NJobTrackerClient::EJobType;
using NJobTrackerClient::EJobState;

typedef TGuid TOperationId;

DEFINE_ENUM(EOperationType,
    (Map)
    (Merge)
    (Erase)
    (Sort)
    (Reduce)
    (MapReduce)
    (RemoteCopy)
);

DEFINE_ENUM(EOperationState,
    (Initializing)
    (Preparing)
    (Reviving)
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
);

DEFINE_ENUM(EUnavailableChunkAction,
    (Fail)
    (Skip)
    (Wait)
);

DEFINE_ENUM(EAbortReason,
    (Scheduler)
    (FailedChunks)
    (ResourceOverdraft)
    (Other)
);

DEFINE_ENUM(EJobFinalState,
    (Failed)
    (Aborted)
    (Completed)
);

class TSchedulerServiceProxy;

class TJobIOConfig;
typedef TIntrusivePtr<TJobIOConfig> TJobIOConfigPtr;

class TOperationSpecBase;
typedef TIntrusivePtr<TOperationSpecBase> TOperationSpecBasePtr;

class TUserJobSpec;
typedef TIntrusivePtr<TUserJobSpec> TUserJobSpecPtr;

class  TMapOperationSpec;
typedef TIntrusivePtr<TMapOperationSpec> TMapOperationSpecPtr;

class TMergeOperationSpecBase;
typedef TIntrusivePtr<TMergeOperationSpecBase> TMergeOperationSpecBasePtr;

class TMergeOperationSpec;
typedef TIntrusivePtr<TMergeOperationSpec> TMergeOperationSpecPtr;

class TUnorderedMergeOperationSpec;
typedef TIntrusivePtr<TUnorderedMergeOperationSpec> TUnorderedMergeOperationSpecPtr;

class TOrderedMergeOperationSpec;
typedef TIntrusivePtr<TOrderedMergeOperationSpec> TOrderedMergeOperationSpecPtr;

class TSortedMergeOperationSpec;
typedef TIntrusivePtr<TSortedMergeOperationSpec> TSortedMergeOperationSpecPtr;

class TEraseOperationSpec;
typedef TIntrusivePtr<TEraseOperationSpec> TEraseOperationSpecPtr;

class TReduceOperationSpec;
typedef TIntrusivePtr<TReduceOperationSpec> TReduceOperationSpecPtr;

class TSortOperationSpecBase;
typedef TIntrusivePtr<TSortOperationSpecBase> TSortOperationSpecBasePtr;

class TSortOperationSpec;
typedef TIntrusivePtr<TSortOperationSpec> TSortOperationSpecPtr;

class TMapReduceOperationSpec;
typedef TIntrusivePtr<TMapReduceOperationSpec> TMapReduceOperationSpecPtr;

class  TRemoteCopyOperationSpec;
typedef TIntrusivePtr<TRemoteCopyOperationSpec> TRemoteCopyOperationSpecPtr;

class TPoolResourceLimitsConfig;
typedef TIntrusivePtr<TPoolResourceLimitsConfig> TPoolResourceLimitsConfigPtr;

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
