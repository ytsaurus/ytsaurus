#pragma once

#include <yt/client/scheduler/public.h>

#include <yt/ytlib/job_tracker_client/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TSchedulerJobSpecExt;
class TSchedulerJobResultExt;
class TTableInputSpec;
class TJobResources;
class TJobResourcesWithQuota;
class TJobMetrics;
class TTreeTaggedJobMetrics;
class TOperationJobMetrics;
class TReqHeartbeat;
class TRspHeartbeat;
class TReqHandshake;
class TRspHandshake;
class TReqGetOperationInfo;
class TRspGetOperationInfo;
class TReqGetJobInfo;
class TRspGetJobInfo;
class TOutputResult;
class TUserJobSpec;
class TResourceLimits;

} // namespace NProto

static constexpr int MaxSchedulingTagRuleCount = 100;

DEFINE_ENUM(EJobFinalState,
    (Failed)
    (Aborted)
    (Completed)
);
DEFINE_ENUM(ESchedulingDelayType,
    (Sync)
    (Async)
);

DECLARE_REFCOUNTED_CLASS(TJobIOConfig)
DECLARE_REFCOUNTED_CLASS(TTestingOperationOptions)
DECLARE_REFCOUNTED_CLASS(TAutoMergeConfig)
DECLARE_REFCOUNTED_CLASS(TSchedulingTagRuleConfig)
DECLARE_REFCOUNTED_CLASS(TResourceLimitsConfig)
DECLARE_REFCOUNTED_CLASS(TOperationSpecBase)
DECLARE_REFCOUNTED_CLASS(TUserJobSpec)
DECLARE_REFCOUNTED_CLASS(TVanillaTaskSpec)
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
DECLARE_REFCOUNTED_CLASS(TNewReduceOperationSpec)
DECLARE_REFCOUNTED_CLASS(TSortOperationSpecBase)
DECLARE_REFCOUNTED_CLASS(TSortOperationSpec)
DECLARE_REFCOUNTED_CLASS(TMapReduceOperationSpec)
DECLARE_REFCOUNTED_CLASS(TRemoteCopyOperationSpec)
DECLARE_REFCOUNTED_CLASS(TVanillaOperationSpec)
DECLARE_REFCOUNTED_CLASS(TPoolConfig)
DECLARE_REFCOUNTED_CLASS(TExtendedSchedulableConfig)
DECLARE_REFCOUNTED_CLASS(TStrategyOperationSpec)
DECLARE_REFCOUNTED_CLASS(TOperationRuntimeParameters)
DECLARE_REFCOUNTED_CLASS(TUserFriendlyOperationRuntimeParameters)
DECLARE_REFCOUNTED_CLASS(TSchedulerConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TOperationFairShareTreeRuntimeParameters)
DECLARE_REFCOUNTED_CLASS(TTentativeTreeEligibilityConfig)

class TJobResources;

class TSchedulerServiceProxy;

using NJobTrackerClient::EJobType;

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
