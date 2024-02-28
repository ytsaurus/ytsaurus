#pragma once

#include <yt/yt/client/scheduler/public.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <yt/yt/library/vector_hdrf/public.h>
#include <yt/yt/library/vector_hdrf/job_resources.h>
#include <yt/yt/library/vector_hdrf/resource_vector.h>
#include <yt/yt/library/vector_hdrf/resource_volume.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TAllocationId, TGuid);

constexpr TAllocationId NullAllocationId{};

TAllocationId AllocationIdFromJobId(TJobId jobId);

////////////////////////////////////////////////////////////////////////////////

using NVectorHdrf::EIntegralGuaranteeType;
using NVectorHdrf::ESchedulingMode;
using NVectorHdrf::EJobResourceType;
using NVectorHdrf::TCpuResource;
using NVectorHdrf::TJobResources;
using NVectorHdrf::TResourceVector;
using NVectorHdrf::TResourceVolume;
using NVectorHdrf::ResourceCount;

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TJobExperimentConfig;
class TJobResources;
class TJobResourcesWithQuota;
class TOperationJobMetrics;
class TReqHeartbeat;
class TRspHeartbeat;
class TReqScheduleJobHeartbeat;
class TRspScheduleJobHeartbeat;
class TReqHandshake;
class TRspHandshake;
class TReqGetOperationInfo;
class TRspGetOperationInfo;
class TReqGetJobInfo;
class TRspGetJobInfo;
class TResourceLimits;
class TQuerySpec;
class TDiskQuota;
class TDiskRequest;

} // namespace NProto

static constexpr int MaxSchedulingTagRuleCount = 100;

DEFINE_ENUM(EJobFinalState,
    (Failed)
    (Aborted)
    (Completed)
);

DEFINE_ENUM(EDelayType,
    (Sync)
    (Async)
);

DEFINE_ENUM(EPreemptionMode,
    (Normal)
    (Graceful)
);

DEFINE_ENUM(EEnablePorto,
    ((Isolate) (0))
    ((None)    (1))
);

DEFINE_ENUM(EFifoSortParameter,
    (Weight)
    (StartTime)
    (PendingJobCount)
    (PendingAllocationCount)
);

DEFINE_ENUM(EFifoPoolSchedulingOrder,
    (Fifo)
    (Satisfaction)
);

DEFINE_ENUM(ESchedulingSegment,
    (Default)
    (LargeGpu)
);

DECLARE_REFCOUNTED_CLASS(TJobIOConfig)
DECLARE_REFCOUNTED_CLASS(TDelayConfig)
DECLARE_REFCOUNTED_CLASS(TTestingOperationOptions)
DECLARE_REFCOUNTED_CLASS(TJobSplitterConfig)
DECLARE_REFCOUNTED_CLASS(TAutoMergeConfig)
DECLARE_REFCOUNTED_CLASS(TSchedulingTagRuleConfig)
DECLARE_REFCOUNTED_CLASS(TSamplingConfig)
DECLARE_REFCOUNTED_CLASS(TJobResourcesConfig)
DECLARE_REFCOUNTED_CLASS(TTmpfsVolumeConfig)
DECLARE_REFCOUNTED_CLASS(TDiskRequestConfig)
DECLARE_REFCOUNTED_CLASS(TJobShell)
DECLARE_REFCOUNTED_CLASS(TUserJobMonitoringConfig)
DECLARE_REFCOUNTED_CLASS(TJobProfilerSpec)
DECLARE_REFCOUNTED_CLASS(TColumnarStatisticsConfig)
DECLARE_REFCOUNTED_CLASS(TOperationSpecBase)
DECLARE_REFCOUNTED_CLASS(TTaskOutputStreamConfig)
DECLARE_REFCOUNTED_CLASS(TJobExperimentConfig)
DECLARE_REFCOUNTED_CLASS(TCudaProfilerEnvironment)
DECLARE_REFCOUNTED_CLASS(TUserJobSpec)
DECLARE_REFCOUNTED_CLASS(TOptionalUserJobSpec)
DECLARE_REFCOUNTED_CLASS(TMandatoryUserJobSpec)
DECLARE_REFCOUNTED_CLASS(TVanillaTaskSpec)
DECLARE_REFCOUNTED_CLASS(TOperationWithInputSpec)
DECLARE_REFCOUNTED_CLASS(TUnorderedOperationSpecBase)
DECLARE_REFCOUNTED_CLASS(TMapOperationSpec)
DECLARE_REFCOUNTED_CLASS(TSortedOperationSpec)
DECLARE_REFCOUNTED_CLASS(TUnorderedMergeOperationSpec)
DECLARE_REFCOUNTED_CLASS(TSimpleOperationSpecBase)
DECLARE_REFCOUNTED_CLASS(TMergeOperationSpec)
DECLARE_REFCOUNTED_CLASS(TOrderedMergeOperationSpec)
DECLARE_REFCOUNTED_CLASS(TSortedMergeOperationSpec)
DECLARE_REFCOUNTED_CLASS(TEraseOperationSpec)
DECLARE_REFCOUNTED_CLASS(TReduceOperationSpec)
DECLARE_REFCOUNTED_CLASS(TSortOperationSpecBase)
DECLARE_REFCOUNTED_CLASS(TSortOperationSpec)
DECLARE_REFCOUNTED_CLASS(TMapReduceOperationSpec)
DECLARE_REFCOUNTED_CLASS(TRemoteCopyOperationSpec)
DECLARE_REFCOUNTED_CLASS(TVanillaOperationSpec)
DECLARE_REFCOUNTED_CLASS(TPreemptionConfig)
DECLARE_REFCOUNTED_CLASS(TPoolConfig)
DECLARE_REFCOUNTED_CLASS(TPoolPresetConfig)
DECLARE_REFCOUNTED_CLASS(TEphemeralSubpoolConfig)
DECLARE_REFCOUNTED_CLASS(TPoolIntegralGuaranteesConfig)
DECLARE_REFCOUNTED_CLASS(TExtendedSchedulableConfig)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyPackingConfig)
DECLARE_REFCOUNTED_CLASS(TStrategyOperationSpec)
DECLARE_REFCOUNTED_CLASS(TOperationFairShareTreeRuntimeParameters)
DECLARE_REFCOUNTED_CLASS(TOperationJobShellRuntimeParameters)
DECLARE_REFCOUNTED_CLASS(TOperationRuntimeParameters)
DECLARE_REFCOUNTED_CLASS(TOperationFairShareTreeRuntimeParametersUpdate)
DECLARE_REFCOUNTED_CLASS(TOperationRuntimeParametersUpdate)
DECLARE_REFCOUNTED_CLASS(TSchedulerConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TTentativeTreeEligibilityConfig)
DECLARE_REFCOUNTED_CLASS(TJobCpuMonitorConfig)
DECLARE_REFCOUNTED_CLASS(TExperimentOperationSpec)

using TJobShellOptionsMap = THashMap<
    TString,
    TOperationJobShellRuntimeParametersPtr>;

using TJobShellOptionsUpdeteMap = THashMap<
    TString,
    std::optional<TOperationJobShellRuntimeParametersPtr>>;

struct TDiskQuota;
class TJobResourcesWithQuota;

class TOperationServiceProxy;

////////////////////////////////////////////////////////////////////////////////

inline const TString RootPoolName("<Root>");
inline const TString PoolTreesRootCypressPath("//sys/pool_trees");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
