#pragma once

#include <yt/yt/client/scheduler/public.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/library/vector_hdrf/public.h>
#include <yt/yt/library/vector_hdrf/job_resources.h>
#include <yt/yt/library/vector_hdrf/resource_vector.h>
#include <yt/yt/library/vector_hdrf/resource_volume.h>

namespace NYT::NScheduler {

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

DEFINE_ENUM(ESchedulingSegment,
    (Default)
    (LargeGpu)
);

DEFINE_ENUM(EExtraEnvironment,
    (DiscoveryServerAddresses)
);

DECLARE_REFCOUNTED_STRUCT(TJobIOConfig)
DECLARE_REFCOUNTED_STRUCT(TDelayConfig)
DECLARE_REFCOUNTED_STRUCT(TPatchSpecProtocolTestingOptions)
DECLARE_REFCOUNTED_STRUCT(TTestingOperationOptions)
DECLARE_REFCOUNTED_STRUCT(TJobSplitterConfig)
DECLARE_REFCOUNTED_STRUCT(TAutoMergeConfig)
DECLARE_REFCOUNTED_STRUCT(TSchedulingTagRuleConfig)
DECLARE_REFCOUNTED_STRUCT(TSamplingConfig)
DECLARE_REFCOUNTED_STRUCT(TJobResourcesConfig)
DECLARE_REFCOUNTED_STRUCT(TTmpfsVolumeConfig)
DECLARE_REFCOUNTED_STRUCT(TNbdDiskConfig)
DECLARE_REFCOUNTED_STRUCT(TDiskRequestConfig)
DECLARE_REFCOUNTED_STRUCT(TJobShell)
DECLARE_REFCOUNTED_STRUCT(TUserJobMonitoringConfig)
DECLARE_REFCOUNTED_STRUCT(TJobProfilerSpec)
DECLARE_REFCOUNTED_STRUCT(TColumnarStatisticsConfig)
DECLARE_REFCOUNTED_STRUCT(TOperationSpecBase)
DECLARE_REFCOUNTED_STRUCT(TTaskOutputStreamConfig)
DECLARE_REFCOUNTED_STRUCT(TJobExperimentConfig)
DECLARE_REFCOUNTED_STRUCT(TCudaProfilerEnvironment)
DECLARE_REFCOUNTED_STRUCT(TJobFailsTolerance);
DECLARE_REFCOUNTED_STRUCT(TFastIntermediateMediumTableWriterConfig)
DECLARE_REFCOUNTED_STRUCT(TUserJobSpec)
DECLARE_REFCOUNTED_STRUCT(TOptionalUserJobSpec)
DECLARE_REFCOUNTED_STRUCT(TMandatoryUserJobSpec)
DECLARE_REFCOUNTED_STRUCT(TGangManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TVanillaTaskSpec)
DECLARE_REFCOUNTED_STRUCT(TOperationWithInputSpec)
DECLARE_REFCOUNTED_STRUCT(TUnorderedOperationSpecBase)
DECLARE_REFCOUNTED_STRUCT(TMapOperationSpec)
DECLARE_REFCOUNTED_STRUCT(TSortedOperationSpec)
DECLARE_REFCOUNTED_STRUCT(TUnorderedMergeOperationSpec)
DECLARE_REFCOUNTED_STRUCT(TSimpleOperationSpecBase)
DECLARE_REFCOUNTED_STRUCT(TMergeOperationSpec)
DECLARE_REFCOUNTED_STRUCT(TOrderedMergeOperationSpec)
DECLARE_REFCOUNTED_STRUCT(TSortedMergeOperationSpec)
DECLARE_REFCOUNTED_STRUCT(TEraseOperationSpec)
DECLARE_REFCOUNTED_STRUCT(TReduceOperationSpec)
DECLARE_REFCOUNTED_STRUCT(TSortOperationSpecBase)
DECLARE_REFCOUNTED_STRUCT(TSortOperationSpec)
DECLARE_REFCOUNTED_STRUCT(TMapReduceOperationSpec)
DECLARE_REFCOUNTED_STRUCT(TRemoteCopyOperationSpec)
DECLARE_REFCOUNTED_STRUCT(TVanillaOperationSpec)
DECLARE_REFCOUNTED_STRUCT(TPreemptionConfig)
DECLARE_REFCOUNTED_STRUCT(TPoolConfig)
DECLARE_REFCOUNTED_STRUCT(TPoolPresetConfig)
DECLARE_REFCOUNTED_STRUCT(TEphemeralSubpoolConfig)
DECLARE_REFCOUNTED_STRUCT(TPoolIntegralGuaranteesConfig)
DECLARE_REFCOUNTED_STRUCT(TExtendedSchedulableConfig)
DECLARE_REFCOUNTED_STRUCT(TFairShareStrategyPackingConfig)
DECLARE_REFCOUNTED_STRUCT(TStrategyOperationSpec)
DECLARE_REFCOUNTED_STRUCT(TOperationFairShareTreeRuntimeParameters)
DECLARE_REFCOUNTED_STRUCT(TOperationJobShellRuntimeParameters)
DECLARE_REFCOUNTED_STRUCT(TOperationRuntimeParameters)
DECLARE_REFCOUNTED_STRUCT(TOperationFairShareTreeRuntimeParametersUpdate)
DECLARE_REFCOUNTED_STRUCT(TOperationRuntimeParametersUpdate)
DECLARE_REFCOUNTED_STRUCT(TSchedulerConnectionConfig)
DECLARE_REFCOUNTED_STRUCT(TTentativeTreeEligibilityConfig)
DECLARE_REFCOUNTED_STRUCT(TJobCpuMonitorConfig)
DECLARE_REFCOUNTED_STRUCT(TExperimentOperationSpec)
DECLARE_REFCOUNTED_STRUCT(TQueryFilterOptions)
DECLARE_REFCOUNTED_STRUCT(TInputQueryOptions)

using TJobShellOptionsMap = THashMap<
    TString,
    TOperationJobShellRuntimeParametersPtr>;

using TJobShellOptionsUpdateMap = THashMap<
    TString,
    std::optional<TOperationJobShellRuntimeParametersPtr>>;

struct TDiskQuota;
class TJobResourcesWithQuota;

class TOperationServiceProxy;

class TAccessControlRule;

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): switch to std::string
inline const TString RootPoolName("<Root>");
inline const NYPath::TYPath PoolTreesRootCypressPath("//sys/pool_trees");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
