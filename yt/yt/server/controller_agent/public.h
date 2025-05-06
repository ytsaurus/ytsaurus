#pragma once

#include <yt/yt/server/lib/controller_agent/public.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMergeJobType,
    (Deep)
    (Shallow)
);

DEFINE_ENUM(EOperationControllerQueue,
    (Default)
    (GetJobSpec)
    (ScheduleJob)
    (JobEvents)
);

DEFINE_ENUM(EFailOnJobRestartReason,
    (JobAborted)
    (JobFailed)
    (JobCountMismatchAfterRevival)
    (RevivalIsForbidden)
    (RevivalWithCleanStart)
    (JobRevivalDisabled)
);

DEFINE_ENUM(ECpuLimitOvercommitMode,
    (Linear)
    (Minimum)
);

DECLARE_REFCOUNTED_STRUCT(IOperationControllerSchedulerHost)
DECLARE_REFCOUNTED_STRUCT(IOperationControllerSnapshotBuilderHost)

DECLARE_REFCOUNTED_CLASS(TIntermediateChunkScraper)
DECLARE_REFCOUNTED_STRUCT(TIntermediateChunkScraperConfig)

DECLARE_REFCOUNTED_STRUCT(TDataBalancerOptions)

DECLARE_REFCOUNTED_STRUCT(TUserJobOptions)

DECLARE_REFCOUNTED_STRUCT(TGpuCheckOptions)
DECLARE_REFCOUNTED_STRUCT(TOperationOptions)
DECLARE_REFCOUNTED_STRUCT(TSimpleOperationOptions)
DECLARE_REFCOUNTED_STRUCT(TMapOperationOptions)
DECLARE_REFCOUNTED_STRUCT(TUnorderedMergeOperationOptions)
DECLARE_REFCOUNTED_STRUCT(TOrderedMergeOperationOptions)
DECLARE_REFCOUNTED_STRUCT(TSortedMergeOperationOptions)
DECLARE_REFCOUNTED_STRUCT(TEraseOperationOptions)
DECLARE_REFCOUNTED_STRUCT(TReduceOperationOptions)
DECLARE_REFCOUNTED_STRUCT(TJoinReduceOperationOptions)
DECLARE_REFCOUNTED_STRUCT(TSortOperationOptionsBase)
DECLARE_REFCOUNTED_STRUCT(TSortOperationOptions)
DECLARE_REFCOUNTED_STRUCT(TMapReduceOperationOptions)
DECLARE_REFCOUNTED_STRUCT(TRemoteCopyOperationOptions)
DECLARE_REFCOUNTED_STRUCT(TVanillaOperationOptions)

DECLARE_REFCOUNTED_STRUCT(TJobSplitterConfig)

DECLARE_REFCOUNTED_STRUCT(TZombieOperationOrchidsConfig)

DECLARE_REFCOUNTED_STRUCT(TAlertManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TLowGpuPowerUsageOnWindowConfig)
DECLARE_REFCOUNTED_STRUCT(TTestingOptions)
DECLARE_REFCOUNTED_STRUCT(TSuspiciousJobsOptions)

DECLARE_REFCOUNTED_STRUCT(TUserJobMonitoringConfig)

DECLARE_REFCOUNTED_STRUCT(TUserFileLimitsConfig)
DECLARE_REFCOUNTED_STRUCT(TUserFileLimitsPatchConfig)

DECLARE_REFCOUNTED_CLASS(TControllerAgent)
DECLARE_REFCOUNTED_STRUCT(TControllerAgentConfig)
DECLARE_REFCOUNTED_STRUCT(TControllerAgentBootstrapConfig)
DECLARE_REFCOUNTED_STRUCT(TControllerAgentProgramConfig)

DECLARE_REFCOUNTED_STRUCT(IOperationControllerHost)
DECLARE_REFCOUNTED_STRUCT(IOperationController)

DECLARE_REFCOUNTED_CLASS(TOperationControllerHost)

DECLARE_REFCOUNTED_CLASS(TMemoryWatchdog)
DECLARE_REFCOUNTED_STRUCT(TMemoryWatchdogConfig)

DECLARE_REFCOUNTED_CLASS(TOperation)
using TOperationIdToOperationMap = THashMap<TOperationId, TOperationPtr>;

struct TOperationControllerInitializeResult;
struct TOperationControllerReviveResult;
struct TOperationControllerPrepareResult;

class TSchedulingContext;
class TAllocationSchedulingContext;

class TMasterConnector;

DECLARE_REFCOUNTED_CLASS(TBootstrap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
