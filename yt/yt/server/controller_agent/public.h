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

DECLARE_REFCOUNTED_STRUCT(IOperationControllerSchedulerHost)
DECLARE_REFCOUNTED_STRUCT(IOperationControllerSnapshotBuilderHost)

DECLARE_REFCOUNTED_CLASS(TIntermediateChunkScraper)
DECLARE_REFCOUNTED_STRUCT(TIntermediateChunkScraperConfig)

DECLARE_REFCOUNTED_CLASS(TDataBalancerOptions)

DECLARE_REFCOUNTED_CLASS(TUserJobOptions)

DECLARE_REFCOUNTED_CLASS(TOperationOptions)
DECLARE_REFCOUNTED_CLASS(TSimpleOperationOptions)
DECLARE_REFCOUNTED_CLASS(TMapOperationOptions)
DECLARE_REFCOUNTED_CLASS(TUnorderedMergeOperationOptions)
DECLARE_REFCOUNTED_CLASS(TOrderedMergeOperationOptions)
DECLARE_REFCOUNTED_CLASS(TSortedMergeOperationOptions)
DECLARE_REFCOUNTED_CLASS(TEraseOperationOptions)
DECLARE_REFCOUNTED_CLASS(TReduceOperationOptions)
DECLARE_REFCOUNTED_CLASS(TJoinReduceOperationOptions)
DECLARE_REFCOUNTED_CLASS(TSortOperationOptionsBase)
DECLARE_REFCOUNTED_CLASS(TSortOperationOptions)
DECLARE_REFCOUNTED_CLASS(TMapReduceOperationOptions)
DECLARE_REFCOUNTED_CLASS(TRemoteCopyOperationOptions)
DECLARE_REFCOUNTED_CLASS(TVanillaOperationOptions)

DECLARE_REFCOUNTED_STRUCT(TJobSplitterConfig)

DECLARE_REFCOUNTED_STRUCT(TZombieOperationOrchidsConfig)

DECLARE_REFCOUNTED_STRUCT(TAlertManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TLowGpuPowerUsageOnWindowConfig)
DECLARE_REFCOUNTED_CLASS(TTestingOptions)
DECLARE_REFCOUNTED_CLASS(TSuspiciousJobsOptions)

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
