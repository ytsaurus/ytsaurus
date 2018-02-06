#pragma once

#include <yt/server/scheduler/public.h>

#include <yt/ytlib/job_tracker_client/public.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

using NScheduler::TOperationId;
using NScheduler::TJobId;
using NScheduler::TJobResources;
using NScheduler::EAbortReason;
using NScheduler::EInterruptReason;
using NScheduler::TExecNodeDescriptorMap;
using NScheduler::TRefCountedExecNodeDescriptorMapPtr;
using NScheduler::EOperationType;
using NScheduler::EJobType;
using NScheduler::EJobState;
using NScheduler::TOperationSpecBasePtr;
// TODO(ignat): Move setting alerts from Scheduler to ControllerAgent.
using NScheduler::EOperationAlertType;

DECLARE_REFCOUNTED_STRUCT(TBriefJobStatistics)

DECLARE_REFCOUNTED_STRUCT(TControllerTransactions)

DECLARE_REFCOUNTED_STRUCT(IJobSizeConstraints)

DECLARE_REFCOUNTED_STRUCT(TScheduleJobStatistics)

DECLARE_REFCOUNTED_STRUCT(IOperationControllerSchedulerHost)
DECLARE_REFCOUNTED_STRUCT(IOperationControllerSnapshotBuilderHost)

DECLARE_REFCOUNTED_CLASS(TIntermediateChunkScraper)
DECLARE_REFCOUNTED_CLASS(TIntermediateChunkScraperConfig)

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

DECLARE_REFCOUNTED_CLASS(TJobSplitterConfig)
DECLARE_REFCOUNTED_CLASS(TJobSizeAdjusterConfig)
DECLARE_REFCOUNTED_CLASS(TOperationAlertsConfig)
DECLARE_REFCOUNTED_CLASS(TTestingOptions)
DECLARE_REFCOUNTED_CLASS(TSuspiciousJobsOptions)

DECLARE_REFCOUNTED_CLASS(TControllerAgent)
DECLARE_REFCOUNTED_CLASS(TControllerAgentConfig)

DECLARE_REFCOUNTED_STRUCT(IOperationControllerHost)
DECLARE_REFCOUNTED_STRUCT(IOperationController)

DECLARE_REFCOUNTED_CLASS(TOperationControllerHost)

DECLARE_REFCOUNTED_CLASS(TOperation)
using TOperationIdToOperationMap = THashMap<TOperationId, TOperationPtr>;

DECLARE_REFCOUNTED_STRUCT(TScheduleJobResult)

struct TOperationControllerInitializationResult;
struct TOperationControllerReviveResult;
struct TOperationControllerPrepareResult;

struct ISchedulingContext;

struct TJobStartDescriptor;

struct TJobSummary;
struct TCompletedJobSummary;
struct TAbortedJobSummary;
using TFailedJobSummary = TJobSummary;
struct TRunningJobSummary;

// XXX(babenko): move private
class TMasterConnector;

DECLARE_REFCOUNTED_CLASS(TProgressCounter)

class TDataFlowGraph;

using TIncarnationId = TGuid;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EScheduleJobFailReason,
    ((Unknown)                       ( 0))
    ((OperationNotRunning)           ( 1))
    ((NoPendingJobs)                 ( 2))
    ((NotEnoughChunkLists)           ( 3))
    ((NotEnoughResources)            ( 4))
    ((Timeout)                       ( 5))
    ((EmptyInput)                    ( 6))
    ((NoLocalJobs)                   ( 7))
    ((TaskDelayed)                   ( 8))
    ((NoCandidateTasks)              ( 9))
    ((ResourceOvercommit)            (10))
    ((TaskRefusal)                   (11))
    ((JobSpecThrottling)             (12))
    ((IntermediateChunkLimitExceeded)(13))
    ((DataBalancingViolation)        (14))
    ((UnknownNode)                   (15))
    ((UnknownOperation)              (16))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
