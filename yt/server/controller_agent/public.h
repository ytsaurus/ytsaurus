#pragma once

#include <yt/server/lib/controller_agent/public.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TOperationDescriptor;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EOperationControllerQueue,
    (Default)
    (GetJobSpec)
    (BuildJobSpec)
    (ScheduleJob)
    (ScheduleJobAndBuildJobSpec)
    (JobEvents)
);

DECLARE_REFCOUNTED_CLASS(TDataFlowGraph)

DECLARE_REFCOUNTED_STRUCT(TBriefJobStatistics)

DECLARE_REFCOUNTED_STRUCT(IJobSizeConstraints)

DECLARE_REFCOUNTED_STRUCT(TScheduleJobStatistics)

DECLARE_REFCOUNTED_STRUCT(IOperationControllerSchedulerHost)
DECLARE_REFCOUNTED_STRUCT(IOperationControllerSnapshotBuilderHost)

DECLARE_REFCOUNTED_CLASS(TIntermediateChunkScraper)
DECLARE_REFCOUNTED_CLASS(TIntermediateChunkScraperConfig)

DECLARE_REFCOUNTED_CLASS(TDataBalancer)
DECLARE_REFCOUNTED_CLASS(TDataBalancerOptions)

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
DECLARE_REFCOUNTED_CLASS(TInputChunkMapping)

DECLARE_REFCOUNTED_CLASS(TJobSizeAdjusterConfig)
DECLARE_REFCOUNTED_CLASS(TOperationAlertsConfig)
DECLARE_REFCOUNTED_CLASS(TTestingOptions)
DECLARE_REFCOUNTED_CLASS(TSuspiciousJobsOptions)

DECLARE_REFCOUNTED_CLASS(TControllerAgent)
DECLARE_REFCOUNTED_CLASS(TControllerAgentConfig)
DECLARE_REFCOUNTED_CLASS(TControllerAgentBootstrapConfig)

DECLARE_REFCOUNTED_STRUCT(IOperationControllerHost)
DECLARE_REFCOUNTED_STRUCT(IOperationController)

DECLARE_REFCOUNTED_CLASS(TOperationControllerHost)

DECLARE_REFCOUNTED_CLASS(TOperation)
using TOperationIdToOperationMap = THashMap<TOperationId, TOperationPtr>;

DECLARE_REFCOUNTED_STRUCT(TScheduleJobResult)

struct TControllerTransactionIds;
struct TOperationControllerInitializeAttributes;
struct TOperationControllerInitializeResult;
struct TOperationControllerReviveResult;
struct TOperationControllerPrepareResult;

struct ISchedulingContext;

struct TJobStartDescriptor;

struct TJobSummary;
struct TCompletedJobSummary;
struct TAbortedJobSummary;
using TFailedJobSummary = TJobSummary;
struct TRunningJobSummary;

class TMasterConnector;
class TBootstrap;

DECLARE_REFCOUNTED_CLASS(TProgressCounter)

class TMemoryTagQueue;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
