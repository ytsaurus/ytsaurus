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
using NScheduler::TSchedulerConfigPtr;
using NScheduler::TExecNodeDescriptorListPtr;
using NScheduler::EOperationType;
using NScheduler::EJobType;
using NScheduler::EJobState;
using NScheduler::TOperation;
using NScheduler::TOperationSpecBasePtr;
using NScheduler::TOperationOptionsPtr;
// TODO(ignat): Move setting alerts from Scheduler to ControllerAgent.
using NScheduler::EOperationAlertType;

DECLARE_REFCOUNTED_STRUCT(TBriefJobStatistics)

DECLARE_REFCOUNTED_STRUCT(TControllerTransactions)

DECLARE_REFCOUNTED_STRUCT(IJobSizeConstraints)

DECLARE_REFCOUNTED_STRUCT(TScheduleJobStatistics)

DECLARE_REFCOUNTED_STRUCT(IOperationControllerStrategyHost)
DECLARE_REFCOUNTED_STRUCT(IOperationControllerSchedulerHost)

DECLARE_REFCOUNTED_CLASS(TIntermediateChunkScraper)
DECLARE_REFCOUNTED_CLASS(TIntermediateChunkScraperConfig)

DECLARE_REFCOUNTED_CLASS(TInputChunkMapping)
DECLARE_REFCOUNTED_CLASS(TJobSizeAdjusterConfig)

DECLARE_REFCOUNTED_CLASS(TControllerAgent)

DECLARE_REFCOUNTED_CLASS(TProgressCounter)

class TDataFlowGraph;

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
