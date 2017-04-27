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
using NScheduler::TIntermediateChunkScraperConfigPtr;
using NScheduler::TOperation;
using NScheduler::TOperationSpecBasePtr;
using NScheduler::TOperationOptionsPtr;
// TODO(ignat): Move setting alerts from Scheduler to ControllerAgent.
using NScheduler::EOperationAlertType;

struct IOperationHost;
struct TOperationControllerInitializeResult;

DECLARE_REFCOUNTED_STRUCT(TBriefJobStatistics)

DECLARE_REFCOUNTED_STRUCT(TControllerTransactions)

DECLARE_REFCOUNTED_STRUCT(TChunkStripeList)

DECLARE_REFCOUNTED_STRUCT(IChunkSliceFetcherFactory)

DECLARE_REFCOUNTED_STRUCT(IJobSizeConstraints)

DECLARE_REFCOUNTED_STRUCT(TScheduleJobStatistics)

DECLARE_REFCOUNTED_STRUCT(IOperationController)

DECLARE_REFCOUNTED_CLASS(TIntermediateChunkScraper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
