#pragma once

#include <yt/ytlib/job_tracker_client/public.h>

#include <yt/ytlib/scheduler/public.h>

#include <yt/core/actions/callback.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

using NJobTrackerClient::TJobId;
using NJobTrackerClient::EJobType;
using NJobTrackerClient::EJobState;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSchedulerService)

DECLARE_REFCOUNTED_CLASS(TOperation)

DECLARE_REFCOUNTED_CLASS(TJob)

DECLARE_REFCOUNTED_STRUCT(TJobStartRequest)

using TJobList = std::list<TJobPtr>;
using TJobSpecBuilder = TCallback<void(NJobTrackerClient::NProto::TJobSpec* jobSpec)>;

class TJobResources;

struct TExecNodeDescriptor;
DECLARE_REFCOUNTED_CLASS(TExecNode)

DECLARE_REFCOUNTED_CLASS(TFairShareStrategyConfig)
DECLARE_REFCOUNTED_CLASS(TEventLogConfig)

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

DECLARE_REFCOUNTED_CLASS(TSchedulerConfig)
DECLARE_REFCOUNTED_CLASS(TScheduler)

struct IEventLogHost;

struct ISchedulerStrategy;
struct ISchedulerStrategyHost;

struct IOperationHost;
struct ISchedulingContext;

DECLARE_REFCOUNTED_STRUCT(IOperationController);

class TMasterConnector;

using TRefCountedJobResult = TRefCountedProto<NJobTrackerClient::NProto::TJobResult>;
DECLARE_REFCOUNTED_TYPE(TRefCountedJobResult);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
