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

DECLARE_REFCOUNTED_STRUCT(TScheduleJobResult)

struct TJobStartRequest;

using TJobList = std::list<TJobPtr>;

struct TUpdatedJob;
struct TCompletedJob;

struct TExecNodeDescriptor;
DECLARE_REFCOUNTED_STRUCT(TExecNodeDescriptorList);

DECLARE_REFCOUNTED_CLASS(TExecNode)

DECLARE_REFCOUNTED_CLASS(TFairShareStrategyConfig)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyOperationControllerConfig)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyTreeConfig)

DECLARE_REFCOUNTED_CLASS(TSchedulerConfig)
DECLARE_REFCOUNTED_CLASS(TScheduler)

struct IEventLogHost;

DECLARE_REFCOUNTED_STRUCT(ISchedulerStrategy)
struct ISchedulerStrategyHost;

DECLARE_REFCOUNTED_STRUCT(ISchedulingContext)

class TMasterConnector;

using NJobTrackerClient::NProto::TJobResult;
using NJobTrackerClient::NProto::TJobStatus;

struct TJobSummary;
struct TCompletedJobSummary;
struct TAbortedJobSummary;
struct TRunningJobSummary;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESchedulerAlertType,
    (UpdatePools)
    (UpdateConfig)
    (UpdateFairShare)
    (UpdateArchiveVersion)
    (SyncClusterDirectory)
    (UnrecognizedConfigOptions)
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EOperationAlertType,
    (UnusedTmpfsSpace)
    (LostIntermediateChunks)
    (LostInputChunks)
    (IntermediateDataSkew)
    (LongAbortedJobs)
    (ExcessiveDiskUsage)
    (ShortJobsDuration)
    (OperationSuspended)
    (ExcessiveJobSpecThrottling)
    (ScheduleJobTimedOut)
    (SlotIndexCollision)
    (InvalidAcl)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
