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

DECLARE_REFCOUNTED_CLASS(TOperationRuntimeData)

DECLARE_REFCOUNTED_CLASS(TOperation)
DECLARE_REFCOUNTED_CLASS(TJob)

DECLARE_REFCOUNTED_STRUCT(TScheduleJobResult)

struct TJobStartRequest;

using TJobList = std::list<TJobPtr>;

struct TUpdatedJob;
struct TCompletedJob;

struct TExecNodeDescriptor;
DECLARE_REFCOUNTED_STRUCT(TExecNodeDescriptorList);

DECLARE_REFCOUNTED_CLASS(TNodeShard)
DECLARE_REFCOUNTED_CLASS(TExecNode)
DECLARE_REFCOUNTED_CLASS(TControllerAgent)

DECLARE_REFCOUNTED_CLASS(TFairShareStrategyConfig)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyOperationControllerConfig)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyTreeConfig)
DECLARE_REFCOUNTED_CLASS(TSchedulerConfig)

DECLARE_REFCOUNTED_CLASS(TScheduler)
DECLARE_REFCOUNTED_CLASS(TControllerAgentTracker)

struct IEventLogHost;

DECLARE_REFCOUNTED_STRUCT(ISchedulerStrategy)
struct ISchedulerStrategyHost;
struct IOperationStrategyHost;

DECLARE_REFCOUNTED_STRUCT(ISchedulingContext)
DECLARE_REFCOUNTED_STRUCT(IOperationController)

// XXX(babenko): move to private
class TMasterConnector;

using NJobTrackerClient::NProto::TJobResult;
using NJobTrackerClient::NProto::TJobStatus;

class TSchedulingTagFilter;

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

DEFINE_ENUM(EAgentToSchedulerOperationEventType,
    ((Completed)                (0))
    ((Suspended)                (1))
    ((Failed)                   (2))
    ((Aborted)                  (3))
);

DEFINE_ENUM(EAgentToSchedulerJobEventType,
    ((Interrupted) (0))
    ((Aborted)     (1))
    ((Failed)      (2))
    ((Released)    (3))
);

DEFINE_ENUM(ESchedulerToAgentJobEventType,
    ((Started)   (0))
    ((Completed) (1))
    ((Failed)    (2))
    ((Aborted)   (3))
    ((Running)   (4))
);

DEFINE_ENUM(ESchedulerToAgentOperationEventType,
    ((Abandon)                    (0))
    ((UpdateMinNeededJobResources)(1))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
