#pragma once

#include <yt/ytlib/job_tracker_client/public.h>

#include <yt/ytlib/scheduler/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/actions/callback.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TSchedulerToAgentJobEvent;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using NJobTrackerClient::TJobId;
using NJobTrackerClient::EJobType;
using NJobTrackerClient::EJobState;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TOperationRuntimeData)

DECLARE_REFCOUNTED_CLASS(TOperation)
DECLARE_REFCOUNTED_CLASS(TJob)

using TJobList = std::list<TJobPtr>;

struct TUpdatedJob;
struct TFinishedJob;

struct TExecNodeDescriptor;
using TExecNodeDescriptorMap = THashMap<NNodeTrackerClient::TNodeId, TExecNodeDescriptor>;
DECLARE_REFCOUNTED_STRUCT(TRefCountedExecNodeDescriptorMap);

DECLARE_REFCOUNTED_CLASS(TNodeShard)
DECLARE_REFCOUNTED_CLASS(TExecNode)
DECLARE_REFCOUNTED_CLASS(TControllerAgent)

DECLARE_REFCOUNTED_CLASS(TFairShareStrategyConfig)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyOperationControllerConfig)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyTreeConfig)
DECLARE_REFCOUNTED_CLASS(TTestingOptions)
DECLARE_REFCOUNTED_CLASS(TOperationsCleanerConfig)
DECLARE_REFCOUNTED_CLASS(TSchedulerConfig)
DECLARE_REFCOUNTED_CLASS(TSchedulerBootstrapConfig)

DECLARE_REFCOUNTED_CLASS(TOperationsCleaner)

DECLARE_REFCOUNTED_CLASS(TScheduler)
DECLARE_REFCOUNTED_CLASS(TControllerAgentTracker)

struct IEventLogHost;

DECLARE_REFCOUNTED_STRUCT(ISchedulerStrategy)
struct ISchedulerStrategyHost;
struct IOperationStrategyHost;

DECLARE_REFCOUNTED_STRUCT(ISchedulingContext)
DECLARE_REFCOUNTED_STRUCT(IOperationControllerStrategyHost)
DECLARE_REFCOUNTED_STRUCT(IOperationController)

struct TOperationControllerInitializationResult;
struct TOperationControllerPrepareResult;
struct TOperationRevivalDescriptor;

class TMasterConnector;

using NJobTrackerClient::NProto::TJobResult;
using NJobTrackerClient::NProto::TJobStatus;

using TIncarnationId = TGuid;
using TAgentId = TString;

class TSchedulingTagFilter;

class TBootstrap;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESchedulerAlertType,
    (UpdatePools)
    (UpdateConfig)
    (UpdateFairShare)
    (UpdateArchiveVersion)
    (SyncClusterDirectory)
    (UnrecognizedConfigOptions)
);

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
    (InvalidAcl)
    (LowCpuUsage)
    (OperationTooLong)
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
    ((UpdateMinNeededJobResources)(0))
);

DEFINE_ENUM(EControlQueue,
    (Default)
    (UserRequest)
    (MasterConnector)
    (Orchid)
    (PeriodicActivity)
    (Operation)
    (AgentTracker)
    (NodeTracker)
    (OperationsCleaner)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
