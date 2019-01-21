#pragma once

#include <yt/ytlib/job_tracker_client/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/scheduler/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

using NJobTrackerClient::TJobId;
using NJobTrackerClient::EJobType;
using NJobTrackerClient::EJobState;

using NJobTrackerClient::NProto::TJobResult;
using NJobTrackerClient::NProto::TJobStatus;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESchedulerAlertType,
    (UpdatePools)
    (UpdateConfig)
    (UpdateFairShare)
    (UpdateArchiveVersion)
    (SyncClusterDirectory)
    (UnrecognizedConfigOptions)
    (OperationsArchivation)
    (JobsArchivation)
    (UpdateNodesFailed)
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
    (OperationPending)
    (OperationCompletedByUserRequest)
    (OperationBannedInTentativeTree)
);

DEFINE_ENUM(EAgentToSchedulerOperationEventType,
    ((Completed)                (0))
    ((Suspended)                (1))
    ((Failed)                   (2))
    ((Aborted)                  (3))
    ((BannedInTentativeTree)    (4))
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

DEFINE_ENUM(EControllerAgentPickStrategy,
    (Random)
    (MemoryUsageBalanced)
);

DEFINE_ENUM(EAccessType,
    (Ownership)
    (IntermediateData)
);

DEFINE_ENUM(ENodeState,
    (Unknown)
    (Offline)
    (Online)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TFairShareStrategyConfig)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyOperationControllerConfig)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyTreeConfig)
DECLARE_REFCOUNTED_CLASS(TTestingOptions)
DECLARE_REFCOUNTED_CLASS(TOperationsCleanerConfig)
DECLARE_REFCOUNTED_CLASS(TControllerAgentTrackerConfig)
DECLARE_REFCOUNTED_CLASS(TSchedulerConfig)
DECLARE_REFCOUNTED_CLASS(TSchedulerBootstrapConfig)

struct TExecNodeDescriptor;
using TExecNodeDescriptorMap = THashMap<NNodeTrackerClient::TNodeId, TExecNodeDescriptor>;
DECLARE_REFCOUNTED_STRUCT(TRefCountedExecNodeDescriptorMap);

class TSchedulingTagFilter;

using TPoolTreeToSchedulingTagFilter = THashMap<TString, TSchedulingTagFilter>;

using TIncarnationId = TGuid;
using TAgentId = TString;

////////////////////////////////////////////////////////////////////////////////

extern const TString RootPoolName;
extern const TString DefaultTreeAttributeName;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
