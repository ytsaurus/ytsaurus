#pragma once

#include <yt/ytlib/job_tracker_client/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/scheduler/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TSchedulerToAgentJobEvent;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using NJobTrackerClient::TJobId;
using NJobTrackerClient::EJobType;
using NJobTrackerClient::EJobState;
using NJobTrackerClient::EJobPhase;

using NJobTrackerClient::NProto::TJobResult;
using NJobTrackerClient::NProto::TJobStatus;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESchedulerAlertType,
    ((UpdatePools)                                  (0))
    ((UpdateConfig)                                 (1))
    ((UpdateFairShare)                              (2))
    ((UpdateArchiveVersion)                         (3))
    ((SyncClusterDirectory)                         (4))
    ((UnrecognizedConfigOptions)                    (5))
    ((OperationsArchivation)                        (6))
    ((JobsArchivation)                              (7))
    ((UpdateNodesFailed)                            (8))
    ((NodesWithoutPoolTree)                         (9))
);

DEFINE_ENUM(EOperationAlertType,
    ((UnusedTmpfsSpace)                             (0))
    ((LostIntermediateChunks)                       (1))
    ((LostInputChunks)                              (2))
    ((IntermediateDataSkew)                         (3))
    ((LongAbortedJobs)                              (4))
    ((ExcessiveDiskUsage)                           (5))
    ((ShortJobsDuration)                            (6))
    ((OperationSuspended)                           (7))
    ((ExcessiveJobSpecThrottling)                   (8))
    ((ScheduleJobTimedOut)                          (9))
    ((InvalidAcl)                                  (10))
    ((LowCpuUsage)                                 (11))
    ((OperationTooLong)                            (12))
    ((OperationPending)                            (13))
    ((OperationCompletedByUserRequest)             (14))
    ((OperationBannedInTentativeTree)              (15))
    ((OwnersInSpecIgnored)                         (16))
    ((OmittedInaccesibleColumnsInInputTables)      (17))
    ((LegacyLivePreviewSuppressed)                 (18))
    ((LowGpuUsage)                                 (19))
    ((HighQueueAverageWaitTime)                    (20))
    ((AutoMergeDisabled)                           (21))
);

DEFINE_ENUM(EAgentToSchedulerOperationEventType,
    ((Completed)                (0))
    ((Suspended)                (1))
    ((Failed)                   (2))
    ((Aborted)                  (3))
    ((BannedInTentativeTree)    (4))
    ((InitializationFinished)   (5))
    ((PreparationFinished)      (6))
    ((MaterializationFinished)  (7))
    ((RevivalFinished)          (8))
    ((CommitFinished)           (9))
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
    (SchedulerProfiling)
    (Operation)
    (AgentTracker)
    (NodeTracker)
    (OperationsCleaner)
    (FairShareStrategy)
);

DEFINE_ENUM(EControllerAgentPickStrategy,
    (Random)
    (MemoryUsageBalanced)
);

DEFINE_ENUM(ENodeState,
    (Unknown)
    (Offline)
    (Online)
);

static constexpr int MaxNodeShardCount = 32;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TStrategyTestingOptions)
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

DECLARE_REFCOUNTED_STRUCT(TControllerScheduleJobResult)

struct TJobStartDescriptor;
struct TOperationControllerInitializeAttributes;

using TIncarnationId = TGuid;
using TAgentId = TString;

////////////////////////////////////////////////////////////////////////////////

extern const TString RootPoolName;
extern const TString DefaultTreeAttributeName;
extern const TString PoolTreesRootCypressPath;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
