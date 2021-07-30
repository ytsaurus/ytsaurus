#pragma once

#include <yt/yt/ytlib/job_tracker_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/scheduler/public.h>

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
    ((SchedulerCannotConnect)                       (10))
    ((InvalidPoolTreeTemplateConfigSet)             (11))
    ((TooFewControllerAgentsAlive)                  (12))
    ((UpdateDefaultUserPoolsFailed)                 (13))
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
    ((OmittedInaccessibleColumnsInInputTables)     (17))
    ((LegacyLivePreviewSuppressed)                 (18))
    ((LowGpuUsage)                                 (19))
    ((HighQueueAverageWaitTime)                    (20))
    ((AutoMergeDisabled)                           (21))
    ((InvalidatedJobsFound)                        (23))
    ((NoTablesWithEnabledDynamicStoreRead)         (24))
    ((UnusedMemory)                                (25))
    ((UserJobMonitoringLimited)                    (26))
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
    (CommonPeriodicActivity)
    (OperationsPeriodicActivity)
    (NodesPeriodicActivity)
    (SchedulerProfiling)
    (Operation)
    (AgentTracker)
    (NodeTracker)
    (OperationsCleaner)
    (FairShareStrategy)
    (EventLog)
    (Metering)
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

DEFINE_ENUM(ESegmentedSchedulingMode,
    (Disabled)
    (LargeGpu)
);

DEFINE_ENUM(ESchedulingSegmentDataCenterAssignmentHeuristic,
    (MaxRemainingCapacity)
    (MinRemainingFeasibleCapacity)
);

static constexpr int MaxNodeShardCount = 32;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TStrategyTestingOptions)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyConfig)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyOperationControllerConfig)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyControllerThrottling)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyTreeConfig)
DECLARE_REFCOUNTED_CLASS(TTestingOptions)
DECLARE_REFCOUNTED_CLASS(TOperationsCleanerConfig)
DECLARE_REFCOUNTED_CLASS(TControllerAgentTrackerConfig)
DECLARE_REFCOUNTED_CLASS(TResourceMeteringConfig)
DECLARE_REFCOUNTED_CLASS(TPoolTreesTemplateConfig)
DECLARE_REFCOUNTED_CLASS(TSchedulerConfig)
DECLARE_REFCOUNTED_CLASS(TSchedulerBootstrapConfig)
DECLARE_REFCOUNTED_CLASS(TSchedulerIntegralGuaranteesConfig)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategySchedulingSegmentsConfig)

DECLARE_REFCOUNTED_STRUCT(TExperimentEffectConfig)
DECLARE_REFCOUNTED_STRUCT(TExperimentGroupConfig)
DECLARE_REFCOUNTED_STRUCT(TExperimentConfig)
DECLARE_REFCOUNTED_STRUCT(TExperimentAssignment)

struct TExecNodeDescriptor;
using TExecNodeDescriptorMap = THashMap<NNodeTrackerClient::TNodeId, TExecNodeDescriptor>;
DECLARE_REFCOUNTED_STRUCT(TRefCountedExecNodeDescriptorMap);

class TSchedulingTagFilter;

DECLARE_REFCOUNTED_STRUCT(TControllerScheduleJobResult)

struct TJobStartDescriptor;
struct TOperationControllerInitializeAttributes;

using TControllerEpoch = int;
using TIncarnationId = TGuid;
using TAgentId = TString;

static constexpr TControllerEpoch InvalidControllerEpoch = -1;

////////////////////////////////////////////////////////////////////////////////

extern const TString CommittedAttribute;

////////////////////////////////////////////////////////////////////////////////

extern const TString DefaultTreeAttributeName;
extern const TString TreeConfigAttributeName;
extern const TString PoolTreesRootCypressPath;
extern const TString StrategyStatePath;
extern const TString SegmentsStatePath;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
