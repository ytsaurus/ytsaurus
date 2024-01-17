#pragma once

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/scheduler/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

namespace NNode {

class TReqHeartbeat;
class TRspHeartbeat;

class TAllocationToAbort;

} // namespace NNode

class TAllocationStatus;

class TSchedulerToAgentAllocationEvent;

class TReqScheduleAllocationHeartbeat;
class TRspScheduleAllocationHeartbeat;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

// NB: Please keep the range of values small as this type
// is used as a key of TEnumIndexedVector.
DEFINE_ENUM(EAllocationState,
    ((Scheduled)  (0))
    ((Waiting)    (1))
    ((Running)    (2))
    ((Finishing)  (3))
    ((Finished)   (4))
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESchedulerAlertType,
    ((UpdatePools)                                  ( 0))
    ((UpdateConfig)                                 ( 1))
    ((UpdateFairShare)                              ( 2))
    ((UpdateArchiveVersion)                         ( 3))
    ((SyncClusterDirectory)                         ( 4))
    ((UnrecognizedConfigOptions)                    ( 5))
    ((OperationsArchivation)                        ( 6))
    ((JobsArchivation)                              ( 7))
    ((UpdateNodesFailed)                            ( 8))
    ((NodesWithoutPoolTree)                         ( 9))
    ((SchedulerCannotConnect)                       (10))
    ((InvalidPoolTreeTemplateConfigSet)             (11))
    ((TooFewControllerAgentsAlive)                  (12))
    ((UpdateUserToDefaultPoolMap)                   (13))
    ((OperationAlertArchivation)                    (14))
    ((ManageSchedulingSegments)                     (15))
    ((UpdateSsdPriorityPreemptionMedia)             (16))
    ((FoundNodesWithUnsupportedPreemption)          (17))
    ((ArchiveIsOutdated)                            (18))
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
    ((HighCpuWait)                                 (30))
    ((OperationTooLong)                            (12))
    ((OperationPending)                            (13))
    ((OperationCompletedByUserRequest)             (14))
    ((OperationBannedInTentativeTree)              (15))
    ((OwnersInSpecIgnored)                         (16))
    ((OmittedInaccessibleColumnsInInputTables)     (17))
    ((LegacyLivePreviewSuppressed)                 (18))
    ((LowGpuUsage)                                 (19))
    ((LowGpuPower)                                 (29))
    ((LowGpuSMUsage)                               (35))
    ((LowGpuPowerOnWindow)                         (32))
    ((HighQueueTotalTimeEstimate)                  (20))
    ((AutoMergeDisabled)                           (21))
    ((InvalidatedJobsFound)                        (23))
    ((NoTablesWithEnabledDynamicStoreRead)         (24))
    ((UnusedMemory)                                (25))
    ((UserJobMonitoringLimited)                    (26))
    ((MemoryOverconsumption)                       (27))
    ((InvalidControllerRuntimeData)                (28))
    ((CustomStatisticsLimitExceeded)               (31))
    ((BaseLayerProbeFailed)                        (33))
    ((MtnExperimentFailed)                         (34))
    ((NewPartitionsCountIsSignificantlyLarger)     (36))
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

DEFINE_ENUM(ESchedulerToAgentOperationEventType,
    ((UpdateMinNeededAllocationResources) (0))
);

DEFINE_ENUM(EControlQueue,
    (Default)
    (UserRequest)
    (MasterConnector)
    (StaticOrchid)
    (DynamicOrchid)
    (CommonPeriodicActivity)
    (OperationsPeriodicActivity)
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

DEFINE_ENUM(ESchedulingSegmentModuleAssignmentHeuristic,
    (MaxRemainingCapacity)
    (MinRemainingFeasibleCapacity)
);

DEFINE_ENUM(ESchedulingSegmentModulePreemptionHeuristic,
    (Greedy)
);

DEFINE_ENUM(ESchedulingSegmentModuleType,
    (DataCenter)
    (InfinibandCluster)
);

DEFINE_ENUM(EOperationPreemptionPriorityScope,
    (OperationOnly)
    (OperationAndAncestors)
);

static constexpr int MaxNodeShardCount = 64;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TStrategyTestingOptions)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyConfig)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyOperationControllerConfig)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyControllerThrottling)
DECLARE_REFCOUNTED_CLASS(TTreeTestingOptions)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategyTreeConfig)
DECLARE_REFCOUNTED_CLASS(TDelayConfig)
DECLARE_REFCOUNTED_CLASS(TTestingOptions)
DECLARE_REFCOUNTED_CLASS(TOperationsCleanerConfig)
DECLARE_REFCOUNTED_CLASS(TControllerAgentTrackerConfig)
DECLARE_REFCOUNTED_CLASS(TResourceMeteringConfig)
DECLARE_REFCOUNTED_CLASS(TPoolTreesTemplateConfig)
DECLARE_REFCOUNTED_CLASS(TSchedulerConfig)
DECLARE_REFCOUNTED_CLASS(TSchedulerBootstrapConfig)
DECLARE_REFCOUNTED_CLASS(TSchedulerIntegralGuaranteesConfig)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategySchedulingSegmentsConfig)
DECLARE_REFCOUNTED_CLASS(TFairShareStrategySsdPriorityPreemptionConfig)
DECLARE_REFCOUNTED_CLASS(TBatchOperationSchedulingConfig)

DECLARE_REFCOUNTED_STRUCT(TExperimentEffectConfig)
DECLARE_REFCOUNTED_STRUCT(TExperimentGroupConfig)
DECLARE_REFCOUNTED_STRUCT(TExperimentConfig)
DECLARE_REFCOUNTED_STRUCT(TExperimentAssignment)

DECLARE_REFCOUNTED_STRUCT(TExecNodeDescriptor)
using TExecNodeDescriptorMap = THashMap<NNodeTrackerClient::TNodeId, TExecNodeDescriptorPtr>;
DECLARE_REFCOUNTED_STRUCT(TRefCountedExecNodeDescriptorMap)

class TSchedulingTagFilter;

DECLARE_REFCOUNTED_STRUCT(TControllerScheduleAllocationResult)

struct TAllocationStartDescriptor;
struct TOperationControllerInitializeAttributes;

struct TPreemptedFor;

using TControllerEpoch = NControllerAgent::TControllerEpoch;
using TIncarnationId = NControllerAgent::TIncarnationId;
using TAgentId = NControllerAgent::TAgentId;
using TControllerAgentDescriptor = NControllerAgent::TControllerAgentDescriptor;

static constexpr TControllerEpoch InvalidControllerEpoch = TControllerEpoch(-1);

////////////////////////////////////////////////////////////////////////////////

extern const TString CommittedAttribute;

////////////////////////////////////////////////////////////////////////////////

extern const TString DefaultTreeAttributeName;
extern const TString TreeConfigAttributeName;
extern const TString IdAttributeName;
extern const TString ParentIdAttributeName;
extern const TString StrategyStatePath;
extern const TString OldSegmentsStatePath;
extern const TString LastMeteringLogTimePath;

////////////////////////////////////////////////////////////////////////////////

inline const TString ProfilingPoolTreeKey{"tree"};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
