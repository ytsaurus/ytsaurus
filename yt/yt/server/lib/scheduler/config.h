#pragma once

#include "public.h"
#include "scheduling_tag.h"
#include "scheduling_segment_map.h"
#include "structs.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/job_proxy/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/ytlib/event_log/public.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/re2/re2.h>

#include <yt/yt/library/program/public.h>

#include <yt/yt/library/server_program/config.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDeactivationReason,
    (IsNotAlive)
    (UnmatchedSchedulingTag)
    (IsNotEligibleForAggressivelyPreemptiveScheduling)
    (IsNotEligibleForPreemptiveScheduling)
    (IsNotEligibleForSsdAggressivelyPreemptiveScheduling)
    (IsNotEligibleForSsdPreemptiveScheduling)
    (IsNotEligibleForDefaultGpuFullHostPreemptiveScheduling)
    (ScheduleAllocationFailed)
    (NoBestLeafDescendant)
    (MinNeededResourcesUnsatisfied)
    (ResourceLimitsExceeded)
    (FairShareLimitsExceeded)
    (SaturatedInTentativeTree)
    (OperationDisabled)
    (BadPacking)
    (FairShareExceeded)
    (MaxConcurrentScheduleAllocationCallsPerNodeShardViolated)
    (MaxConcurrentScheduleAllocationExecDurationPerNodeShardViolated)
    (RecentScheduleAllocationFailed)
    (IncompatibleSchedulingSegment)
    (NoAvailableDemand)
    (RegularAllocationOnSsdNodeForbidden)
);

////////////////////////////////////////////////////////////////////////////////

struct TStrategyTestingOptions
    : public NYTree::TYsonStruct
{
    // Testing option that enables sleeping during strategy update.
    std::optional<TDuration> DelayInsideFairShareUpdate;

    REGISTER_YSON_STRUCT(TStrategyTestingOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStrategyTestingOptions)

////////////////////////////////////////////////////////////////////////////////

class TStrategyControllerThrottling
    : public virtual NYTree::TYsonStruct
{
public:
    TDuration ScheduleAllocationStartBackoffTime;
    TDuration ScheduleAllocationMaxBackoffTime;
    double ScheduleAllocationBackoffMultiplier;

    REGISTER_YSON_STRUCT(TStrategyControllerThrottling);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStrategyControllerThrottling)

////////////////////////////////////////////////////////////////////////////////

// TODO(ignat): move it to subconfig.
struct TStrategyOperationControllerConfig
    : public virtual NYTree::TYsonStruct
{
    //! Limits on the number and total duration of concurrent schedule allocation calls to a single controller.
    int MaxConcurrentControllerScheduleAllocationCalls;
    TDuration MaxConcurrentControllerScheduleAllocationExecDuration;

    //! Enable throttling of total duration of concurrent schedule allocation calls.
    bool EnableConcurrentScheduleAllocationExecDurationThrottling;

    //! How much times averaged MaxConcurrentControllerScheduleAllocationCalls can be exceeded on each NodeShard.
    double ConcurrentControllerScheduleAllocationCallsRegularization;

    //! Maximum allowed time for single allocation scheduling.
    TDuration ScheduleAllocationTimeLimit;

    //! Backoff time after controller schedule allocation failure.
    TDuration ScheduleAllocationFailBackoffTime;

    //! Configuration of schedule allocation backoffs in case of throttling from controller.
    TStrategyControllerThrottlingPtr ControllerThrottling;

    //! Timeout after which "schedule allocation timed out" alert is expired and unset.
    TDuration ScheduleAllocationTimeoutAlertResetTime;

    //! Timeout for allocation scheduling in strategy.
    TDuration ScheduleAllocationsTimeout;

    //! Schedule allocation that longer this duration will be logged.
    TDuration LongScheduleAllocationLoggingThreshold;

    REGISTER_YSON_STRUCT(TStrategyOperationControllerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStrategyOperationControllerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerIntegralGuaranteesConfig
    : public NYTree::TYsonStruct
{
    TDuration SmoothPeriod;

    TDuration PoolCapacitySaturationPeriod;

    double RelaxedShareMultiplierLimit;

    REGISTER_YSON_STRUCT(TSchedulerIntegralGuaranteesConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerIntegralGuaranteesConfig)

////////////////////////////////////////////////////////////////////////////////

struct TModuleShareAndNetworkPriority
    : public NYTree::TYsonStructLite
{
    double ModuleShare;

    TNetworkPriority NetworkPriority;

    static const TNetworkPriority MinNetworkPriority = 0;
    static const TNetworkPriority MaxNetworkPriority = 15;

    REGISTER_YSON_STRUCT_LITE(TModuleShareAndNetworkPriority);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TStrategySchedulingSegmentsConfig
    : public NYTree::TYsonStruct
{
    ESegmentedSchedulingMode Mode;

    TSegmentToResourceAmount ReserveFairResourceAmount;

    TDuration InitializationTimeout;

    TDuration ManagePeriod;

    TDuration UnsatisfiedSegmentsRebalancingTimeout;

    TDuration ModuleReconsiderationTimeout;

    THashSet<std::string> DataCenters;

    THashSet<std::string> InfinibandClusters;

    ESchedulingSegmentModuleAssignmentHeuristic ModuleAssignmentHeuristic;

    ESchedulingSegmentModulePreemptionHeuristic ModulePreemptionHeuristic;

    ESchedulingSegmentModuleType ModuleType;

    bool EnableInfinibandClusterTagValidation;

    bool AllowOnlyGangOperationsInLargeSegment;

    bool EnableDetailedLogs;

    bool EnableModuleResetOnZeroFairShareAndUsage;

    TDuration PriorityModuleAssignmentTimeout;

    std::optional<double> ModuleOversatisfactionThreshold;

    bool ForceIncompatibleSegmentPreemption;

    std::vector<TModuleShareAndNetworkPriority> ModuleShareToNetworkPriority;

    const THashSet<std::string>& GetModules() const;

    REGISTER_YSON_STRUCT(TStrategySchedulingSegmentsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStrategySchedulingSegmentsConfig)

////////////////////////////////////////////////////////////////////////////////

struct TGpuSchedulingPolicyConfig
    : public NYTree::TYsonStruct
{
    EGpuSchedulingPolicyMode Mode;

    TDuration PlanUpdatePeriod;

    TDuration ModuleReconsiderationTimeout;

    // TODO(eshcherbin): Rework how modules are configured.
    ESchedulingSegmentModuleType ModuleType;

    THashSet<std::string> Modules;

    TDuration PriorityModuleBindingTimeout;

    TDuration FullHostAggressivePreemptionTimeout;

    TDuration MinAssignmentPreemptibleDuration;

    TDuration InitializationTimeout;

    REGISTER_YSON_STRUCT(TGpuSchedulingPolicyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGpuSchedulingPolicyConfig)

////////////////////////////////////////////////////////////////////////////////

struct TStrategySsdPriorityPreemptionConfig
    : public NYTree::TYsonStruct
{
    bool Enable;

    TSchedulingTagFilter NodeTagFilter;

    std::vector<std::string> MediumNames;

    REGISTER_YSON_STRUCT(TStrategySsdPriorityPreemptionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStrategySsdPriorityPreemptionConfig)

////////////////////////////////////////////////////////////////////////////////

struct TStrategyDefaultGpuFullHostPreemptionConfig
    : public NYTree::TYsonStruct
{
    bool Enable;

    double MaxPreemptionPenalty;

    TDuration Timeout;

    REGISTER_YSON_STRUCT(TStrategyDefaultGpuFullHostPreemptionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStrategyDefaultGpuFullHostPreemptionConfig)

////////////////////////////////////////////////////////////////////////////////

struct TBatchOperationSchedulingConfig
    : public NYTree::TYsonStruct
{
    int BatchSize;

    TJobResourcesConfigPtr FallbackMinSpareAllocationResources;

    REGISTER_YSON_STRUCT(TBatchOperationSchedulingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBatchOperationSchedulingConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTreeTestingOptions
    : public NYTree::TYsonStruct
{
    TDelayConfigPtr DelayInsideFairShareUpdate;

    TDelayConfigPtr DelayInsidePoolPermissionsValidation;

    std::optional<TDuration> ResourceTreeInitializeResourceUsageDelay;
    std::optional<TDuration> ResourceTreeReleaseResourcesRandomDelay;
    std::optional<TDuration> ResourceTreeIncreaseLocalResourceUsagePrecommitRandomDelay;
    std::optional<TDuration> ResourceTreeRevertResourceUsagePrecommitRandomDelay;

    REGISTER_YSON_STRUCT(TTreeTestingOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTreeTestingOptions)

////////////////////////////////////////////////////////////////////////////////

struct TStrategyTreeConfig
    : virtual public NYTree::TYsonStruct
{
    // Specifies nodes that are served by this tree.
    TSchedulingTagFilter NodeTagFilter;

    bool EnableUnrecognizedAlert;

    // The following settings can be overridden in operation spec.
    TDuration FairShareStarvationTimeout;
    TDuration FairShareAggressiveStarvationTimeout;
    double FairShareStarvationTolerance;

    bool EnableAggressiveStarvation;

    //! Any operation which resource usage is not greater than this cannot be preempted.
    TJobResourcesConfigPtr NonPreemptibleResourceUsageThreshold;

    //! Limit on number of operations in pool.
    int MaxOperationCountPerPool;
    int MaxRunningOperationCountPerPool;

    //! If enabled, pools will be able to starve and provoke preemption.
    bool EnablePoolStarvation;

    //! Default parent pool for operations with unknown pool.
    TString DefaultParentPool;
    //! Forbid immediate operations in root.
    bool ForbidImmediateOperationsInRoot;

    // Preemption timeout for operations with small number of allocations will be
    // discounted proportionally to this coefficient.
    double AllocationCountPreemptionTimeoutCoefficient;

    //! Thresholds to partition allocations of operation
    //! to preemptible, aggressively preemptible and non-preemptible lists.
    double PreemptionSatisfactionThreshold;
    double AggressivePreemptionSatisfactionThreshold;

    //! Backoff for printing tree scheduling info in heartbeat.
    TDuration HeartbeatTreeSchedulingInfoLogBackoff;

    //! Maximum number of ephemeral pools that can be created by user.
    int MaxEphemeralPoolsPerUser;

    //! If update of preemptible lists of operation takes more than that duration
    //! then this event will be logged.
    TDuration UpdatePreemptibleListDurationLoggingThreshold;

    //! Enables profiling strategy attributes for operations.
    bool EnableOperationsProfiling;

    NRe2::TRe2Ptr CustomProfilingTagFilter;

    //! Limit on number of operations in tree.
    int MaxRunningOperationCount;
    int MaxOperationCount;

    //! Duration after scheduler restart, during which total resource limits might not be stable yet, because nodes are still reconnecting.
    TDuration NodeReconnectionTimeout;

    //! Backoff for scheduling with preemption on the node (it is need to decrease number of calls of PrescheduleAllocation).
    TDuration PreemptiveSchedulingBackoff;

    //! Period of ban from the moment of operation saturation in tentative tree.
    TDuration TentativeTreeSaturationDeactivationPeriod;

    //! Enables infer of weight from strong guarantee share (if weight is not implicitly specified);
    //! inferred weight is this number multiplied by dominant strong guarantee share.
    std::optional<double> InferWeightFromGuaranteesShareMultiplier;

    TStrategyPackingConfigPtr Packing;

    //! List of operation types which should not be run in that tree as tentative.
    std::optional<THashSet<EOperationType>> NonTentativeOperationTypes;

    //! Period of best allocation ratio update for operations.
    TDuration BestAllocationShareUpdatePeriod;

    bool EnableByUserProfiling;

    TSchedulerIntegralGuaranteesConfigPtr IntegralGuarantees;

    bool EnableResourceTreeStructureLockProfiling;
    bool EnableResourceTreeUsageLockProfiling;

    bool AllowPreemptionFromStarvingOperations;

    // COMPAT(eshcherbin): Remove after 25.4 is created.
    bool PreemptionCheckStarvation;
    bool PreemptionCheckSatisfaction;

    // Allocation preemption timeout.
    TDuration AllocationPreemptionTimeout;

    // Allocation graceful preemption timeout.
    TDuration AllocationGracefulPreemptionTimeout;

    TStrategySchedulingSegmentsConfigPtr SchedulingSegments;

    bool EnablePoolsVectorProfiling;
    bool EnableOperationsVectorProfiling;
    bool SparsifyFairShareProfiling;

    bool EnableLimitingAncestorCheck;

    THashSet<EJobResourceType> ProfiledPoolResources;
    THashSet<EJobResourceType> ProfiledOperationResources;

    std::optional<TDuration> WaitingForResourcesOnNodeTimeout;

    // If pool has at least #MinChildHeapSize children,
    // then it uses heap for maintaining best active child.
    int MinChildHeapSize;

    EJobResourceType MainResource;

    THashMap<TString, TString> MeteringTags;

    THashMap<TString, NYTree::INodePtr> PoolConfigPresets;

    bool EnableFairShareTruncationInFifoPool;

    // COMPAT(eshcherbin): Remove after 25.4 is created.
    bool EnableConditionalPreemption;

    TDuration AllowedResourceUsageStaleness;

    //! How often to update allocation preemption statuses snapshot.
    TDuration CachedAllocationPreemptionStatusesUpdatePeriod;

    bool ShouldDistributeFreeVolumeAmongChildren;

    bool UseUserDefaultParentPoolMap;

    int MaxEventLogPoolBatchSize;
    int MaxEventLogOperationBatchSize;

    TDuration AccumulatedResourceDistributionUpdatePeriod;

    bool AllowAggressivePreemptionForGangOperations;

    TStrategySsdPriorityPreemptionConfigPtr SsdPriorityPreemption;
    TStrategyDefaultGpuFullHostPreemptionConfigPtr DefaultGpuFullHostPreemption;

    //! Enables profiling of scheduled and preempted resources in strategy.
    bool EnableScheduledAndPreemptedResourcesProfiling;

    //! Limit to the number of schedulable elements in FIFO pools.
    std::optional<int> MaxSchedulableElementCountInFifoPool;

    //! Check that operation is alive in preschedule phase.
    bool CheckOperationForLivenessInPreschedule;

    TTreeTestingOptionsPtr TestingOptions;

    EOperationPreemptionPriorityScope SchedulingPreemptionPriorityScope;

    TDuration RunningAllocationStatisticsUpdatePeriod;

    TBatchOperationSchedulingConfigPtr BatchOperationScheduling;

    THistogramDigestConfigPtr PerPoolSatisfactionDigest;
    std::vector<double> PerPoolSatisfactionProfilingQuantiles;

    bool EnableGuaranteePriorityScheduling;

    bool EnableStepFunctionForGangOperations;
    bool EnableImprovedFairShareByFitFactorComputation;
    bool EnableImprovedFairShareByFitFactorComputationDistributionGap;

    TJobResourcesConfigPtr MinJobResourceLimits;
    TJobResourcesConfigPtr MaxJobResourceLimits;
    TJobResourcesWithDiskConfigPtr GuaranteedJobResources;
    TJobResourcesConfigPtr MinNodeResourceLimits;

    TDuration MinNodeResourceLimitsCheckPeriod;

    bool AllowGangOperationsOnlyInFifoPools;

    // TODO(eshcherbin): Remove when 24.2 is finalized.
    bool AllowSingleJobLargeGpuOperationsInMultipleTrees;

    TJobResourcesConfigPtr MinSpareAllocationResourcesOnNode;

    bool EnableDetailedLogsForStarvingOperations;

    bool EnableDetailedLogsForSingleAllocationVanillaOperations;

    bool ConsiderSingleAllocationVanillaOperationsAsGang;

    TGpuSchedulingPolicyConfigPtr GpuSchedulingPolicy;

    bool EnableAbsoluteFairShareStarvationTolerance;
    bool ConsiderAllocationOnFairShareBoundPreemptible;

    // Testing options.
    bool EnablePreliminaryResourceLimitsCheck;
    bool EnableResourceTreeRandomSleeps;

    REGISTER_YSON_STRUCT(TStrategyTreeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStrategyTreeConfig)

////////////////////////////////////////////////////////////////////////////////

struct TPoolTreesTemplateConfig
    : public NYTree::TYsonStruct
{
    //! Priority to apply filter.
    int Priority;

    //! Tree name filter.
    NRe2::TRe2Ptr Filter;

    //! Tree config patch.
    NYTree::INodePtr Config;

    REGISTER_YSON_STRUCT(TPoolTreesTemplateConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPoolTreesTemplateConfig)

////////////////////////////////////////////////////////////////////////////////

struct TOperationStuckCheckOptions
    : public NYTree::TYsonStruct
{
    TDuration Period;

    //! During this timeout after activation operation can not be considered as stuck.
    TDuration SafeTimeout;

    //! Operation that has less than this number of schedule allocation calls can not be considered as stuck.
    int MinScheduleAllocationAttempts;

    //! Reasons that are considered as unsuccessful in schedule allocation attempts.
    THashSet<EDeactivationReason> DeactivationReasons;

    //! During this timeout after activation operation can not be considered stuck due to limiting ancestor.
    TDuration LimitingAncestorSafeTimeout;

    REGISTER_YSON_STRUCT(TOperationStuckCheckOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOperationStuckCheckOptions);

////////////////////////////////////////////////////////////////////////////////

struct TStrategyConfig
    : public TStrategyOperationControllerConfig
{
    //! How often to update, log, profile fair share in pool trees.
    TDuration FairShareUpdatePeriod;
    TDuration FairShareProfilingPeriod;
    TDuration FairShareLogPeriod;
    TDuration AccumulatedUsageLogPeriod;

    //! How often min needed resources for allocations are retrieved from controller.
    TDuration MinNeededResourcesUpdatePeriod;

    //! How often to build and log resource usage and guarantee statistics.
    TDuration ResourceMeteringPeriod;

    //! How often to update resource usage snapshot.
    TDuration ResourceUsageSnapshotUpdatePeriod;

    //! Limit on number of operations in cluster.
    int MaxOperationCount;

    // COMPAT(eshcherbin)
    std::optional<TDuration> OperationHangupCheckPeriod;
    std::optional<TDuration> OperationHangupSafeTimeout;
    std::optional<int> OperationHangupMinScheduleAllocationAttempts;
    std::optional<THashSet<EDeactivationReason>> OperationHangupDeactivationReasons;
    std::optional<TDuration> OperationHangupDueToLimitingAncestorSafeTimeout;

    TOperationStuckCheckOptionsPtr OperationStuckCheck;

    //! List of operation types which should be disabled in tentative tree by default.
    THashSet<EOperationType> OperationsWithoutTentativePoolTrees;

    //! Tentative pool trees used by default for operations that specified 'UseDefaultTentativePoolTrees' options.
    THashSet<std::string> DefaultTentativePoolTrees;

    //! Enables the "schedule_in_single_tree" operation spec option cluster-wide.
    bool EnableScheduleInSingleTree;

    TStrategyTestingOptionsPtr StrategyTestingOptions;

    //! Template pool tree configs.
    THashMap<std::string, TPoolTreesTemplateConfigPtr> TemplatePoolTreeConfigMap;

    TDuration SchedulerTreeAlertsUpdatePeriod;

    // COMPAT(renadeen): remove when optimization proves worthy.
    bool EnableOptimizedOperationOrchid;

    TString EphemeralPoolNameRegex;

    bool RequireSpecifiedOperationPoolsExistence;

    //! Minimum amount of resources to continue schedule allocation attempts.
    TJobResourcesConfigPtr MinSpareAllocationResourcesOnNode;

    REGISTER_YSON_STRUCT(TStrategyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStrategyConfig)


////////////////////////////////////////////////////////////////////////////////

struct TTestingOptions
    : public NYTree::TYsonStruct
{
    // Testing options that enables random master disconnections.
    bool EnableRandomMasterDisconnection;
    TDuration RandomMasterDisconnectionMaxBackoff;

    // Testing option that enables sleeping during master connect.
    TDelayConfigPtr MasterConnectionDelay;

    // Testing option that enables sleeping during master disconnect.
    std::optional<TDuration> MasterDisconnectDelay;

    // Testing option that enabled sleeping before handle orphaned operations.
    std::optional<TDuration> HandleOrphanedOperationsDelay;

    // Testing option that enables sleeping between intermediate and final states of operation.
    TDelayConfigPtr FinishOperationTransitionDelay;

    // Testing option that enables sleeping after node state checking.
    TDelayConfigPtr NodeHeartbeatProcessingDelay;

    // Testing option that enables sleeping right before creation of operation node.
    TDelayConfigPtr OperationNodeCreationDelay;

    // Testing option that enables sleeping after creation of operation node, but before creation of secure vault node.
    TDelayConfigPtr SecureVaultCreationDelay;

    REGISTER_YSON_STRUCT(TTestingOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTestingOptions)

////////////////////////////////////////////////////////////////////////////////

struct TOperationsCleanerConfig
    : public virtual NYTree::TYsonStruct
{
    //! Enables cleaner.
    bool Enable;

    //! Enables archivation, if set to false then operations will be removed from Cypress
    //! without insertion to archive.
    bool EnableOperationArchivation;

    //! Operations are kept in Cypress for this duration after finish.
    TDuration CleanDelay;

    //! Analysis period.
    TDuration AnalysisPeriod;

    //! Number of operations to remove in one batch.
    int RemoveBatchSize;

    //! Number of operations to remove in one subbatch request.
    int RemoveSubbatchSize;

    //! Timeout for removal batch to be collected. If timeout expires then
    //! removal of smaller batch will be performed.
    TDuration RemoveBatchTimeout;

    //! Operations older than this timeout will be removed.
    TDuration MaxOperationAge;

    //! Number of operations to archive in one batch.
    //! Should be moderate since row of operation is rather big.
    int ArchiveBatchSize;

    //! Timeout for archival batch to be collected. If timeout expires then
    //! archivation of smaller batch will be performed.
    TDuration ArchiveBatchTimeout;

    //! Timeout for write transaction.
    TDuration TabletTransactionTimeout;

    //! Leave no more than this amount of operation per each user.
    int MaxOperationCountPerUser;

    //! Leave no more than this amount of completed and aborted operations.
    int SoftRetainedOperationCount;

    //! Leave no more than this amount of operations in total.
    int HardRetainedOperationCount;

    //! Min sleep delay in retries between two insertion invocations.
    TDuration MinArchivationRetrySleepDelay;

    //! Max sleep delay in retries between two insertion invocations.
    TDuration MaxArchivationRetrySleepDelay;

    //! Archivation will be disabled if enqueued operation count exceeds this limit.
    int MaxOperationCountEnqueuedForArchival;

    //! Duration after which archivation will be turned on again.
    TDuration ArchivationEnableDelay;

    //! Max sleep delay between two removal invocations.
    TDuration MaxRemovalSleepDelay;

    //! Number of operations failed to archive to set scheduler alert.
    int MinOperationCountEnqueuedForAlert;

    //! Timeout to wait for finished operations information from archive.
    TDuration FinishedOperationsArchiveLookupTimeout;

    //! The number of operations in batch to parse.
    int ParseOperationAttributesBatchSize;

    //! Enables alert event archivation. If set to false events will be discarded.
    bool EnableOperationAlertEventArchivation;

    //! Max enqueued alert event count stored on cleaner.
    int MaxEnqueuedOperationAlertEventCount;

    //! Max alert events stored in archive for each alert type (per operation).
    int MaxAlertEventCountPerAlertType;

    //! How often to send enqueued operation alert events.
    TDuration OperationAlertEventSendPeriod;

    //! If alert events are not successfully sent
    //! within this period, alert is raised.
    TDuration OperationAlertSenderAlertThreshold;

    //! How often to retry removing a previously locked operation.
    TDuration LockedOperationWaitTimeout;

    //! Disconnect from master in case of finished operation fetch failure.
    //! This option does not affect fetching after cleaner is re-enabled.
    bool DisconnectOnFinishedOperationFetchFailure;

    //! Timeout to remove operation from Cypress before setting an alert.
    TDuration OperationRemovalStuckTimeout;

    //! Time the operation stays in operations cleaner if we can't remove it.
    //! For example, if operation was removed not by operations cleaner.
    //! This timeout should give enough time to set corresponding alert.
    TDuration OperationRemovalDropTimeout;

    REGISTER_YSON_STRUCT(TOperationsCleanerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOperationsCleanerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TAliveControllerAgentThresholds final
{
    i64 Absolute;
    double Relative;

    friend void Deserialize(TAliveControllerAgentThresholds& thresholds, const NYTree::INodePtr& node);
    friend void Deserialize(TAliveControllerAgentThresholds& thresholds, NYson::TYsonPullParserCursor* cursor);
    friend void Serialize(const TAliveControllerAgentThresholds& thresholds, NYson::IYsonConsumer* consumer);
};

struct TControllerAgentTrackerConfig
    : public virtual NYTree::TYsonStruct
{
    NRpc::TResponseKeeperConfigPtr ResponseKeeper;

    // Scheduler scheduler-to-agent operation request timeout for light requests.
    // These are expected to be served in O(1).
    TDuration LightRpcTimeout;

    // Scheduler scheduler-to-agent operation request timeout for heavy requests.
    // These may run for prolonged time periods (e.g. operation preparation).
    TDuration HeavyRpcTimeout;

    // If the agent does not report a heartbeat within this period,
    // it is automatically unregistered.
    TDuration HeartbeatTimeout;

    // Timeout of incarnation transaction.
    TDuration IncarnationTransactionTimeout;

    // Timeout of incarnation transaction.
    TDuration IncarnationTransactionPingPeriod;

    // Strategy to pick controller agent for operation.
    EControllerAgentPickStrategy AgentPickStrategy;

    // Agent score weight will be raised to this power.
    double MemoryBalancedPickStrategyScorePower;

    // Agent must have at least #MinAgentAvailableMemory free memory to serve new operation.
    i64 MinAgentAvailableMemory;

    // Agent must have at least #MinAgentAvailableMemoryFraction of free memory to serve new operation.
    double MinAgentAvailableMemoryFraction;

    // Must be at least #MinAgentCount controller agent for successful assignment agent to waiting operation.
    int MinAgentCount;

    // Tag to threshols for alive agents with the tag
    THashMap<TString, TAliveControllerAgentThresholds> TagToAliveControllerAgentThresholds;

    i64 MaxMessageAllocationEventCount;

    int MessageOffloadThreadCount;

    bool EnableResponseKeeper;

    REGISTER_YSON_STRUCT(TControllerAgentTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TResourceMeteringConfig
    : public NYTree::TYsonStruct
{
    //! Default ABC id for use in resource metering
    int DefaultAbcId;

    REGISTER_YSON_STRUCT(TResourceMeteringConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TResourceMeteringConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerConfig
    : public TStrategyConfig
    , public TSingletonsDynamicConfig
{
    //! Number of shards the nodes are split into.
    int NodeShardCount;

    TDuration ConnectRetryBackoffTime;

    //! Timeout for node expiration in scheduler.
    TDuration NodeRegistrationTimeout;

    //! Timeout for node heartbeat expiration.
    //! After that timeout scheduler state of node becomes offline.
    TDuration NodeHeartbeatTimeout;

    TDuration WatchersUpdatePeriod;

    TDuration NodesAttributesUpdatePeriod;

    TDuration ProfilingUpdatePeriod;

    TDuration AlertsUpdatePeriod;

    //! All update and completed allocations submitted to strategy with at least such frequency.
    TDuration NodeShardSubmitAllocationsToStrategyPeriod;

    TDuration LockTransactionTimeout;

    TDuration PoolTreesLockTransactionTimeout;

    TDuration PoolTreesLockCheckBackoff;

    TDuration ClusterInfoLoggingPeriod;

    TDuration NodesInfoLoggingPeriod;

    TDuration ExecNodeDescriptorsUpdatePeriod;

    //! Allocations running on node are logged periodically or when they change their state.
    TDuration AllocationsLoggingPeriod;

    //! Statistics and resource usages of allocations running on a node are updated
    //! not more often then this period.
    TDuration RunningAllocationsUpdatePeriod;

    //! Missing allocations are checked not more often then this period.
    TDuration MissingAllocationsCheckPeriod;

    TDuration TransientOperationQueueScanPeriod;

    TDuration PendingByPoolOperationScanPeriod;

    TDuration OperationToAgentAssignmentBackoff;

    //! Maximum number of allocations to start within a single heartbeat.
    std::optional<int> MaxStartedAllocationsPerHeartbeat;

    //! Timeout to store cached value of exec nodes information
    //! for scheduling tag filter without access.
    TDuration NodeShardExecNodesCacheUpdatePeriod;

    // Backoff for processing successive heartbeats.
    TDuration HeartbeatProcessBackoff;
    // Number of heartbeats that can be processed without applying backoff.
    int SoftConcurrentHeartbeatLimit;
    // Maximum number of simultaneously processed heartbeats.
    int HardConcurrentHeartbeatLimit;

    // Maximum scheduling capacity of concurrent heartbeats in a node shard.
    int SchedulingHeartbeatComplexityLimit;

    // Use heartbeat complexities in throttling.
    bool UseHeartbeatSchedulingComplexityThrottling;

    // Scheduler does not apply this option on the fly yet.
    TDuration OrchidKeysUpdatePeriod;

    // Scheduler does not apply this option on the fly yet.
    TDuration StaticOrchidCacheUpdatePeriod;

    bool EnableUnrecognizedAlert;

    // How much time we wait before aborting the revived allocation that was not confirmed
    // by the corresponding execution node.
    TDuration AllocationRevivalAbortTimeout;

    //! Timeout of cached exec nodes information entries
    //! per scheduling tag filters.
    TDuration SchedulingTagFilterExpireTimeout;

    //! Timeout of finished allocation storing before forced removal.
    TDuration FinishedAllocationStoringTimeout;

    //! Timeout of finished operation allocations storing before forced removal.
    TDuration FinishedOperationAllocationStoringTimeout;

    TDuration OperationsUpdatePeriod;

    TTestingOptionsPtr TestingOptions;

    NEventLog::TStaticTableEventLogManagerConfigPtr EventLog;

    NYTree::IMapNodePtr SpecTemplate;

    TControllerAgentTrackerConfigPtr ControllerAgentTracker;

    TDuration JobReporterIssuesCheckPeriod;

    int JobReporterWriteFailuresAlertThreshold;
    int JobReporterQueueIsTooLargeAlertThreshold;

    int NodeChangesCountThresholdToUpdateCache;

    TDuration OperationTransactionPingPeriod;

    // Operations cleaner config.
    TOperationsCleanerConfigPtr OperationsCleaner;

    bool PoolChangeIsAllowed;

    TDuration MaxOfflineNodeAge;
    TDuration MaxNodeUnseenPeriodToAbortAllocations;

    //! By default, when the scheduler encounters a malformed operation spec during revival, it disconnects.
    //! This serves as a safeguard protecting us from accidentally failing all operations in case a bug
    //! is introduced in spec parser. This option, when set to true, overrides this behavior and enables
    //! such operations to be just skipped.
    bool SkipOperationsWithMalformedSpecDuringRevival;

    //! The number of threads in OrchidWorker thread pool used for serving reads from
    //! the scheduler's orchid.
    int OrchidWorkerThreadCount;

    //! The number of threads in FSUpdatePool thread pool used for running pool tree updates concurrently.
    int FairShareUpdateThreadCount;

    //! The number of threads for background activity.
    int BackgroundThreadCount;

    //! Allowed resources overcommit duration before scheduler initiate allocation abortions.
    TDuration AllowedNodeResourcesOvercommitDuration;

    //! Path to Cypress root node with pool tree and pool configs.
    //! Can be a path to simple map node or special virtual map node.
    TString PoolTreesRoot;

    int MaxEventLogNodeBatchSize;

    //! Period of scanning node infos to check that it belongs to some pool tree.
    TDuration ValidateNodeTagsPeriod;

    //! Enable immediate allocation abort if node reported zero number of user slots.
    bool EnableAllocationAbortOnZeroUserSlots;

    //! Option to manage subbatch size for fetching operation during registration.
    //! Increase this value to speedup registration.
    int FetchOperationAttributesSubbatchSize;

    //! The number of operations in batch to parse during connection.
    int ParseOperationAttributesBatchSize;

    //! Config for some resource metering defaults.
    TResourceMeteringConfigPtr ResourceMetering;

    //! All registered scheduler experiments keyed by experiment names.
    THashMap<TString, TExperimentConfigPtr> Experiments;

    //! How often to check for errors on matching operations against experiment filters.
    TDuration ExperimentAssignmentErrorCheckPeriod;

    //! How long the alert will remain on after an error occured when matching operations against experiment filters.
    //! Should be significantly longer than the #ExperimentAssignmentErrorCheckPeriod.
    TDuration ExperimentAssignmentAlertDuration;

    bool ConsiderDiskQuotaInPreemptiveSchedulingDiscount;

    //! Duration of ScheduleAllocation call to log this result.
    TDuration ScheduleAllocationDurationLoggingThreshold;

    //! Enables updating last metering log time.
    bool UpdateLastMeteringLogTime;

    bool EnableHeavyRuntimeParameters;

    bool EnableOperationHeavyAttributesArchivation;
    TDuration OperationHeavyAttributesArchivationTimeout;

    TDuration ScheduleAllocationEntryRemovalTimeout;
    TDuration ScheduleAllocationEntryCheckPeriod;

    NRpc::TResponseKeeperConfigPtr OperationServiceResponseKeeper;

    bool CrashOnAllocationHeartbeatProcessingException;

    bool DisableSchedulingOnNodeWithWaitingAllocations;

    int MinRequiredArchiveVersion;

    NRpc::TServerDynamicConfigPtr RpcServer;

    int OperationSpecTreeSizeLimit;
    i64 OperationSpecTooLargeAlertThreshold;

    //! Configures the default expiration timeout used when creating temporary
    //! tokens for operations. In a regular scenario the expiration timeout
    //! is removed right after creating the operation node in Cypress. However,
    //! if a scheduler does disconnect after the token is issued but before the
    //! operation node is created, this will ensure it is cleaned up eventually.
    TDuration TemporaryOperationTokenExpirationTimeout;

    THashSet<EOperationManagementAction> OperationActionsAllowedForPoolManagers;

    REGISTER_YSON_STRUCT(TSchedulerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
{
    NScheduler::TSchedulerConfigPtr Scheduler;

    //! Known scheduler addresses.
    NNodeTrackerClient::TNetworkAddressList Addresses;

    NYTree::IMapNodePtr CypressAnnotations;

    bool AbortOnUnrecognizedOptions;

    REGISTER_YSON_STRUCT(TSchedulerBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerProgramConfig
    : public TSchedulerBootstrapConfig
    , public TServerProgramConfig
{
    REGISTER_YSON_STRUCT(TSchedulerProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerProgramConfig)

////////////////////////////////////////////////////////////////////////////////

struct TOperationOptions
    : public NYTree::TYsonStruct
{
    // Allocation preemption timeout.
    std::optional<TDuration> AllocationPreemptionTimeout;

    // Allocation graceful preemption timeout.
    std::optional<TDuration> AllocationGracefulPreemptionTimeout;

    REGISTER_YSON_STRUCT(TOperationOptions)

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOperationOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
