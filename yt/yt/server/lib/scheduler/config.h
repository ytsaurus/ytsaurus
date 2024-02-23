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

#include <yt/yt/ytlib/program/config.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/re2/re2.h>

#include <yt/yt/library/program/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDeactivationReason,
    (IsNotAlive)
    (UnmatchedSchedulingTag)
    (IsNotEligibleForAggressivelyPreemptiveScheduling)
    (IsNotEligibleForPreemptiveScheduling)
    (IsNotEligibleForSsdAggressivelyPreemptiveScheduling)
    (IsNotEligibleForSsdPreemptiveScheduling)
    (ScheduleAllocationFailed)
    (NoBestLeafDescendant)
    (MinNeededResourcesUnsatisfied)
    (ResourceLimitsExceeded)
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

class TStrategyTestingOptions
    : public NYTree::TYsonStruct
{
public:
    // Testing option that enables sleeping during fair share strategy update.
    std::optional<TDuration> DelayInsideFairShareUpdate;

    REGISTER_YSON_STRUCT(TStrategyTestingOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStrategyTestingOptions)

////////////////////////////////////////////////////////////////////////////////

class TFairShareStrategyControllerThrottling
    : public virtual NYTree::TYsonStruct
{
public:
    TDuration ScheduleAllocationStartBackoffTime;
    TDuration ScheduleAllocationMaxBackoffTime;
    double ScheduleAllocationBackoffMultiplier;

    REGISTER_YSON_STRUCT(TFairShareStrategyControllerThrottling);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategyControllerThrottling)

////////////////////////////////////////////////////////////////////////////////

// TODO(ignat): move it to subconfig.
class TFairShareStrategyOperationControllerConfig
    : public virtual NYTree::TYsonStruct
{
public:
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
    TFairShareStrategyControllerThrottlingPtr ControllerThrottling;

    //! Timeout after which "schedule allocation timed out" alert is expired and unset.
    TDuration ScheduleAllocationTimeoutAlertResetTime;

    //! Timeout for schedule allocations in fair share strategy.
    TDuration ScheduleAllocationsTimeout;

    //! Schedule allocation that longer this duration will be logged.
    TDuration LongScheduleAllocationLoggingThreshold;

    REGISTER_YSON_STRUCT(TFairShareStrategyOperationControllerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategyOperationControllerConfig)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerIntegralGuaranteesConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration SmoothPeriod;

    TDuration PoolCapacitySaturationPeriod;

    double RelaxedShareMultiplierLimit;

    REGISTER_YSON_STRUCT(TSchedulerIntegralGuaranteesConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerIntegralGuaranteesConfig)

////////////////////////////////////////////////////////////////////////////////

class TFairShareStrategySchedulingSegmentsConfig
    : public NYTree::TYsonStruct
{
public:
    ESegmentedSchedulingMode Mode;

    TSegmentToResourceAmount ReserveFairResourceAmount;

    TDuration InitializationTimeout;

    TDuration ManagePeriod;

    TDuration UnsatisfiedSegmentsRebalancingTimeout;

    TDuration ModuleReconsiderationTimeout;

    THashSet<TString> DataCenters;

    THashSet<TString> InfinibandClusters;

    ESchedulingSegmentModuleAssignmentHeuristic ModuleAssignmentHeuristic;

    ESchedulingSegmentModulePreemptionHeuristic ModulePreemptionHeuristic;

    ESchedulingSegmentModuleType ModuleType;

    bool EnableInfinibandClusterTagValidation;

    bool AllowOnlyGangOperationsInLargeSegment;

    bool EnableDetailedLogs;

    bool EnableModuleResetOnZeroFairShareAndUsage;

    TDuration PriorityModuleAssignmentTimeout;

    const THashSet<TString>& GetModules() const;

    REGISTER_YSON_STRUCT(TFairShareStrategySchedulingSegmentsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategySchedulingSegmentsConfig)

////////////////////////////////////////////////////////////////////////////////

class TFairShareStrategySsdPriorityPreemptionConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;

    TSchedulingTagFilter NodeTagFilter;

    std::vector<TString> MediumNames;

    REGISTER_YSON_STRUCT(TFairShareStrategySsdPriorityPreemptionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategySsdPriorityPreemptionConfig)

////////////////////////////////////////////////////////////////////////////////

class TBatchOperationSchedulingConfig
    : public NYTree::TYsonStruct
{
public:
    int BatchSize;

    TJobResourcesConfigPtr FallbackMinSpareAllocationResources;

    REGISTER_YSON_STRUCT(TBatchOperationSchedulingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBatchOperationSchedulingConfig)

////////////////////////////////////////////////////////////////////////////////

class TTreeTestingOptions
    : public NYTree::TYsonStruct
{
public:
    TDelayConfigPtr DelayInsideFairShareUpdate;

    std::optional<TDuration> DelayInsideResourceUsageInitializationInTree;

    REGISTER_YSON_STRUCT(TTreeTestingOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTreeTestingOptions)

////////////////////////////////////////////////////////////////////////////////

class TFairShareStrategyTreeConfig
    : virtual public NYTree::TYsonStruct
{
public:
    // Specifies nodes that are served by this tree.
    TSchedulingTagFilter NodesFilter;

    // The following settings can be overridden in operation spec.
    TDuration FairShareStarvationTimeout;
    TDuration FairShareAggressiveStarvationTimeout;
    double FairShareStarvationTolerance;

    bool EnableAggressiveStarvation;

    // TODO(eshcherbin): Remove in favor of NonPreemptibleResourceUsageThreshold.
    //! Any operation with less than this number of running allocations cannot be preempted.
    std::optional<int> MaxUnpreemptibleRunningAllocationCount;

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

    //! To investigate CPU load of node shard threads.
    bool EnableSchedulingTags;

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

    TFairShareStrategyPackingConfigPtr Packing;

    //! List of operation types which should not be run in that tree as tentative.
    std::optional<THashSet<EOperationType>> NonTentativeOperationTypes;

    //! Period of best allocation ratio update for operations.
    TDuration BestAllocationShareUpdatePeriod;

    bool EnableByUserProfiling;

    TSchedulerIntegralGuaranteesConfigPtr IntegralGuarantees;

    bool EnableResourceTreeStructureLockProfiling;
    bool EnableResourceTreeUsageLockProfiling;

    bool PreemptionCheckStarvation;
    bool PreemptionCheckSatisfaction;

    // Allocation preemption timeout.
    TDuration AllocationPreemptionTimeout;

    // Allocation graceful preemption timeout.
    TDuration AllocationGracefulPreemptionTimeout;

    TFairShareStrategySchedulingSegmentsConfigPtr SchedulingSegments;

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

    bool EnableConditionalPreemption;

    bool UseResourceUsageWithPrecommit;

    TDuration AllowedResourceUsageStaleness;

    //! How often to update allocation preemption statuses snapshot.
    TDuration CachedAllocationPreemptionStatusesUpdatePeriod;

    bool ShouldDistributeFreeVolumeAmongChildren;

    bool UseUserDefaultParentPoolMap;

    bool EnableResourceUsageSnapshot;

    int MaxEventLogPoolBatchSize;
    int MaxEventLogOperationBatchSize;

    TDuration AccumulatedResourceUsageUpdatePeriod;

    bool AllowAggressivePreemptionForGangOperations;

    //! If set, remote copy operation will fail to start if it can acquire
    //! more than RequiredResourceLimitsForRemoteCopy resources in any pool tree.
    bool FailRemoteCopyOnMissingResourceLimits;
    TJobResourcesConfigPtr RequiredResourceLimitsForRemoteCopy;

    TFairShareStrategySsdPriorityPreemptionConfigPtr SsdPriorityPreemption;

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

    EFifoPoolSchedulingOrder FifoPoolSchedulingOrder;

    bool UsePoolSatisfactionForScheduling;

    THistogramDigestConfigPtr PerPoolSatisfactionDigest;
    std::vector<double> PerPoolSatisfactionProfilingQuantiles;

    bool EnableGuaranteePriorityScheduling;

    THashSet<EJobResourceType> NecessaryResourcesForOperation;

    REGISTER_YSON_STRUCT(TFairShareStrategyTreeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategyTreeConfig)

////////////////////////////////////////////////////////////////////////////////

class TPoolTreesTemplateConfig
    : public NYTree::TYsonStruct
{
public:
    //! Priority to apply filter.
    int Priority;

    //! Tree name filter.
    NRe2::TRe2Ptr Filter;

    //! Fair share strategy config for filter.
    NYTree::INodePtr Config;

    REGISTER_YSON_STRUCT(TPoolTreesTemplateConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPoolTreesTemplateConfig)

////////////////////////////////////////////////////////////////////////////////

class TFairShareStrategyConfig
    : public TFairShareStrategyOperationControllerConfig
{
public:
    //! How often to update, log, profile fair share in fair share trees.
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

    //! Unschedulable operations check period.
    TDuration OperationHangupCheckPeriod;

    //! During this timeout after activation operation can not be considered as unschedulable.
    TDuration OperationHangupSafeTimeout;

    //! Operation that has less than this number of schedule allocation calls can not be considered as unschedulable.
    int OperationHangupMinScheduleAllocationAttempts;

    //! Reasons that consider as unsuccessful in schedule allocation attempts.
    THashSet<EDeactivationReason> OperationHangupDeactivationReasons;

    //! During this timeout after activation operation can not be considered as unschedulable due to limiting ancestor.
    TDuration OperationHangupDueToLimitingAncestorSafeTimeout;

    //! List of operation types which should be disabled in tentative tree by default.
    THashSet<EOperationType> OperationsWithoutTentativePoolTrees;

    //! Tentative pool trees used by default for operations that specified 'UseDefaultTentativePoolTrees' options.
    THashSet<TString> DefaultTentativePoolTrees;

    //! Enables the "schedule_in_single_tree" operation spec option cluster-wide.
    bool EnableScheduleInSingleTree;

    TStrategyTestingOptionsPtr StrategyTestingOptions;

    //! Template pool tree configs.
    THashMap<TString, TPoolTreesTemplateConfigPtr> TemplatePoolTreeConfigMap;

    bool EnablePoolTreesConfigCache;

    TDuration SchedulerTreeAlertsUpdatePeriod;

    // COMPAT(renadeen): remove when optimization proves worthy.
    bool EnableOptimizedOperationOrchid;

    // COMPAT(renadeen): remove when optimization proves worthy.
    bool EnableAsyncOperationEventLogging;

    TString EphemeralPoolNameRegex;

    REGISTER_YSON_STRUCT(TFairShareStrategyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategyConfig)


////////////////////////////////////////////////////////////////////////////////

class TTestingOptions
    : public NYTree::TYsonStruct
{
public:
    // Testing options that enables random master disconnections.
    bool EnableRandomMasterDisconnection;
    TDuration RandomMasterDisconnectionMaxBackoff;

    // Testing option that enables sleeping during master disconnect.
    std::optional<TDuration> MasterDisconnectDelay;

    // Testing option that enabled sleeping before handle orphaned operations.
    std::optional<TDuration> HandleOrphanedOperationsDelay;

    // Testing option that enables sleeping between intermediate and final states of operation.
    TDelayConfigPtr FinishOperationTransitionDelay;

    // Testing option that enables sleeping after node state checking.
    TDelayConfigPtr NodeHeartbeatProcessingDelay;

    // Testing option that enables sleeping after creation of operation node, but before creation of secure vault node.
    TDelayConfigPtr SecureVaultCreationDelay;

    REGISTER_YSON_STRUCT(TTestingOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTestingOptions)

////////////////////////////////////////////////////////////////////////////////

class TOperationsCleanerConfig
    : public virtual NYTree::TYsonStruct
{
public:
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

class TControllerAgentTrackerConfig
    : public virtual NYTree::TYsonStruct
{
public:
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

class TResourceMeteringConfig
    : public NYTree::TYsonStruct
{
public:
    //! Enables new format for abc_id.
    //! It enables writing abc_id as integer and disable writing could_id and folder_id.
    bool EnableNewAbcFormat;

    //! Default ABC id for use in resource metering
    int DefaultAbcId;

    //! Default id for all metering records.
    TString DefaultCloudId;
    TString DefaultFolderId;

    //! Enable separate schemas for guarantees and allocations.
    bool EnableSeparateSchemaForAllocation;

    REGISTER_YSON_STRUCT(TResourceMeteringConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TResourceMeteringConfig)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerConfig
    : public TFairShareStrategyConfig
    , public TNativeSingletonsDynamicConfig
{
public:
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

    bool AlwaysSendControllerAgentDescriptors;

    bool SendFullControllerAgentDescriptorsForAllocations;

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

    NEventLog::TEventLogManagerConfigPtr EventLog;

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

    //! The number of threads in FSUpdatePool thread pool used for running fair share tree updates concurrently.
    int FairShareUpdateThreadCount;

    //! The number of threads for background activity.
    int BackgroundThreadCount;

    //! Allowed resources overcommit duration before scheduler initiate allocation abortions.
    TDuration AllowedNodeResourcesOvercommitDuration;

    //! Path to Cypress root node with pool tree and pool configs.
    //! Can be a path to simple map node or special virtual map node.
    TString PoolTreesRoot;

    int MaxEventLogNodeBatchSize;

    //! Period of scanning node infos to check that it belongs to some fair share tree.
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

    //! Minimum amount of resources to continue schedule allocation attempts.
    std::optional<TJobResourcesConfigPtr> MinSpareAllocationResourcesOnNode;

    bool SendPreemptionReasonInNodeHeartbeat;

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

    bool WaitForAgentHeartbeatDuringOperationUnregistrationAtController;

    bool CrashOnAllocationHeartbeatProcessingException;

    int MinRequiredArchiveVersion;

    NRpc::TServerDynamicConfigPtr RpcServer;

    REGISTER_YSON_STRUCT(TSchedulerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConfig)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerBootstrapConfig
    : public TNativeServerConfig
{
public:

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

} // namespace NYT::NScheduler
