
#pragma once

#include "public.h"
#include "scheduling_tag.h"

#include <yt/server/lib/job_proxy/config.h>

#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/ytlib/hive/config.h>

#include <yt/ytlib/scheduler/config.h>

#include <yt/client/ypath/public.h>

#include <yt/ytlib/event_log/config.h>

#include <yt/core/concurrency/config.h>

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/yson_serializable.h>

#include <yt/library/re2/re2.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDeactivationReason,
    (IsNotAlive)
    (UnmatchedSchedulingTag)
    (IsNotStarving)
    (ScheduleJobFailed)
    (NoBestLeafDescendant)
    (MinNeededResourcesUnsatisfied)
    (ResourceLimitsExceeded)
    (SaturatedInTentativeTree)
    (OperationDisabled)
    (BadPacking)
    (FairShareExceeded)
    (MaxConcurrentScheduleJobCallsPerNodeShardViolated)
    (RecentScheduleJobFailed)
);

////////////////////////////////////////////////////////////////////////////////

class TStrategyTestingOptions
    : public NYTree::TYsonSerializable
{
public:
    // Testing option that enables sleeping during fair share strategy update.
    std::optional<TDuration> DelayInsideFairShareUpdate;

    TStrategyTestingOptions();
};

DEFINE_REFCOUNTED_TYPE(TStrategyTestingOptions)

////////////////////////////////////////////////////////////////////////////////

class TFairShareStrategyOperationControllerConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    // NB(eshcherbin): This limit is only checked once every fair share update. Finer throttling is achieved
    // via the "per node shard" limit.
    //! Limit on the number of concurrent calls to ScheduleJob of single controller.
    int MaxConcurrentControllerScheduleJobCalls;

    //! Limit on the number of concurrent calls to ScheduleJob of single controller per node shard.
    int MaxConcurrentControllerScheduleJobCallsPerNodeShard;

    //! Maximum allowed time for single job scheduling.
    TDuration ScheduleJobTimeLimit;

    //! Backoff time after controller schedule job failure.
    TDuration ScheduleJobFailBackoffTime;

    //! Timeout after which "schedule job timed out" alert is expired and unset.
    TDuration ScheduleJobTimeoutAlertResetTime;

    //! Timeout for schedule jobs in fair share strategy.
    TDuration ScheduleJobsTimeout;

    TFairShareStrategyOperationControllerConfig();
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategyOperationControllerConfig)

////////////////////////////////////////////////////////////////////////////////

class TFairShareStrategyTreeConfig
    : virtual public NYTree::TYsonSerializable
{
public:
    // Specifies nodes that are served by this tree.
    TSchedulingTagFilter NodesFilter;

    // The following settings can be overridden in operation spec.
    TDuration MinSharePreemptionTimeout;
    TDuration FairSharePreemptionTimeout;
    double FairShareStarvationTolerance;

    TDuration MinSharePreemptionTimeoutLimit;
    TDuration FairSharePreemptionTimeoutLimit;
    double FairShareStarvationToleranceLimit;

    bool EnableAggressiveStarvation;

    //! Any operation with less than this number of running jobs cannot be preempted.
    int MaxUnpreemptableRunningJobCount;

    //! Limit on number of operations in pool.
    int MaxOperationCountPerPool;
    int MaxRunningOperationCountPerPool;

    //! If enabled, pools will be able to starve and provoke preemption.
    bool EnablePoolStarvation;

    //! Default parent pool for operations with unknown pool.
    TString DefaultParentPool;
    //! Forbid immediate operations in root.
    bool ForbidImmediateOperationsInRoot;

    // Preemption timeout for operations with small number of jobs will be
    // discounted proportionally to this coefficient.
    double JobCountPreemptionTimeoutCoefficient;

    //! Thresholds to partition jobs of operation
    //! to preemptable, aggressively preemptable and non-preemptable lists.
    double PreemptionSatisfactionThreshold;
    double AggressivePreemptionSatisfactionThreshold;

    //! To investigate CPU load of node shard threads.
    bool EnableSchedulingTags;

    //! Backoff for printing tree scheduling info in heartbeat.
    TDuration HeartbeatTreeSchedulingInfoLogBackoff;

    //! Maximum number of ephemeral pools that can be created by user.
    int MaxEphemeralPoolsPerUser;

    //! If update of preemptable lists of operation takes more than that duration
    //! then this event will be logged.
    TDuration UpdatePreemptableListDurationLoggingThreshold;

    //! Enables profiling strategy attributes for operations.
    bool EnableOperationsProfiling;

    NRe2::TRe2Ptr CustomProfilingTagFilter;

    //! If usage ratio is less than threshold multiplied by demand ratio we enables regularization.
    double ThresholdToEnableMaxPossibleUsageRegularization;

    //! Limit on number of operations in tree.
    int MaxRunningOperationCount;
    int MaxOperationCount;

    //! Delay before starting considering total resource limits after scheduler connection.
    TDuration TotalResourceLimitsConsiderDelay;

    //! Backoff for scheduling with preemption on the node (it is need to decrease number of calls of PrescheduleJob).
    TDuration PreemptiveSchedulingBackoff;

    //! Period of ban from the moment of operation saturation in tentative tree.
    TDuration TentativeTreeSaturationDeactivationPeriod;

    //! Enables infer of weight from effective min share ratios (if weight is not implicitly specified); inferred weight is this number mupltiplied by min share ratio.
    std::optional<double> InferWeightFromMinShareRatioMultiplier;

    TFairShareStrategyPackingConfigPtr Packing;

    //! List of operation types which should not be run in that tree as tentative.
    std::optional<THashSet<EOperationType>> NonTentativeOperationTypes;

    // TODO(ignat): Remove this.
    double LogFairShareRatioDisagreementThreshold;

    //! Period of best allocation ratio update for operations.
    TDuration BestAllocationRatioUpdatePeriod;

    bool EnableByUserProfiling;

    TFairShareStrategyTreeConfig();
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategyTreeConfig)

////////////////////////////////////////////////////////////////////////////////

class TFairShareStrategyConfig
    : public TFairShareStrategyOperationControllerConfig
{
public:
    //! How often to update, log, profile fair share in fair share trees.
    TDuration FairShareUpdatePeriod;
    TDuration FairShareProfilingPeriod;
    TDuration FairShareLogPeriod;

    //! How often min needed resources for jobs are retrieved from controller.
    TDuration MinNeededResourcesUpdatePeriod;

    //! How often to build and log resource usage and guarantee statistics.
    TDuration ResourceMeteringPeriod;

    //! Limit on number of operations in cluster.
    int MaxOperationCount;

    //! Unschedulable operations check period.
    TDuration OperationUnschedulableCheckPeriod;

    //! During this timeout after activation operation can not be considered as unschedulable.
    TDuration OperationUnschedulableSafeTimeout;

    //! Operation that has less than this number of schedule job calls can not be considered as unschedulable.
    int OperationUnschedulableMinScheduleJobAttempts;

    //! Reasons that consider as unsuccessfull in schedule job attempts.
    THashSet<EDeactivationReason> OperationUnschedulableDeactiovationReasons;

    //! List of operation types which should be disabled in tentative tree by default.
    THashSet<EOperationType> OperationsWithoutTentativePoolTrees;

    //! Tentative pool trees used by default for operations that specified 'UseDefaultTentativePoolTrees' options.
    THashSet<TString> DefaultTentativePoolTrees;

    //! Enables the "schedule_in_single_tree" operation spec option cluster-wide.
    bool EnableScheduleInSingleTree;

    TStrategyTestingOptionsPtr StrategyTestingOptions;

    TFairShareStrategyConfig();
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategyConfig)

////////////////////////////////////////////////////////////////////////////////

class TTestingOptions
    : public NYTree::TYsonSerializable
{
public:
    // Testing options that enables random master disconnections.
    bool EnableRandomMasterDisconnection;
    TDuration RandomMasterDisconnectionMaxBackoff;

    // Testing option that enables sleeping during master disconnect.
    std::optional<TDuration> MasterDisconnectDelay;

    // Testing option that enables sleeping between intermediate and final states of operation.
    std::optional<TDuration> FinishOperationTransitionDelay;

    TTestingOptions();
};

DEFINE_REFCOUNTED_TYPE(TTestingOptions)

////////////////////////////////////////////////////////////////////////////////

class TOperationsCleanerConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Enables cleaner.
    bool Enable;

    //! Enables archivation, if set to false then operations will be removed from Cypress
    //! without insertion to archive.
    bool EnableArchivation;

    //! Operations are kept in Cypress for this duration after finish.
    TDuration CleanDelay;

    //! Analysis period.
    TDuration AnalysisPeriod;

    //! Number of operations to remove in one batch.
    int RemoveBatchSize;

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

    TOperationsCleanerConfig();
};

DEFINE_REFCOUNTED_TYPE(TOperationsCleanerConfig)

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentTrackerConfig
    : public virtual NYTree::TYsonSerializable
{
public:
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

    TControllerAgentTrackerConfig();
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerConfig
    : public TFairShareStrategyConfig
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

    //! All update and completed jobs submitted to strategy with at least such frequency.
    TDuration NodeShardSubmitJobsToStrategyPeriod;

    TDuration LockTransactionTimeout;

    TDuration JobProberRpcTimeout;

    TDuration ClusterInfoLoggingPeriod;

    TDuration NodesInfoLoggingPeriod;

    TDuration ExecNodeDescriptorsUpdatePeriod;

    //! Jobs running on node are logged periodically or when they change their state.
    TDuration JobsLoggingPeriod;

    //! Statistics and resource usages of jobs running on a node are updated
    //! not more often then this period.
    TDuration RunningJobsUpdatePeriod;

    //! Missing jobs are checked not more often then this period.
    TDuration MissingJobsCheckPeriod;

    TDuration TransientOperationQueueScanPeriod;

    TDuration WaitingForPoolOperationScanPeriod;

    TDuration OperationToAgentAssignmentBackoff;

    //! Maximum number of jobs to start within a single heartbeat.
    std::optional<int> MaxStartedJobsPerHeartbeat;

    //! Timeout to store cached value of exec nodes information
    //! for scheduling tag filter without access.
    TDuration NodeShardExecNodesCacheUpdatePeriod;

    // Backoff for processing successive heartbeats.
    TDuration HeartbeatProcessBackoff;
    // Number of heartbeats that can be processed without applying backoff.
    int SoftConcurrentHeartbeatLimit;
    // Maximum number of simultaneously processed heartbeats.
    int HardConcurrentHeartbeatLimit;

    // Scheduler does not apply this option on the fly yet.
    TDuration OrchidKeysUpdatePeriod;

    // Scheduler does not apply this option on the fly yet.
    TDuration StaticOrchidCacheUpdatePeriod;

    // Enables job reporter to send job events/statistics etc.
    bool EnableJobReporter;

    // Enables job reporter to send job specs.
    bool EnableJobSpecReporter;

    // Enables job reporter to send job stderrs.
    bool EnableJobStderrReporter;

    // Enables job reporter to send job profiles.
    bool EnableJobProfileReporter;

    // Enables job reporter to send job fail contexts.
    bool EnableJobFailContextReporter;

    // Timeout for job interruption before we abort it.
    TDuration JobInterruptTimeout;

    // Timeout for graceful job interruption before we abort it.
    TDuration GracefulPreemptionJobInterruptTimeout;

    bool EnableUnrecognizedAlert;

    // How much time we wait before aborting the revived job that was not confirmed
    // by the corresponding execution node.
    TDuration JobRevivalAbortTimeout;

    //! Timeout of cached exec nodes information entries
    //! per scheduling tag filters.
    TDuration SchedulingTagFilterExpireTimeout;

    //! Timeout of finished job storing before forced removal.
    TDuration FinishedJobStoringTimeout;

    //! Timeout of finished operation jobs storing before forced removal.
    TDuration FinishedOperationJobStoringTimeout;

    TDuration OperationsUpdatePeriod;

    TDuration OperationsDestroyPeriod;

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
    TDuration MaxNodeUnseenPeriodToAbortJobs;

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

    //! This option enables special logic to handle the situation when node has changed the id.
    //! To prevent node duplication in orchid we must detect such situations and immediately remove node from old node shard.
    //! This option can cause performance issues.
    bool HandleNodeIdChangesStrictly;

    //! Allowed resources overcommit duration before scheduler initiate job abortions.
    TDuration AllowedNodeResourcesOvercommitDuration;

    //! Path to Cypress root node with pool tree and pool configs.
    //! Can be a path to simple map node or special virtual map node.
    TString PoolTreesRoot;

    //! Period of scanning node infos to check that it belongs to some fair share tree.
    TDuration ValidateNodeTagsPeriod;

    //! Enable immediate job abort if node reported zero number of user slots.
    bool EnableJobAbortOnZeroUserSlots;

    //! Option to manage subbatch size for fetching operation during registration.
    //! Increase this value to speedup registration.
    int FetchOperationAttributesSubbatchSize;

    //! Use scheduling strategy based on fair share ratio instead of new one based on vector fair share.
    bool UseClassicScheduler;

    //! Default ABC id for use in resource metering
    int DefaultAbcId;

    TSchedulerConfig();
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConfig)

class TSchedulerBootstrapConfig
    : public TServerConfig
{
public:
    //! Node-to-master connection.
    NApi::NNative::TConnectionConfigPtr ClusterConnection;

    NScheduler::TSchedulerConfigPtr Scheduler;

    NRpc::TResponseKeeperConfigPtr ResponseKeeper;

    //! Known scheduler addresses.
    NNodeTrackerClient::TNetworkAddressList Addresses;

    NYTree::IMapNodePtr CypressAnnotations;

    bool AbortOnUnrecognizedOptions;

    TSchedulerBootstrapConfig();
};

DEFINE_REFCOUNTED_TYPE(TSchedulerBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
