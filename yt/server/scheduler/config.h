#pragma once

#include "private.h"

#include <yt/server/job_proxy/config.h>

#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/ytlib/hive/config.h>

#include <yt/ytlib/ypath/public.h>

#include <yt/ytlib/event_log/config.h>

#include <yt/core/concurrency/config.h>

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TFairShareStrategyOperationControllerConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Limit on the number of concurrent calls to ScheduleJob of single controller.
    int MaxConcurrentControllerScheduleJobCalls;

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

    //! If update of preemtable lists of operation takes more than that duration
    //! then this event will be logged.
    TDuration UpdatePreemptableListDurationLoggingThreshold;

    //! Enables profiling strategy attributes for operations.
    bool EnableOperationsProfiling;

    //! If usage ratio is less than threshold multiplied by demand ratio we enables regularization.
    double ThresholdToEnableMaxPossibleUsageRegularization;

    //! Limit on number of operations in tree.
    int MaxRunningOperationCount;
    int MaxOperationCount;

    //! Delay before starting considering total resource limits after scheduler connection.
    TDuration TotalResourceLimitsConsiderDelay;

    //! Backoff for scheduling with preemption on the node (it is need to decrease number of calls of PrescheduleJob).
    TDuration PreemptiveSchedulingBackoff;

    //! Enables new possible resource usage computation scheme.
    //! TODO(asaitgalin): Use this by default and remove ThresholdToEnableMaxPossibleUsageRegularization
    //! option.
    bool EnableNewPossibleResourceUsageComputation;

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

    //! Limit on number of operations in cluster.
    int MaxOperationCount;

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
    TNullable<TDuration> MasterDisconnectDelay;

    // Testing option that enables sleeping between intermediate and final states of operation.
    TNullable<TDuration> FinishOperationTransitionDelay;

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

    //! Number of operations to fetch from Cypress in one batch.
    int FetchBatchSize;

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

    TOperationsCleanerConfig();
};

DEFINE_REFCOUNTED_TYPE(TOperationsCleanerConfig)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerConfig
    : public TFairShareStrategyConfig
{
public:
    //! Number of shards the nodes are split into.
    int NodeShardCount;

    TDuration ConnectRetryBackoffTime;

    //! Timeout for node expiration.
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

    TDuration ExecNodeDescriptorsUpdatePeriod;

    //! Jobs running on node are logged periodically or when they change their state.
    TDuration JobsLoggingPeriod;

    //! Statistics and resource usages of jobs running on a node are updated
    //! not more often then this period.
    TDuration RunningJobsUpdatePeriod;

    //! Missing jobs are checked not more often then this period.
    TDuration MissingJobsCheckPeriod;

    TDuration TransientOperationQueueScanPeriod;

    TDuration OperationToAgentAssignmentBackoff;

    //! Maximum number of jobs to start within a single heartbeat.
    TNullable<int> MaxStartedJobsPerHeartbeat;

    //! Timeout to store cached value of exec nodes information
    //! for scheduling tag filter without access.
    TDuration NodeShardExecNodesCacheUpdatePeriod;

    // Backoff for processing successive heartbeats.
    TDuration HeartbeatProcessBackoff;
    // Number of heartbeats that can be processed without applying backoff.
    int SoftConcurrentHeartbeatLimit;
    // Maximum number of simultaneously processed heartbeats.
    int HardConcurrentHeartbeatLimit;

    TDuration OrchidKeysUpdatePeriod;

    TDuration StaticOrchidCacheUpdatePeriod;

    // Enables job reporter to send job events/statistics etc.
    bool EnableJobReporter;

    // Enables job reporter to send job specs.
    bool EnableJobSpecReporter;

    // Enables job reporter to send job stderrs.
    bool EnableJobStderrReporter;

    // Enables job reporter to send job fail contexts.
    bool EnableJobFailContextReporter;

    // Timeout to try interrupt job before abort it.
    TDuration JobInterruptTimeout;

    bool EnableUnrecognizedAlert;

    // Number of nodes to store by memory distribution.
    int MemoryDistributionDifferentNodeTypesThreshold;

    // How much time we wait before aborting the revived job that was not confirmed
    // by the corresponding execution node.
    TDuration JobRevivalAbortTimeout;

    // Scheduler scheduler-to-agent operation request timeout for light requests.
    // These are expected to be served in O(1).
    TDuration ControllerAgentLightRpcTimeout;

    // Scheduler scheduler-to-agent operation request timeout for heavy requests.
    // These may run for prolonged time periods (e.g. operation preparation).
    TDuration ControllerAgentHeavyRpcTimeout;

    // If the agent does not report a heartbeat within this period,
    // it is automatically unregistered.
    TDuration ControllerAgentHeartbeatTimeout;

    //! Timeout of cached exec nodes information entries
    //! per scheduling tag filters.
    TDuration SchedulingTagFilterExpireTimeout;

    //! Timeout of finished job storing before forced removal.
    TDuration FinishedJobStoringTimeout;

    //! Timeout of finished operation jobs storing before forced removal.
    TDuration FinishedOperationJobStoringTimeout;

    TDuration OperationsUpdatePeriod;

    TTestingOptionsPtr TestingOptions;

    NEventLog::TEventLogConfigPtr EventLog;

    NYTree::IMapNodePtr SpecTemplate;

    int MinAgentCountForWaitingOperation;

    // Operations cleaner config.
    TOperationsCleanerConfigPtr OperationsCleaner;

    TSchedulerConfig();
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConfig)

class TSchedulerBootstrapConfig
    : public TServerConfig
{
public:
    //! Node-to-master connection.
    NApi::TNativeConnectionConfigPtr ClusterConnection;

    NScheduler::TSchedulerConfigPtr Scheduler;

    NRpc::TResponseKeeperConfigPtr ResponseKeeper;

    //! Known scheduler addresses.
    NNodeTrackerClient::TNetworkAddressList Addresses;

    TSchedulerBootstrapConfig()
    {
        RegisterParameter("cluster_connection", ClusterConnection);
        RegisterParameter("scheduler", Scheduler)
            .DefaultNew();
        RegisterParameter("response_keeper", ResponseKeeper)
            .DefaultNew();
        RegisterParameter("addresses", Addresses)
            .Default();

        RegisterPreprocessor([&] () {
            ResponseKeeper->EnableWarmup = false;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TSchedulerBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
