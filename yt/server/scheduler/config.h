#pragma once

#include "private.h"

#include <yt/server/controller_agent/config.h>

#include <yt/server/job_proxy/config.h>

#include <yt/ytlib/api/config.h>

#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/event_log/config.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/ytlib/hive/config.h>

#include <yt/ytlib/ypath/public.h>

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

    //! Backoff between schedule job statistics logging.
    TDuration ScheduleJobStatisticsLogBackoff;

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

    //! Limit on number of running operations in cluster.
    int MaxRunningOperationCount;
    //! Limit on number of operations in cluster.
    int MaxOperationCount;

    TFairShareStrategyConfig();

private:
    //! COMPAT
    bool EnableOperationsProfiling;
    TSchedulingTagFilter MainNodesFilter;
    TDuration TotalResourceLimitsConsiderDelay;
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategyConfig)

////////////////////////////////////////////////////////////////////////////////

class TJobSplitterConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MinJobTime;
    double ExecToPrepareTimeRatio;
    i64 MinTotalDataWeight;
    TDuration UpdatePeriod;
    TDuration MedianExcessDuration;
    double CandidatePercentile;
    int MaxJobsPerSplit;

    TJobSplitterConfig();
};

DEFINE_REFCOUNTED_TYPE(TJobSplitterConfig)

////////////////////////////////////////////////////////////////////////////////

class TOperationOptions
    : public NYTree::TYsonSerializable
    , public virtual NPhoenix::TDynamicTag
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TOperationOptions, 0x6d2a0bdd);

public:
    NYTree::INodePtr SpecTemplate;

    //! Controls finer initial slicing of input data to ensure even distribution of data split sizes among jobs.
    double SliceDataWeightMultiplier;

    //! Maximum number of primary data slices per job.
    int MaxDataSlicesPerJob;

    i64 MaxSliceDataWeight;
    i64 MinSliceDataWeight;

    //! Maximum number of output tables times job count an operation can have.
    int MaxOutputTablesTimesJobsCount;

    TJobSplitterConfigPtr JobSplitter;

    TOperationOptions();
};

DEFINE_REFCOUNTED_TYPE(TOperationOptions)

class TSimpleOperationOptions
    : public TOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSimpleOperationOptions, 0x875251fa);

public:
    int MaxJobCount;
    i64 DataWeightPerJob;

    TSimpleOperationOptions();
};

DEFINE_REFCOUNTED_TYPE(TSimpleOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TMapOperationOptions
    : public TSimpleOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMapOperationOptions, 0x5d08252b);

public:
    NControllerAgent::TJobSizeAdjusterConfigPtr JobSizeAdjuster;

    TMapOperationOptions();
};

DEFINE_REFCOUNTED_TYPE(TMapOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TUnorderedMergeOperationOptions
    : public TSimpleOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TUnorderedMergeOperationOptions, 0x28332598);
};

DEFINE_REFCOUNTED_TYPE(TUnorderedMergeOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TOrderedMergeOperationOptions
    : public TSimpleOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TOrderedMergeOperationOptions, 0xc71863e6);
};

DEFINE_REFCOUNTED_TYPE(TOrderedMergeOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TSortedMergeOperationOptions
    : public TSimpleOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortedMergeOperationOptions, 0x9089b24a);
};

DEFINE_REFCOUNTED_TYPE(TSortedMergeOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TReduceOperationOptions
    : public TSortedMergeOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TReduceOperationOptions, 0x91371bf5);

public:

    TReduceOperationOptions();
};

DEFINE_REFCOUNTED_TYPE(TReduceOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TJoinReduceOperationOptions
    : public TReduceOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TJoinReduceOperationOptions, 0xdd9303bc);
};

DEFINE_REFCOUNTED_TYPE(TJoinReduceOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TEraseOperationOptions
    : public TOrderedMergeOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TEraseOperationOptions, 0x73cb9f3b);
};

DEFINE_REFCOUNTED_TYPE(TEraseOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TSortOperationOptionsBase
    : public TOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortOperationOptionsBase, 0x1f448453);

public:
    int MaxPartitionJobCount;
    int MaxPartitionCount;
    i32 MaxSampleSize;
    i64 CompressedBlockSize;
    i64 MinPartitionWeight;
    i64 MinUncompressedBlockSize;
    NControllerAgent::TJobSizeAdjusterConfigPtr PartitionJobSizeAdjuster;

    TSortOperationOptionsBase();
};

DEFINE_REFCOUNTED_TYPE(TSortOperationOptionsBase)

////////////////////////////////////////////////////////////////////////////////

class TSortOperationOptions
    : public TSortOperationOptionsBase
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortOperationOptions, 0xc11251c0);
};

DEFINE_REFCOUNTED_TYPE(TSortOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TMapReduceOperationOptions
    : public TSortOperationOptionsBase
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMapReduceOperationOptions, 0x91e3968d);
};

DEFINE_REFCOUNTED_TYPE(TMapReduceOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TRemoteCopyOperationOptions
    : public TSimpleOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TRemoteCopyOperationOptions, 0xf3893dc8);
};

DEFINE_REFCOUNTED_TYPE(TRemoteCopyOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TVanillaOperationOptions
    : public TOperationOptions
{
public:
    //! Maximum number of tasks allowed.
    int MaxTaskCount;

    //! Maximum total number of jobs.
    int MaxTotalJobCount;

    TVanillaOperationOptions()
    {
        RegisterParameter("max_task_count", MaxTaskCount)
            .Default(100);
        RegisterParameter("max_total_job_count", MaxTotalJobCount)
            .Default(100 * 1000);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TVanillaOperationOptions, 0x93998ffa);
};

DEFINE_REFCOUNTED_TYPE(TVanillaOperationOptions)

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

    // Testing option that enables snapshot build/load cycle after operation materialization.
    bool EnableSnapshotCycleAfterMaterialization;

    // Testing option that enables sleeping between intermediate and final states of operation.
    TNullable<TDuration> FinishOperationTransitionDelay;

    TTestingOptions();
};

DEFINE_REFCOUNTED_TYPE(TTestingOptions)

////////////////////////////////////////////////////////////////////////////////

class TOperationAlertsConfig
    : public NYTree::TYsonSerializable
{
public:
    // Maximum allowed ratio of unused tmpfs size.
    double TmpfsAlertMaxUnusedSpaceRatio;

    // Min unused space threshold. If unutilized space is less than
    // this threshold then operation alert will not be set.
    i64 TmpfsAlertMinUnusedSpaceThreshold;

    // Maximum allowed aborted jobs time. If it is violated
    // then operation alert will be set.
    i64 AbortedJobsAlertMaxAbortedTime;

    // Maximum allowed aborted jobs time ratio.
    double AbortedJobsAlertMaxAbortedTimeRatio;

    // Minimum desired job duration.
    TDuration ShortJobsAlertMinJobDuration;

    // Minimum number of completed jobs after which alert can be set.
    i64 ShortJobsAlertMinJobCount;

    // Minimum partition size to enable data skew check.
    i64 IntermediateDataSkewAlertMinPartitionSize;

    // Minimum interquartile range to consider data to be skewed.
    i64 IntermediateDataSkewAlertMinInterquartileRange;

    // Job spec throttling alert is triggered if throttler activation
    // count is above this threshold.
    i64 JobSpecThrottlingAlertActivationCountThreshold;

    TOperationAlertsConfig();
};

DEFINE_REFCOUNTED_TYPE(TOperationAlertsConfig)

////////////////////////////////////////////////////////////////////////////////

class TSuspiciousJobsOptions
    : public NYTree::TYsonSerializable
{
public:
    //! Duration of no activity by job to be considered as suspicious.
    TDuration InactivityTimeout;

    //! Cpu usage delta that is considered insignificant when checking if job is suspicious.
    i64 CpuUsageThreshold;

    //! Time fraction spent in idle state of JobProxy -> UserJob pipe enough for job to be considered suspicious.
    double InputPipeIdleTimeFraction;

    //! Time fraction spent in idle state of UserJob -> JobProxy pipe enough for job to be considered suspicious.
    double OutputPipeIdleTimeFraction;

    //! Suspicious jobs per operation recalculation period.
    TDuration UpdatePeriod;

    //! Maximum number of suspicious jobs that are reported in Orchid for each job type.
    i64 MaxOrchidEntryCountPerType;

    TSuspiciousJobsOptions();
};

DEFINE_REFCOUNTED_TYPE(TSuspiciousJobsOptions)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerConfig
    : public TFairShareStrategyConfig
    , public NChunkClient::TChunkTeleporterConfig
{
public:
    //! Priority of control thread.
    TNullable<int> ControlThreadPriority;

    //! Number of threads for running controllers invokers.
    int ControllerThreadCount;

    //! Number of parallel operation snapshot builders.
    int ParallelSnapshotBuilderCount;

    //! Number of shards the nodes are split into.
    int NodeShardCount;

    TDuration ConnectRetryBackoffTime;

    //! Timeout for node expiration.
    TDuration NodeHeartbeatTimeout;

    TDuration TransactionsRefreshPeriod;

    TDuration OperationsUpdatePeriod;

    TDuration WatchersUpdatePeriod;

    TDuration NodesAttributesUpdatePeriod;

    TDuration ProfilingUpdatePeriod;

    TDuration AlertsUpdatePeriod;

    TDuration ChunkUnstagePeriod;

    TDuration ResourceDemandSanityCheckPeriod;

    TDuration LockTransactionTimeout;

    TDuration OperationTransactionTimeout;

    //! Timeout on operation initialization.
    //! It is needed to prevent hanging of remote copy when remote cluster is unavailable.
    TDuration OperationInitializationTimeout;

    TDuration JobProberRpcTimeout;

    TDuration OperationControllerSuspendTimeout;

    TDuration OperationLogProgressBackoff;

    TDuration OperationLogFairSharePeriod;

    TDuration ClusterInfoLoggingPeriod;

    TDuration UpdateExecNodeDescriptorsPeriod;

    TDuration OperationTimeLimitCheckPeriod;

    TDuration OperationControllerFailTimeout;

    TDuration AvailableExecNodesCheckPeriod;

    TDuration OperationProgressAnalysisPeriod;

    TDuration OperationBuildProgressPeriod;

    TDuration TaskUpdatePeriod;

    //! Jobs running on node are logged periodically or when they change their state.
    TDuration JobsLoggingPeriod;

    //! Statistics and resource usages of jobs running on a node are updated
    //! not more often then this period.
    TDuration RunningJobsUpdatePeriod;

    //! Missing jobs are checked not more often then this period.
    TDuration CheckMissingJobsPeriod;

    //! Max available exec node resources are updated not more often then this period.
    TDuration MaxAvailableExecNodeResourcesUpdatePeriod;

    //! Maximum allowed running time of operation. Null value is interpreted as infinity.
    TNullable<TDuration> OperationTimeLimit;

    //! Maximum number of job nodes per operation.
    int MaxJobNodesPerOperation;

    //! Number of chunk lists to be allocated when an operation starts.
    int ChunkListPreallocationCount;

    //! Maximum number of chunk lists to request via a single request.
    int MaxChunkListAllocationCount;

    //! Better keep the number of spare chunk lists above this threshold.
    int ChunkListWatermarkCount;

    //! Each time the number of spare chunk lists drops below #ChunkListWatermarkCount or
    //! the controller requests more chunk lists than we currently have,
    //! another batch is allocated. Each time we allocate #ChunkListAllocationMultiplier times
    //! more chunk lists than previously.
    double ChunkListAllocationMultiplier;

    //! Desired number of chunks to release in one batch.
    int DesiredChunkListsPerRelease;

    //! Maximum number of chunks per single fetch.
    int MaxChunksPerFetch;

    //! Maximum number of chunk trees to attach per request.
    int MaxChildrenPerAttachRequest;

    //! Maximum size of file allowed to be passed to jobs.
    i64 MaxFileSize;

    //! Maximum number of input tables an operation can have.
    int MaxInputTableCount;

    //! Maximum number of ranges on the input table.
    int MaxRangesOnTable;

    //! Maximum number of files per user job.
    int MaxUserFileCount;

    //! Maximum number of jobs to start within a single heartbeat.
    TNullable<int> MaxStartedJobsPerHeartbeat;

    //! Don't check resource demand for sanity if the number of online
    //! nodes is less than this bound.
    // TODO(ignat): rename to SafeExecNodeCount.
    int SafeOnlineNodeCount;

    //! Don't check resource demand for sanity if scheduler is online
    //! less than this timeout.
    TDuration SafeSchedulerOnlineTime;

    //! Time between two consecutive calls in operation controller to get exec nodes information from scheduler.
    TDuration ControllerUpdateExecNodesInformationDelay;

    //! Backoff between requesting exec nodes from scheduler.
    TDuration ExecNodesRequestPeriod;

    //! Timeout to store cached value of exec nodes information
    //! for scheduling tag filter without access.
    TDuration SchedulingTagFilterExpireTimeout;

    //! Timeout to store cached value of exec nodes information
    //! for scheduling tag filter without access.
    TDuration NodeShardExecNodesCacheUpdatePeriod;

    //! All update and completed jobs submitted to strategy with at least such frequency.
    TDuration NodeShardSubmitJobsToStrategyPeriod;

    //! Maximum number of foreign chunks to locate per request.
    int MaxChunksPerLocateRequest;

    //! Limit on the number of concurrent core dumps that can be written because
    //! of failed safe assertions inside controllers.
    int MaxConcurrentSafeCoreDumps;

	TSuspiciousJobsOptionsPtr SuspiciousJobs;

    //! Patch for all operation options.
    NYT::NYTree::INodePtr OperationOptions;

    //! Specific operation options.
    TMapOperationOptionsPtr MapOperationOptions;
    TReduceOperationOptionsPtr ReduceOperationOptions;
    TJoinReduceOperationOptionsPtr JoinReduceOperationOptions;
    TEraseOperationOptionsPtr EraseOperationOptions;
    TOrderedMergeOperationOptionsPtr OrderedMergeOperationOptions;
    TUnorderedMergeOperationOptionsPtr UnorderedMergeOperationOptions;
    TSortedMergeOperationOptionsPtr SortedMergeOperationOptions;
    TMapReduceOperationOptionsPtr MapReduceOperationOptions;
    TSortOperationOptionsPtr SortOperationOptions;
    TRemoteCopyOperationOptionsPtr RemoteCopyOperationOptions;
    TVanillaOperationOptionsPtr VanillaOperationOptions;

    //! Default environment variables set for every job.
    yhash<TString, TString> Environment;

    //! Interval between consequent snapshots.
    TDuration SnapshotPeriod;

    //! Timeout for snapshot construction.
    TDuration SnapshotTimeout;

    //! If |true|, snapshots are periodically constructed and uploaded into the system.
    bool EnableSnapshotBuilding;

    //! If |true|, snapshots are loaded during revival.
    bool EnableSnapshotLoading;

    //! If |true|, jobs are revived from snapshot.
    bool EnableJobRevival;

    //! If |false|, all locality timeouts are considered 0.
    bool EnableLocality;

    //! Allow failing a controller by passing testing option `controller_failure`
    //! in operation spec. Used only for testing purposes.
    bool EnableControllerFailureSpecOption;

    TString SnapshotTempPath;
    NApi::TFileReaderConfigPtr SnapshotReader;
    NApi::TFileWriterConfigPtr SnapshotWriter;

    NChunkClient::TFetcherConfigPtr Fetcher;

    NEventLog::TEventLogConfigPtr EventLog;

    //! Limits the rate (measured in chunks) of location requests issued by all active chunk scrapers.
    NConcurrency::TThroughputThrottlerConfigPtr ChunkLocationThrottler;

    TNullable<NYPath::TYPath> UdfRegistryPath;

    // Backoff for processing successive heartbeats.
    TDuration HeartbeatProcessBackoff;
    // Number of heartbeats that can be processed without applying backoff.
    int SoftConcurrentHeartbeatLimit;
    // Maximum number of simultaneously processed heartbeats.
    int HardConcurrentHeartbeatLimit;

    // Controls the rate at which jobs are scheduled in termes of slices per second.
    NConcurrency::TThroughputThrottlerConfigPtr JobSpecSliceThrottler;
    // Discriminates between "heavy" and "light" job specs. For those with slice count
    // not exceeding this threshold no throttling is done.
    int HeavyJobSpecSliceCountThreshold;


    // Enables using tmpfs if tmpfs_path is specified in user spec.
    bool EnableTmpfs;

    // Enable dynamic change of job sizes.
    bool EnablePartitionMapJobSizeAdjustment;

    bool EnableMapJobSizeAdjustment;

    // Enable splitting of long jobs.
    bool EnableJobSplitting;

    //! Acl used for intermediate tables and stderrs additional to acls specified by user.
    NYTree::IListNodePtr AdditionalIntermediateDataAcl;

    double UserJobMemoryDigestPrecision;
    double UserJobMemoryReserveQuantile;
    double JobProxyMemoryReserveQuantile;
    double ResourceOverdraftFactor;



    // If user job iops threshold is exceeded, iops throttling is enabled via cgroups.
    TNullable<i32> IopsThreshold;
    TNullable<i32> IopsThrottlerLimit;

    TDuration OrchidKeysUpdatePeriod;

    TDuration StaticOrchidCacheUpdatePeriod;

    // We use the same config for input chunk scraper and intermediate chunk scraper.
    NControllerAgent::TIntermediateChunkScraperConfigPtr ChunkScraper;

    // Enables job reporter to send job events/statistics etc.
    bool EnableJobReporter;

    // Enables job spec reporter to send job specs.
    bool EnableJobSpecReporter;

    // Timeout to try interrupt job before abort it.
    TDuration JobInterruptTimeout;

    // Total number of data slices in operation, summed up over all jobs.
    i64 MaxTotalSliceCount;

    // Config for operation alerts.
    TOperationAlertsConfigPtr OperationAlertsConfig;
    bool EnableUnrecognizedAlert;

    // Chunk size in per-controller row buffers.
    i64 ControllerRowBufferChunkSize;

    // Number of nodes to store by memory distribution.
    int MemoryDistributionDifferentNodeTypesThreshold;

    // Some special options for testing purposes.
    TTestingOptionsPtr TestingOptions;

    NCompression::ECodec JobSpecCodec;

    // How much time we wait before aborting the revived job that was not confirmed
    // by the corresponding execution node.
    TDuration JobRevivalAbortTimeout;

    // Controller agent-to-scheduler heartbeat period.
    TDuration ControllerAgentHeartbeatPeriod;

    // Controller agent-to-scheduler heartbeat timeout.
    TDuration ControllerAgentHeartbeatRpcTimeout;

    // Scheduler scheduler-to-agent operation request timeout.
    TDuration ControllerAgentOperationRpcTimeout;

    // Backoff to report job metrics from operation to scheduler.
    TDuration JobMetricsDeltaReportBackoff;

    // Cypress path to a special layer containing YT-specific data required to
    // run jobs with custom rootfs, e.g. statically linked job-satellite.
    // Is applied on top of user layers if they are used.
    TNullable<TString> SystemLayerPath;

    TSchedulerConfig();

private:
    template <class TOptions>
    void UpdateOptions(TOptions* options, NYT::NYTree::INodePtr patch);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

#define CONFIG_INL_H_
#include "config-inl.h"
#undef CONFIG_INL_H_
