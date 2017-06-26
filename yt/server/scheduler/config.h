#pragma once

#include "private.h"

#include <yt/server/job_proxy/config.h>

#include <yt/ytlib/api/config.h>

#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/ytlib/hive/config.h>

#include <yt/ytlib/ypath/public.h>

#include <yt/core/concurrency/config.h>

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TFairShareStrategyConfig
    : virtual public NYTree::TYsonSerializable
{
public:
    // The following settings can be overridden in operation spec.
    TDuration MinSharePreemptionTimeout;
    TDuration FairSharePreemptionTimeout;
    double FairShareStarvationTolerance;

    TDuration MinSharePreemptionTimeoutLimit;
    TDuration FairSharePreemptionTimeoutLimit;
    double FairShareStarvationToleranceLimit;

    TDuration FairShareUpdatePeriod;
    TDuration FairShareProfilingPeriod;
    TDuration FairShareLogPeriod;

    //! Any operation with less than this number of running jobs cannot be preempted.
    int MaxUnpreemptableRunningJobCount;

    //! Limit on number of operations in cluster.
    int MaxRunningOperationCount;
    int MaxOperationCount;

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

    //! Limit on the number of concurrent calls to ScheduleJob of single controller.
    int MaxConcurrentControllerScheduleJobCalls;

    //! Maximum allowed time for single job scheduling.
    TDuration ControllerScheduleJobTimeLimit;

    //! Backoff time after controller schedule job failure.
    TDuration ControllerScheduleJobFailBackoffTime;

    //! Backoff between schedule job statistics logging.
    TDuration ScheduleJobStatisticsLogBackoff;

    //! Thresholds to partition jobs of operation
    //! to preemptable, aggressively preemptable and non-preemptable lists.
    double PreemptionSatisfactionThreshold;
    double AggressivePreemptionSatisfactionThreshold;

    //! Allow failing a controller by passing testing option `controller_failure`
    //! in operation spec. Used only for testing purposes.
    bool EnableControllerFailureSpecOption;

    //! To investigate CPU load of node shard threads.
    bool EnableSchedulingTags;

    //! Backoff for printing tree scheduling info in heartbeat.
    TDuration HeartbeatTreeSchedulingInfoLogBackoff;

    //! Maximum number of ephemeral pools that can be created by user.
    int MaxEphemeralPoolsPerUser;

    //! If update of preemtable lists of operation takes more than that duration
    //! then this event will be logged.
    TDuration UpdatePreemptableListDurationLoggingThreshold;

    TFairShareStrategyConfig()
    {
        RegisterParameter("min_share_preemption_timeout", MinSharePreemptionTimeout)
            .Default(TDuration::Seconds(15));
        RegisterParameter("fair_share_preemption_timeout", FairSharePreemptionTimeout)
            .Default(TDuration::Seconds(30));
        RegisterParameter("fair_share_starvation_tolerance", FairShareStarvationTolerance)
            .InRange(0.0, 1.0)
            .Default(0.8);

        RegisterParameter("min_share_preemption_timeout_limit", MinSharePreemptionTimeoutLimit)
            .Default(TDuration::Seconds(15));
        RegisterParameter("fair_share_preemption_timeout_limit", FairSharePreemptionTimeoutLimit)
            .Default(TDuration::Seconds(30));
        RegisterParameter("fair_share_starvation_tolerance_limit", FairShareStarvationToleranceLimit)
            .InRange(0.0, 1.0)
            .Default(0.8);

        RegisterParameter("fair_share_update_period", FairShareUpdatePeriod)
            .InRange(TDuration::MilliSeconds(10), TDuration::Seconds(60))
            .Default(TDuration::MilliSeconds(1000));

        RegisterParameter("fair_share_profiling_period", FairShareProfilingPeriod)
            .InRange(TDuration::MilliSeconds(10), TDuration::Seconds(60))
            .Default(TDuration::MilliSeconds(5000));

        RegisterParameter("fair_share_log_period", FairShareLogPeriod)
            .InRange(TDuration::MilliSeconds(10), TDuration::Seconds(60))
            .Default(TDuration::MilliSeconds(1000));

        RegisterParameter("max_unpreemptable_running_job_count", MaxUnpreemptableRunningJobCount)
            .Default(10);

        RegisterParameter("max_running_operation_count", MaxRunningOperationCount)
            .Alias("max_running_operations")
            .Default(200)
            .GreaterThan(0);

        RegisterParameter("max_running_operation_count_per_pool", MaxRunningOperationCountPerPool)
            .Alias("max_running_operations_per_pool")
            .Default(50)
            .GreaterThan(0);

        RegisterParameter("max_operation_count_per_pool", MaxOperationCountPerPool)
            .Alias("max_operations_per_pool")
            .Default(50)
            .GreaterThan(0);

        RegisterParameter("max_operation_count", MaxOperationCount)
            .Default(1000)
            .GreaterThan(0);

        RegisterParameter("enable_pool_starvation", EnablePoolStarvation)
            .Default(true);

        RegisterParameter("default_parent_pool", DefaultParentPool)
            .Default(RootPoolName);

        RegisterParameter("forbid_immediate_operations_in_root", ForbidImmediateOperationsInRoot)
            .Default(true);

        RegisterParameter("job_count_preemption_timeout_coefficient", JobCountPreemptionTimeoutCoefficient)
            .Default(1.0)
            .GreaterThanOrEqual(1.0);

        RegisterParameter("max_concurrent_controller_schedule_job_calls", MaxConcurrentControllerScheduleJobCalls)
            .Default(10)
            .GreaterThan(0);

        RegisterParameter("schedule_job_time_limit", ControllerScheduleJobTimeLimit)
            .Default(TDuration::Seconds(60));

        RegisterParameter("schedule_job_fail_backoff_time", ControllerScheduleJobFailBackoffTime)
            .Default(TDuration::MilliSeconds(100));

        RegisterParameter("schedule_job_statistics_log_backoff", ScheduleJobStatisticsLogBackoff)
            .Default(TDuration::Seconds(1));

        RegisterParameter("preemption_satisfaction_threshold", PreemptionSatisfactionThreshold)
            .Default(1.0)
            .GreaterThan(0);

        RegisterParameter("aggressive_preemption_satisfaction_threshold", AggressivePreemptionSatisfactionThreshold)
            .Default(0.5)
            .GreaterThan(0);

        RegisterParameter("enable_controller_failure_spec_option", EnableControllerFailureSpecOption)
            .Default(false);

        RegisterParameter("enable_scheduling_tags", EnableSchedulingTags)
            .Default(true);

        RegisterParameter("heartbeat_tree_scheduling_info_log_period", HeartbeatTreeSchedulingInfoLogBackoff)
            .Default(TDuration::MilliSeconds(100));

        RegisterParameter("max_ephemeral_pools_per_user", MaxEphemeralPoolsPerUser)
            .GreaterThanOrEqual(1)
            .Default(5);

        RegisterParameter("update_preemptable_list_duration_logging_threshold", UpdatePreemptableListDurationLoggingThreshold)
            .Default(TDuration::MilliSeconds(100));

        RegisterValidator([&] () {
            if (AggressivePreemptionSatisfactionThreshold > PreemptionSatisfactionThreshold) {
                THROW_ERROR_EXCEPTION("Aggressive preemption satisfaction threshold must be less than preemption satisfaction threshold")
                    << TErrorAttribute("aggressive_threshold", AggressivePreemptionSatisfactionThreshold)
                    << TErrorAttribute("threshold", PreemptionSatisfactionThreshold);
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategyConfig)

////////////////////////////////////////////////////////////////////////////////

class TIntermediateChunkScraperConfig
    : public NChunkClient::TChunkScraperConfig
{
public:
    TDuration RestartTimeout;

    TIntermediateChunkScraperConfig()
    {
        RegisterParameter("restart_timeout", RestartTimeout)
            .Default(TDuration::Seconds(10));
    }
};

DEFINE_REFCOUNTED_TYPE(TIntermediateChunkScraperConfig)

////////////////////////////////////////////////////////////////////////////////

class TEventLogConfig
    : public NTableClient::TBufferedTableWriterConfig
{
public:
    NYPath::TYPath Path;

    TEventLogConfig()
    {
        RegisterParameter("path", Path)
            .Default("//sys/scheduler/event_log");
    }
};

DEFINE_REFCOUNTED_TYPE(TEventLogConfig)

////////////////////////////////////////////////////////////////////////////////

class TJobSizeAdjusterConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MinJobTime;
    TDuration MaxJobTime;

    double ExecToPrepareTimeRatio;

    TJobSizeAdjusterConfig()
    {
        RegisterParameter("min_job_time", MinJobTime)
            .Default(TDuration::Seconds(60));

        RegisterParameter("max_job_time", MaxJobTime)
            .Default(TDuration::Minutes(10));

        RegisterParameter("exec_to_prepare_time_ratio", ExecToPrepareTimeRatio)
            .Default(20.0);
    }
};

DEFINE_REFCOUNTED_TYPE(TJobSizeAdjusterConfig)

////////////////////////////////////////////////////////////////////////////////

class TJobSplitterConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MinJobTime;
    double ExecToPrepareTimeRatio;
    i64 MinTotalDataSize;
    TDuration UpdatePeriod;
    TDuration MedianExcessDuration;
    double CandidatePercentile;
    int MaxJobsPerSplit;

    TJobSplitterConfig()
    {
        RegisterParameter("min_job_time", MinJobTime)
            .Default(TDuration::Seconds(60));

        RegisterParameter("exec_to_prepare_time_ratio", ExecToPrepareTimeRatio)
            .Default(20.0);

        RegisterParameter("min_total_data_size", MinTotalDataSize)
            .Default((i64)1024 * 1024 * 1024);

        RegisterParameter("update_period", UpdatePeriod)
            .Default(TDuration::Seconds(60));

        RegisterParameter("median_excess_duration", MedianExcessDuration)
            .Default(TDuration::Minutes(3));

        RegisterParameter("candidate_percentile", CandidatePercentile)
            .GreaterThanOrEqual(0.5)
            .LessThanOrEqual(1.0)
            .Default(0.8);

        RegisterParameter("max_jobs_per_split", MaxJobsPerSplit)
            .GreaterThan(0)
            .Default(5);
    }
};

DEFINE_REFCOUNTED_TYPE(TJobSplitterConfig)

////////////////////////////////////////////////////////////////////////////////

class TOperationOptions
    : public NYTree::TYsonSerializable
{
public:
    NYTree::INodePtr SpecTemplate;

    //! Controls finer initial slicing of input data to ensure even distribution of data split sizes among jobs.
    double SliceDataSizeMultiplier;

    //! Maximum number of primary data slices per job.
    int MaxDataSlicesPerJob;

    i64 MaxSliceDataSize;
    i64 MinSliceDataSize;

    //! Maximum number of output tables times job count an operation can have.
    int MaxOutputTablesTimesJobsCount;

    TOperationOptions()
    {
        RegisterParameter("spec_template", SpecTemplate)
            .Default()
            .MergeBy(NYTree::EMergeStrategy::Combine);

        RegisterParameter("slice_data_size_multiplier", SliceDataSizeMultiplier)
            .Default(0.51)
            .GreaterThan(0.0);

        RegisterParameter("max_data_slices_per_job", MaxDataSlicesPerJob)
            .Default(100000)
            .GreaterThan(0);

        RegisterParameter("max_slice_data_size", MaxSliceDataSize)
            .Default((i64)256 * 1024 * 1024)
            .GreaterThan(0);

        RegisterParameter("min_slice_data_size", MinSliceDataSize)
            .Default((i64)1 * 1024 * 1024)
            .GreaterThan(0);

        RegisterParameter("max_output_tables_times_jobs_count", MaxOutputTablesTimesJobsCount)
            .Default(20 * 100000)
            .GreaterThanOrEqual(100000);

        RegisterValidator([&] () {
            if (MaxSliceDataSize < MinSliceDataSize) {
                THROW_ERROR_EXCEPTION("Minimum slice data size must be less than or equal to maximum slice data size")
                    << TErrorAttribute("min_slice_data_size", MinSliceDataSize)
                    << TErrorAttribute("max_slice_data_size", MaxSliceDataSize);
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TOperationOptions)

class TSimpleOperationOptions
    : public TOperationOptions
{
public:
    int MaxJobCount;
    i64 DataSizePerJob;

    TSimpleOperationOptions()
    {
        RegisterParameter("max_job_count", MaxJobCount)
            .Default(100000);

        RegisterParameter("data_size_per_job", DataSizePerJob)
            .Default((i64) 256 * 1024 * 1024)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TSimpleOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TMapOperationOptions
    : public TSimpleOperationOptions
{
public:
    TJobSizeAdjusterConfigPtr JobSizeAdjuster;
    TJobSplitterConfigPtr JobSplitter;

    TMapOperationOptions()
    {
        RegisterParameter("job_size_adjuster", JobSizeAdjuster)
            .DefaultNew();
        RegisterParameter("job_splitter", JobSplitter)
            .DefaultNew();

        RegisterInitializer([&] () {
            DataSizePerJob = (i64) 128 * 1024 * 1024;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TMapOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TUnorderedMergeOperationOptions
    : public TSimpleOperationOptions
{ };

DEFINE_REFCOUNTED_TYPE(TUnorderedMergeOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TOrderedMergeOperationOptions
    : public TSimpleOperationOptions
{ };

DEFINE_REFCOUNTED_TYPE(TOrderedMergeOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TSortedMergeOperationOptions
    : public TSimpleOperationOptions
{ };

DEFINE_REFCOUNTED_TYPE(TSortedMergeOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TReduceOperationOptions
    : public TSortedMergeOperationOptions
{
public:
    TJobSplitterConfigPtr JobSplitter;

    TReduceOperationOptions()
    {
        RegisterParameter("job_splitter", JobSplitter)
            .DefaultNew();

        RegisterInitializer([&] () {
            DataSizePerJob = (i64) 128 * 1024 * 1024;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TReduceOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TJoinReduceOperationOptions
    : public TReduceOperationOptions
{ };

DEFINE_REFCOUNTED_TYPE(TJoinReduceOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TEraseOperationOptions
    : public TOrderedMergeOperationOptions
{ };

DEFINE_REFCOUNTED_TYPE(TEraseOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TSortOperationOptionsBase
    : public TOperationOptions
{
public:
    int MaxPartitionJobCount;
    int MaxPartitionCount;
    i64 SortJobMaxSliceDataSize;
    i64 PartitionJobMaxSliceDataSize;
    i32 MaxSampleSize;
    i64 CompressedBlockSize;
    i64 MinPartitionSize;
    i64 MinUncompressedBlockSize;
    TJobSizeAdjusterConfigPtr PartitionJobSizeAdjuster;

    TSortOperationOptionsBase()
    {
        RegisterParameter("max_partition_job_count", MaxPartitionJobCount)
            .Default(100000)
            .GreaterThan(0);

        RegisterParameter("max_partition_count", MaxPartitionCount)
            .Default(10000)
            .GreaterThan(0);

        RegisterParameter("partition_job_max_slice_data_size", PartitionJobMaxSliceDataSize)
            .Default((i64)256 * 1024 * 1024)
            .GreaterThan(0);

        RegisterParameter("sort_job_max_slice_data_size", SortJobMaxSliceDataSize)
            .Default((i64)256 * 1024 * 1024)
            .GreaterThan(0);

        RegisterParameter("max_sample_size", MaxSampleSize)
            .Default(10 * 1024)
            .GreaterThanOrEqual(1024)
            // NB(psushin): removing this validator may lead to weird errors in sorting.
            .LessThanOrEqual(NTableClient::MaxSampleSize);

        RegisterParameter("compressed_block_size", CompressedBlockSize)
            .Default(1 * 1024 * 1024)
            .GreaterThanOrEqual(1024);

        RegisterParameter("min_partition_size", MinPartitionSize)
            .Default(256 * 1024 * 1024)
            .GreaterThanOrEqual(1);

        // Minimum is 1 for tests.
        RegisterParameter("min_uncompressed_block_size", MinUncompressedBlockSize)
            .Default(1024 * 1024)
            .GreaterThanOrEqual(1);

        RegisterParameter("partition_job_size_adjuster", PartitionJobSizeAdjuster)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TSortOperationOptionsBase)

////////////////////////////////////////////////////////////////////////////////

class TSortOperationOptions
    : public TSortOperationOptionsBase
{ };

DEFINE_REFCOUNTED_TYPE(TSortOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TMapReduceOperationOptions
    : public TSortOperationOptionsBase
{ };

DEFINE_REFCOUNTED_TYPE(TMapReduceOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TRemoteCopyOperationOptions
    : public TSimpleOperationOptions
{ };

DEFINE_REFCOUNTED_TYPE(TRemoteCopyOperationOptions)

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

    TTestingOptions()
    {
        RegisterParameter("enable_random_master_disconnection", EnableRandomMasterDisconnection)
            .Default(false);
        RegisterParameter("random_master_disconnection_max_backoff", RandomMasterDisconnectionMaxBackoff)
            .Default(TDuration::Seconds(5));
        RegisterParameter("master_disconnect_delay", MasterDisconnectDelay)
            .Default(Null);
        RegisterParameter("enable_snapshot_cycle_after_materialization", EnableSnapshotCycleAfterMaterialization)
            .Default(false);
        RegisterParameter("finish_operation_transition_delay", FinishOperationTransitionDelay)
            .Default(Null);
    }
};

DEFINE_REFCOUNTED_TYPE(TTestingOptions)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerConfig
    : public TFairShareStrategyConfig
    , public NChunkClient::TChunkTeleporterConfig
{
public:
    //! Number of threads for running controllers invokers.
    int ControllerThreadCount;

    //! Number of threads for retrieving important fields from job statistics.
    int StatisticsAnalyzerThreadCount;

    //! Number of threads for building job specs.
    int JobSpecBuilderThreadCount;

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

    TDuration ProfilingUpdatePeriod;

    TDuration AlertsUpdatePeriod;

    NHiveClient::TClusterDirectorySynchronizerConfigPtr ClusterDirectorySynchronizer;

    TDuration NodeShardsUpdatePeriod;

    TDuration ResourceDemandSanityCheckPeriod;

    TDuration LockTransactionTimeout;

    TDuration OperationTransactionTimeout;

    //! Timeout on operation initialization.
    //! It is needed to prevent hanging of remote copy when remote cluster is unavailable.
    TDuration OperationInitializationTimeout;

    TDuration JobProberRpcTimeout;

    TDuration OperationControllerSuspendTimeout;

    TDuration OperationLogProgressBackoff;

    TDuration ClusterInfoLoggingPeriod;

    TDuration PendingEventLogRowsFlushPeriod;

    TDuration UpdateExecNodeDescriptorsPeriod;

    TDuration OperationTimeLimitCheckPeriod;

    TDuration AvailableExecNodesCheckPeriod;

    TDuration OperationBuildProgressPeriod;

    TDuration TaskUpdatePeriod;

    //! Jobs running on node are logged periodically or when they change their state.
    TDuration JobsLoggingPeriod;

    //! Statistics and resource usages of jobs running on a node are updated
    //! not more often then this period.
    TDuration RunningJobsUpdatePeriod;

    //! Missing jobs are checked not more often then this period.
    TDuration CheckMissingJobsPeriod;

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

    //! Minimum time between two consecutive chunk list release requests
    //! until number of chunk lists to release less that desired chunk lists to release.
    //! This option necessary to prevent chunk list release storm.
    TDuration ChunkListReleaseBatchDelay;

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

    //! Timeout to store cached value of exec nodes information
    //! for scheduling tag filter without access.
    TDuration SchedulingTagFilterExpireTimeout;

    //! Time between two consecutive calls in node shard to calculate exec nodes list.
    TDuration NodeShardUpdateExecNodesInformationDelay;

    //! Maximum number of foreign chunks to locate per request.
    int MaxChunksPerLocateRequest;

    //! Limit on the number of concurrent core dumps that can be written because
    //! of failed safe assertions inside controllers.
    int MaxConcurrentSafeCoreDumps;

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

    TString SnapshotTempPath;
    NApi::TFileReaderConfigPtr SnapshotReader;
    NApi::TFileWriterConfigPtr SnapshotWriter;

    NChunkClient::TFetcherConfigPtr Fetcher;

    TEventLogConfigPtr EventLog;

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

    // Duration of no activity by job to be considered as suspicious.
    TDuration SuspiciousInactivityTimeout;

    // Cpu usage delta that is considered insignificant when checking if job is suspicious.
    i64 SuspiciousCpuUsageThreshold;
    // Time fraction spent in idle state enough for job to be considered suspicious.
    double SuspiciousInputPipeIdleTimeFraction;

    // If user job iops threshold is exceeded, iops throttling is enabled via cgroups.
    TNullable<i32> IopsThreshold;
    TNullable<i32> IopsThrottlerLimit;

    TDuration StaticOrchidCacheUpdatePeriod;

    // We use the same config for input chunk scraper and intermediate chunk scraper.
    TIntermediateChunkScraperConfigPtr ChunkScraper;

    // Enables statistics reporter to send job events/statistics/specs etc.
    bool EnableStatisticsReporter;

    // Timeout to try interrupt job before abort it.
    TDuration JobInterruptTimeout;

    // Total number of data slices in operation, summed up over all jobs.
    i64 MaxTotalSliceCount;

    // Chunk size in per-controller row buffers.
    i64 ControllerRowBufferChunkSize;

    // Filter of main nodes, used to calculate resource limits in fair share strategy.
    TDnfFormula MainNodesFilterFormula;
    TSchedulingTagFilter MainNodesFilter;

    // Some special options for testing purposes.
    TTestingOptionsPtr TestingOptions;

    NCompression::ECodec JobSpecCodec;

    TSchedulerConfig()
    {
        RegisterParameter("controller_thread_count", ControllerThreadCount)
            .Default(4)
            .GreaterThan(0);
        RegisterParameter("statistics_analyzer_thread_count", StatisticsAnalyzerThreadCount)
            .Default(2)
            .GreaterThan(0);
        RegisterParameter("job_spec_builder_thread_count", JobSpecBuilderThreadCount)
            .Default(8)
            .GreaterThan(0);
        RegisterParameter("parallel_snapshot_builder_count", ParallelSnapshotBuilderCount)
            .Default(4)
            .GreaterThan(0);
        RegisterParameter("node_shard_count", NodeShardCount)
            .Default(4)
            .GreaterThan(0);

        RegisterParameter("connect_retry_backoff_time", ConnectRetryBackoffTime)
            .Default(TDuration::Seconds(15));
        RegisterParameter("node_heartbeat_timeout", NodeHeartbeatTimeout)
            .Default(TDuration::Seconds(60));
        RegisterParameter("transactions_refresh_period", TransactionsRefreshPeriod)
            .Default(TDuration::Seconds(3));
        RegisterParameter("operations_update_period", OperationsUpdatePeriod)
            .Default(TDuration::Seconds(3));
        RegisterParameter("watchers_update_period", WatchersUpdatePeriod)
            .Default(TDuration::Seconds(3));
        RegisterParameter("profiling_update_period", ProfilingUpdatePeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("alerts_update_period", AlertsUpdatePeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("cluster_directory_synchronizer", ClusterDirectorySynchronizer)
            .DefaultNew();
        RegisterParameter("node_shards_update_period", NodeShardsUpdatePeriod)
            .Default(TDuration::Seconds(10));

        RegisterParameter("resource_demand_sanity_check_period", ResourceDemandSanityCheckPeriod)
            .Default(TDuration::Seconds(15));
        RegisterParameter("lock_transaction_timeout", LockTransactionTimeout)
            .Default(TDuration::Seconds(15));
        RegisterParameter("operation_initialization_timeout", OperationInitializationTimeout)
            .Default(TDuration::Seconds(60));
        RegisterParameter("operation_transaction_timeout", OperationTransactionTimeout)
            .Default(TDuration::Minutes(60));
        RegisterParameter("job_prober_rpc_timeout", JobProberRpcTimeout)
            .Default(TDuration::Seconds(300));

        RegisterParameter("operation_controller_suspend_timeout", OperationControllerSuspendTimeout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("operation_progress_log_backoff", OperationLogProgressBackoff)
            .Default(TDuration::Seconds(1));

        RegisterParameter("task_update_period", TaskUpdatePeriod)
            .Default(TDuration::Seconds(3));

        RegisterParameter("cluster_info_logging_period", ClusterInfoLoggingPeriod)
            .Default(TDuration::Seconds(1));

        RegisterParameter("pending_event_log_rows_flush_period", PendingEventLogRowsFlushPeriod)
            .Default(TDuration::Seconds(1));

        RegisterParameter("update_exec_node_descriptors_period", UpdateExecNodeDescriptorsPeriod)
            .Default(TDuration::Seconds(10));


        RegisterParameter("operation_time_limit_check_period", OperationTimeLimitCheckPeriod)
            .Default(TDuration::Seconds(1));

        RegisterParameter("available_exec_nodes_check_period", AvailableExecNodesCheckPeriod)
            .Default(TDuration::Seconds(5));

        RegisterParameter("operation_build_progress_period", OperationBuildProgressPeriod)
            .Default(TDuration::Seconds(3));

        RegisterParameter("jobs_logging_period", JobsLoggingPeriod)
            .Default(TDuration::Seconds(30));

        RegisterParameter("running_jobs_update_period", RunningJobsUpdatePeriod)
            .Default(TDuration::Seconds(10));

        RegisterParameter("check_missing_jobs_period", CheckMissingJobsPeriod)
            .Default(TDuration::Seconds(10));

        RegisterParameter("operation_time_limit", OperationTimeLimit)
            .Default();

        RegisterParameter("max_job_nodes_per_operation", MaxJobNodesPerOperation)
            .Default(200)
            .GreaterThanOrEqual(0)
            .LessThanOrEqual(250);

        RegisterParameter("chunk_list_preallocation_count", ChunkListPreallocationCount)
            .Default(128)
            .GreaterThanOrEqual(0);
        RegisterParameter("max_chunk_list_allocation_count", MaxChunkListAllocationCount)
            .Default(16384)
            .GreaterThanOrEqual(0);
        RegisterParameter("chunk_list_watermark_count", ChunkListWatermarkCount)
            .Default(50)
            .GreaterThanOrEqual(0);
        RegisterParameter("chunk_list_allocation_multiplier", ChunkListAllocationMultiplier)
            .Default(2.0)
            .GreaterThan(1.0);
        RegisterParameter("chunk_list_release_batch_delay", ChunkListReleaseBatchDelay)
            .Default(TDuration::Seconds(30));
        RegisterParameter("desired_chunk_lists_per_release", DesiredChunkListsPerRelease)
            .Default(1000);

        RegisterParameter("max_chunks_per_fetch", MaxChunksPerFetch)
            .Default(100000)
            .GreaterThan(0);

        RegisterParameter("max_children_per_attach_request", MaxChildrenPerAttachRequest)
            .Default(10000)
            .GreaterThan(0);

        RegisterParameter("max_file_size", MaxFileSize)
            .Default((i64) 10 * 1024 * 1024 * 1024);

        RegisterParameter("max_input_table_count", MaxInputTableCount)
            .Default(1000)
            .GreaterThan(0);

        RegisterParameter("max_ranges_on_table", MaxRangesOnTable)
            .Default(1000)
            .GreaterThan(0);

        RegisterParameter("max_user_file_count", MaxUserFileCount)
            .Default(1000)
            .GreaterThan(0);

        RegisterParameter("max_started_jobs_per_heartbeat", MaxStartedJobsPerHeartbeat)
            .Default()
            .GreaterThan(0);

        RegisterParameter("max_concurrent_safe_core_dumps", MaxConcurrentSafeCoreDumps)
            .Default(1)
            .GreaterThanOrEqual(0);

        RegisterParameter("safe_online_node_count", SafeOnlineNodeCount)
            .GreaterThanOrEqual(0)
            .Default(1);

        RegisterParameter("safe_scheduler_online_time", SafeSchedulerOnlineTime)
            .Default(TDuration::Minutes(10));

        RegisterParameter("controller_update_exec_nodes_information_delay", ControllerUpdateExecNodesInformationDelay)
            .Default(TDuration::Seconds(30));

        RegisterParameter("scheduling_tag_filter_expire_timeout", SchedulingTagFilterExpireTimeout)
            .Default(TDuration::Hours(1));

        RegisterParameter("max_chunks_per_locate_request", MaxChunksPerLocateRequest)
            .GreaterThan(0)
            .Default(10000);

        RegisterParameter("operation_options", OperationOptions)
            .Default()
            .MergeBy(NYTree::EMergeStrategy::Combine);

        RegisterParameter("map_operation_options", MapOperationOptions)
            .DefaultNew();
        RegisterParameter("reduce_operation_options", ReduceOperationOptions)
            .DefaultNew();
        RegisterParameter("join_reduce_operation_options", JoinReduceOperationOptions)
            .DefaultNew();
        RegisterParameter("erase_operation_options", EraseOperationOptions)
            .DefaultNew();
        RegisterParameter("ordered_merge_operation_options", OrderedMergeOperationOptions)
            .DefaultNew();
        RegisterParameter("unordered_merge_operation_options", UnorderedMergeOperationOptions)
            .DefaultNew();
        RegisterParameter("sorted_merge_operation_options", SortedMergeOperationOptions)
            .DefaultNew();
        RegisterParameter("map_reduce_operation_options", MapReduceOperationOptions)
            .DefaultNew();
        RegisterParameter("sort_operation_options", SortOperationOptions)
            .DefaultNew();
        RegisterParameter("remote_copy_operation_options", RemoteCopyOperationOptions)
            .DefaultNew();

        RegisterParameter("environment", Environment)
            .Default(yhash<TString, TString>())
            .MergeBy(NYTree::EMergeStrategy::Combine);

        RegisterParameter("snapshot_timeout", SnapshotTimeout)
            .Default(TDuration::Seconds(60));
        RegisterParameter("snapshot_period", SnapshotPeriod)
            .Default(TDuration::Seconds(300));
        RegisterParameter("enable_snapshot_building", EnableSnapshotBuilding)
            .Default(true);
        RegisterParameter("enable_snapshot_loading", EnableSnapshotLoading)
            .Default(false);
        RegisterParameter("snapshot_temp_path", SnapshotTempPath)
            .NonEmpty()
            .Default("/tmp/yt/scheduler/snapshots");
        RegisterParameter("snapshot_reader", SnapshotReader)
            .DefaultNew();
        RegisterParameter("snapshot_writer", SnapshotWriter)
            .DefaultNew();

        RegisterParameter("fetcher", Fetcher)
            .DefaultNew();
        RegisterParameter("event_log", EventLog)
            .DefaultNew();

        RegisterParameter("chunk_location_throttler", ChunkLocationThrottler)
            .DefaultNew();

        RegisterParameter("udf_registry_path", UdfRegistryPath)
            .Default(Null);

        RegisterParameter("heartbeat_process_backoff", HeartbeatProcessBackoff)
            .Default(TDuration::MilliSeconds(5000));
        RegisterParameter("soft_concurrent_heartbeat_limit", SoftConcurrentHeartbeatLimit)
            .Default(50)
            .GreaterThanOrEqual(1);
        RegisterParameter("hard_concurrent_heartbeat_limit", HardConcurrentHeartbeatLimit)
            .Default(100)
            .GreaterThanOrEqual(1);

        RegisterParameter("job_spec_slice_throttler", JobSpecSliceThrottler)
            .Default(New<NConcurrency::TThroughputThrottlerConfig>(500000));
        RegisterParameter("heavy_job_spec_slice_count_threshold", HeavyJobSpecSliceCountThreshold)
            .Default(1000)
            .GreaterThan(0);

        RegisterParameter("enable_tmpfs", EnableTmpfs)
            .Default(true);
        RegisterParameter("enable_map_job_size_adjustment", EnableMapJobSizeAdjustment)
            .Default(true);
        RegisterParameter("enable_job_splitting", EnableJobSplitting)
            .Default(true);

        RegisterParameter("additional_intermediate_data_acl", AdditionalIntermediateDataAcl)
            .Default(NYTree::BuildYsonNodeFluently()
                .BeginList()
                .EndList()->AsList());

        //! By default we disable job size adjustment for partition maps,
        //! since it may lead to partition data skew between nodes.
        RegisterParameter("enable_partition_map_job_size_adjustment", EnablePartitionMapJobSizeAdjustment)
            .Default(false);

        RegisterParameter("user_job_memory_digest_precision", UserJobMemoryDigestPrecision)
            .Default(0.01)
            .GreaterThan(0);
        RegisterParameter("user_job_memory_reserve_quantile", UserJobMemoryReserveQuantile)
            .InRange(0.0, 1.0)
            .Default(0.95);
        RegisterParameter("job_proxy_memory_reserve_quantile", JobProxyMemoryReserveQuantile)
            .InRange(0.0, 1.0)
            .Default(0.95);
        RegisterParameter("resource_overdraft_factor", ResourceOverdraftFactor)
            .InRange(1.0, 10.0)
            .Default(1.1);

        RegisterParameter("suspicious_inactivity_timeout", SuspiciousInactivityTimeout)
            .Default(TDuration::Minutes(1));
        RegisterParameter("suspicious_cpu_usage_threshold", SuspiciousCpuUsageThreshold)
            .Default(300);
        RegisterParameter("suspicious_input_pipe_time_idle_fraction", SuspiciousInputPipeIdleTimeFraction)
            .Default(0.95);

        RegisterParameter("static_orchid_cache_update_period", StaticOrchidCacheUpdatePeriod)
            .Default(TDuration::Seconds(1));

        RegisterParameter("iops_threshold", IopsThreshold)
            .Default(Null);
        RegisterParameter("iops_throttler_limit", IopsThrottlerLimit)
            .Default(Null);

        RegisterParameter("chunk_scraper", ChunkScraper)
            .DefaultNew();

        RegisterParameter("enable_statistics_reporter", EnableStatisticsReporter)
            .Default(false);

        RegisterParameter("job_interrupt_timeout", JobInterruptTimeout)
            .Default(TDuration::Seconds(10));

        RegisterParameter("max_total_slice_count", MaxTotalSliceCount)
            .Default((i64) 10 * 1000 * 1000)
            .GreaterThan(0);

        RegisterParameter("controller_row_buffer_chunk_size", ControllerRowBufferChunkSize)
            .Default((i64) 64 * 1024)
            .GreaterThan(0);

        RegisterParameter("main_nodes_filter", MainNodesFilterFormula)
            .Default();

        RegisterParameter("testing_options", TestingOptions)
            .DefaultNew();

        RegisterParameter("job_spec_codec", JobSpecCodec)
            .Default(NCompression::ECodec::Lz4);

        RegisterInitializer([&] () {
            ChunkLocationThrottler->Limit = 10000;

            EventLog->MaxRowWeight = (i64) 128 * 1024 * 1024;
        });

        RegisterValidator([&] () {
            if (SoftConcurrentHeartbeatLimit > HardConcurrentHeartbeatLimit) {
                THROW_ERROR_EXCEPTION("Soft limit on concurrent heartbeats must be less than or equal to hard limit on concurrent heartbeats")
                    << TErrorAttribute("soft_limit", SoftConcurrentHeartbeatLimit)
                    << TErrorAttribute("hard_limit", HardConcurrentHeartbeatLimit);
            }
        });
    }

    virtual void OnLoaded() override
    {
        UpdateOptions(&MapOperationOptions, OperationOptions);
        UpdateOptions(&ReduceOperationOptions, OperationOptions);
        UpdateOptions(&JoinReduceOperationOptions, OperationOptions);
        UpdateOptions(&EraseOperationOptions, OperationOptions);
        UpdateOptions(&OrderedMergeOperationOptions, OperationOptions);
        UpdateOptions(&UnorderedMergeOperationOptions, OperationOptions);
        UpdateOptions(&SortedMergeOperationOptions, OperationOptions);
        UpdateOptions(&MapReduceOperationOptions, OperationOptions);
        UpdateOptions(&SortOperationOptions, OperationOptions);
        UpdateOptions(&RemoteCopyOperationOptions, OperationOptions);

        MainNodesFilter.Reload(MainNodesFilterFormula);
    }

private:
    template <class TOptions>
    void UpdateOptions(TOptions* options, NYT::NYTree::INodePtr patch)
    {
        using NYTree::INodePtr;
        using NYTree::ConvertTo;

        if (!patch) {
            return;
        }

        if (*options) {
            *options = ConvertTo<TOptions>(UpdateNode(patch, ConvertTo<INodePtr>(*options)));
        } else {
            *options = ConvertTo<TOptions>(patch);
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
