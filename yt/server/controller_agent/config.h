#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/api/config.h>

#include <yt/ytlib/event_log/config.h>

#include <yt/core/concurrency/config.h>

#include <yt/core/ytree/yson_serializable.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/misc/phoenix.h>

namespace NYT {
namespace NControllerAgent {

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

// TODO(babenko): split further
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

    TOperationAlertsConfig()
    {
        RegisterParameter("tmpfs_alert_max_unused_space_ratio", TmpfsAlertMaxUnusedSpaceRatio)
            .InRange(0.0, 1.0)
            .Default(0.2);

        RegisterParameter("tmpfs_alert_min_unused_space_threshold", TmpfsAlertMinUnusedSpaceThreshold)
            .Default(512_MB)
            .GreaterThan(0);

        RegisterParameter("aborted_jobs_alert_max_aborted_time", AbortedJobsAlertMaxAbortedTime)
            .Default((i64) 10 * 60 * 1000)
            .GreaterThan(0);

        RegisterParameter("aborted_jobs_alert_max_aborted_time_ratio", AbortedJobsAlertMaxAbortedTimeRatio)
            .InRange(0.0, 1.0)
            .Default(0.25);

        RegisterParameter("short_jobs_alert_min_job_duration", ShortJobsAlertMinJobDuration)
            .Default(TDuration::Minutes(1));

        RegisterParameter("short_jobs_alert_min_job_count", ShortJobsAlertMinJobCount)
            .Default(1000);

        RegisterParameter("intermediate_data_skew_alert_min_partition_size", IntermediateDataSkewAlertMinPartitionSize)
            .Default(10_GB)
            .GreaterThan(0);

        RegisterParameter("intermediate_data_skew_alert_min_interquartile_range", IntermediateDataSkewAlertMinInterquartileRange)
            .Default(1_GB)
            .GreaterThan(0);

        RegisterParameter("job_spec_throttling_alert_activation_count_threshold", JobSpecThrottlingAlertActivationCountThreshold)
            .Default(1000)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TOperationAlertsConfig)

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

    TJobSplitterConfig()
    {
        RegisterParameter("min_job_time", MinJobTime)
            .Default(TDuration::Seconds(60));

        RegisterParameter("exec_to_prepare_time_ratio", ExecToPrepareTimeRatio)
            .Default(20.0);

        RegisterParameter("min_total_data_weight", MinTotalDataWeight)
            .Alias("min_total_data_size")
            .Default(1_GB);

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

    TOperationOptions()
    {
        RegisterParameter("spec_template", SpecTemplate)
            .Default()
            .MergeBy(NYTree::EMergeStrategy::Combine);

        RegisterParameter("slice_data_weight_multiplier", SliceDataWeightMultiplier)
            .Alias("slice_data_size_multiplier")
            .Default(0.51)
            .GreaterThan(0.0);

        RegisterParameter("max_data_slices_per_job", MaxDataSlicesPerJob)
            // This is a reasonable default for jobs with user code.
            // Defaults for system jobs are in Initializer.
            .Default(1000)
            .GreaterThan(0);

        RegisterParameter("max_slice_data_weight", MaxSliceDataWeight)
            .Alias("max_slice_data_size")
            .Default(1_GB)
            .GreaterThan(0);

        RegisterParameter("min_slice_data_weight", MinSliceDataWeight)
            .Alias("min_slice_data_size")
            .Default(1_MB)
            .GreaterThan(0);

        RegisterParameter("max_output_tables_times_jobs_count", MaxOutputTablesTimesJobsCount)
            .Default(20 * 100000)
            .GreaterThanOrEqual(100000);

        RegisterValidator([&] () {
            if (MaxSliceDataWeight < MinSliceDataWeight) {
                THROW_ERROR_EXCEPTION("Minimum slice data weight must be less than or equal to maximum slice data size")
                        << TErrorAttribute("min_slice_data_weight", MinSliceDataWeight)
                        << TErrorAttribute("max_slice_data_weight", MaxSliceDataWeight);
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TSimpleOperationOptions
    : public TOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSimpleOperationOptions, 0x875251fa);

public:
    int MaxJobCount;
    i64 DataWeightPerJob;

    TSimpleOperationOptions()
    {
        RegisterParameter("max_job_count", MaxJobCount)
            .Default(100000);

        RegisterParameter("data_weight_per_job", DataWeightPerJob)
            .Alias("data_size_per_job")
            .Default(256_MB)
            .GreaterThan(0);
    }
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
    TJobSplitterConfigPtr JobSplitter;

    TMapOperationOptions()
    {
        RegisterParameter("job_size_adjuster", JobSizeAdjuster)
            .DefaultNew();
        RegisterParameter("job_splitter", JobSplitter)
            .DefaultNew();

        RegisterInitializer([&] () {
            DataWeightPerJob = 128_MB;
        });
    }
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
    TJobSplitterConfigPtr JobSplitter;

    TReduceOperationOptions()
    {
        RegisterParameter("job_splitter", JobSplitter)
            .DefaultNew();

        RegisterInitializer([&] () {
            DataWeightPerJob = 128_MB;
        });
    }
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

    TSortOperationOptionsBase()
    {
        RegisterParameter("max_partition_job_count", MaxPartitionJobCount)
            .Default(100000)
            .GreaterThan(0);

        RegisterParameter("max_partition_count", MaxPartitionCount)
            .Default(10000)
            .GreaterThan(0);

        RegisterParameter("max_sample_size", MaxSampleSize)
            .Default(10_KB)
            .GreaterThanOrEqual(1_KB)
                // NB(psushin): removing this validator may lead to weird errors in sorting.
            .LessThanOrEqual(NTableClient::MaxSampleSize);

        RegisterParameter("compressed_block_size", CompressedBlockSize)
            .Default(1_MB)
            .GreaterThanOrEqual(1_KB);

        RegisterParameter("min_partition_weight", MinPartitionWeight)
            .Alias("min_partition_size")
            .Default(256_MB)
            .GreaterThanOrEqual(1);

        // Minimum is 1 for tests.
        RegisterParameter("min_uncompressed_block_size", MinUncompressedBlockSize)
            .Default(100_KB)
            .GreaterThanOrEqual(1);

        RegisterParameter("partition_job_size_adjuster", PartitionJobSizeAdjuster)
            .DefaultNew();
    }
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

class TControllerAgentConfig
    : public NChunkClient::TChunkTeleporterConfig
{
public:
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

    //! If |true|, snapshots are periodically constructed and uploaded into the system.
    bool EnableSnapshotBuilding;

    //! Interval between consequent snapshots.
    TDuration SnapshotPeriod;

    //! Timeout for snapshot construction.
    TDuration SnapshotTimeout;

    //! Timeout to wait for controller suspension before constructing a snapshot.
    TDuration OperationControllerSuspendTimeout;

    //! Number of parallel operation snapshot builders.
    int ParallelSnapshotBuilderCount;

    //! Configuration for uploading snapshots to Cypress.
    NApi::TFileWriterConfigPtr SnapshotWriter;

    //! If |true|, snapshots are loaded during revival.
    bool EnableSnapshotLoading;

    //! Configuration for downloading snapshots from Cypress.
    NApi::TFileReaderConfigPtr SnapshotReader;

    TDuration TransactionsRefreshPeriod;
    TDuration OperationsUpdatePeriod;
    TDuration ChunkUnstagePeriod;

    //! Maximum number of chunk trees to attach per request.
    int MaxChildrenPerAttachRequest;

    //! Limits the rate (measured in chunks) of location requests issued by all active chunk scrapers.
    NConcurrency::TThroughputThrottlerConfigPtr ChunkLocationThrottler;

    NEventLog::TEventLogConfigPtr EventLog;

    // Controller agent-to-scheduler heartbeat period.
    TDuration ControllerAgentHeartbeatPeriod;

    //! Controller agent-to-scheduler heartbeat timeout.
    TDuration ControllerAgentHeartbeatRpcTimeout;

    //! Period between requesting exec nodes from scheduler.
    TDuration ExecNodesRequestPeriod;

    //! Number of threads for running controllers invokers.
    int ControllerThreadCount;

    //! Limit on the number of concurrent core dumps that can be written because
    //! of failed safe assertions inside controllers.
    int MaxConcurrentSafeCoreDumps;

    //! Timeout to store cached value of exec nodes information
    //! for scheduling tag filter without access.
    TDuration SchedulingTagFilterExpireTimeout;

    //! Duration of no activity by job to be considered as suspicious.
    TDuration SuspiciousInactivityTimeout;

    //! Cpu usage delta that is considered insignificant when checking if job is suspicious.
    i64 SuspiciousCpuUsageThreshold;

    //! Time fraction spent in idle state enough for job to be considered suspicious.
    double SuspiciousInputPipeIdleTimeFraction;

    //! Suspicious jobs per operation recalculation period.
    TDuration SuspiciousJobsUpdatePeriod;

    //! Maximum allowed running time of operation. Null value is interpreted as infinity.
    TNullable<TDuration> OperationTimeLimit;

    TDuration OperationTimeLimitCheckPeriod;

    TDuration ResourceDemandSanityCheckPeriod;

    //! Timeout on operation initialization.
    //! Prevents hanging of remote copy when remote cluster is unavailable.
    TDuration OperationInitializationTimeout;

    TDuration OperationTransactionTimeout;

    TDuration OperationLogProgressBackoff;

    TDuration OperationLogFairSharePeriod;

    TDuration OperationControllerFailTimeout;

    TDuration AvailableExecNodesCheckPeriod;

    TDuration OperationProgressAnalysisPeriod;

    TDuration OperationBuildProgressPeriod;

    TDuration TaskUpdatePeriod;

    //! Max available exec node resources are updated not more often then this period.
    TDuration MaxAvailableExecNodeResourcesUpdatePeriod;

    //! Maximum number of job nodes per operation.
    int MaxJobNodesPerOperation;

    //! Maximum number of chunks per single fetch.
    int MaxChunksPerFetch;

    //! Maximum size of file allowed to be passed to jobs.
    i64 MaxFileSize;

    //! Maximum number of input tables an operation can have.
    int MaxInputTableCount;

    //! Maximum number of ranges on the input table.
    int MaxRangesOnTable;

    //! Maximum number of files per user job.
    int MaxUserFileCount;

    //! Don't check resource demand for sanity if the number of online
    //! nodes is less than this bound.
    // TODO(ignat): rename to SafeExecNodeCount.
    int SafeOnlineNodeCount;

    //! Don't check resource demand for sanity if scheduler is online
    //! less than this timeout.
    TDuration SafeSchedulerOnlineTime;

    //! Time between two consecutive calls in operation controller to get exec nodes information from scheduler.
    TDuration ControllerUpdateExecNodesInformationDelay;

    //! Maximum number of foreign chunks to locate per request.
    int MaxChunksPerLocateRequest;

    //! Enables using tmpfs if tmpfs_path is specified in user spec.
    bool EnableTmpfs;

    //! Enables dynamic change of job sizes.
    bool EnablePartitionMapJobSizeAdjustment;

    bool EnableMapJobSizeAdjustment;

    //! Enables splitting of long jobs.
    bool EnableJobSplitting;

    //! Acl used for intermediate tables and stderrs additional to acls specified by user.
    NYTree::IListNodePtr AdditionalIntermediateDataAcl;

    double UserJobMemoryDigestPrecision;
    double UserJobMemoryReserveQuantile;
    double JobProxyMemoryReserveQuantile;
    double ResourceOverdraftFactor;

    //! If user job iops threshold is exceeded, iops throttling is enabled via cgroups.
    TNullable<int> IopsThreshold;
    TNullable<int> IopsThrottlerLimit;

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

    //! If |true|, jobs are revived from snapshot.
    bool EnableJobRevival;

    //! If |false|, all locality timeouts are considered 0.
    bool EnableLocality;

    //! Allow failing a controller by passing testing option `controller_failure`
    //! in operation spec. Used only for testing purposes.
    bool EnableControllerFailureSpecOption;

    NChunkClient::TFetcherConfigPtr Fetcher;

    TNullable<NYPath::TYPath> UdfRegistryPath;

    //! Discriminates between "heavy" and "light" job specs. For those with slice count
    //! not exceeding this threshold no throttling is done.
    int HeavyJobSpecSliceCountThreshold;

    //! We use the same config for input chunk scraper and intermediate chunk scraper.
    NControllerAgent::TIntermediateChunkScraperConfigPtr ChunkScraper;

    //! Total number of data slices in operation, summed up over all jobs.
    i64 MaxTotalSliceCount;

    TOperationAlertsConfigPtr OperationAlerts;

    //! Chunk size in per-controller row buffers.
    i64 ControllerRowBufferChunkSize;

    //! Some special options for testing purposes.
    TTestingOptionsPtr TestingOptions;

    NCompression::ECodec JobSpecCodec;

    //! Backoff to report job metrics from operation to scheduler.
    TDuration JobMetricsDeltaReportBackoff;

    // Cypress path to a special layer containing YT-specific data required to
    // run jobs with custom rootfs, e.g. statically linked job-satellite.
    // Is applied on top of user layers if they are used.
    TNullable<TString> SystemLayerPath;

    //! Backoff between schedule job statistics logging.
    TDuration ScheduleJobStatisticsLogBackoff;

    TControllerAgentConfig()
    {
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
        RegisterParameter("desired_chunk_lists_per_release", DesiredChunkListsPerRelease)
            .Default(10 * 1000);

        RegisterParameter("enable_snapshot_building", EnableSnapshotBuilding)
            .Default(true);
        RegisterParameter("snapshot_period", SnapshotPeriod)
            .Default(TDuration::Seconds(300));
        RegisterParameter("snapshot_timeout", SnapshotTimeout)
            .Default(TDuration::Seconds(60));
        RegisterParameter("operation_controller_suspend_timeout", OperationControllerSuspendTimeout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("parallel_snapshot_builder_count", ParallelSnapshotBuilderCount)
            .Default(4)
            .GreaterThan(0);
        RegisterParameter("snapshot_writer", SnapshotWriter)
            .DefaultNew();

        RegisterParameter("enable_snapshot_loading", EnableSnapshotLoading)
            .Default(false);
        RegisterParameter("snapshot_reader", SnapshotReader)
            .DefaultNew();

        RegisterParameter("transactions_refresh_period", TransactionsRefreshPeriod)
            .Default(TDuration::Seconds(3));
        RegisterParameter("operations_update_period", OperationsUpdatePeriod)
            .Default(TDuration::Seconds(3));
        RegisterParameter("chunk_unstage_period", ChunkUnstagePeriod)
            .Default(TDuration::MilliSeconds(100));

        RegisterParameter("max_children_per_attach_request", MaxChildrenPerAttachRequest)
            .Default(10000)
            .GreaterThan(0);

        RegisterParameter("chunk_location_throttler", ChunkLocationThrottler)
            .DefaultNew();

        RegisterParameter("event_log", EventLog)
            .DefaultNew();

        RegisterParameter("controller_agent_heartbeat_period", ControllerAgentHeartbeatPeriod)
            .Default(TDuration::MilliSeconds(10));

        RegisterParameter("controller_agent_heartbeat_rpc_timeout", ControllerAgentHeartbeatRpcTimeout)
            .Default(TDuration::Seconds(10));

        RegisterParameter("exec_nodes_request_period", ExecNodesRequestPeriod)
            .Default(TDuration::Seconds(10));

        RegisterParameter("controller_thread_count", ControllerThreadCount)
            .Default(4)
            .GreaterThan(0);

        RegisterParameter("max_concurrent_safe_core_dumps", MaxConcurrentSafeCoreDumps)
            .Default(1)
            .GreaterThanOrEqual(0);

        RegisterParameter("scheduling_tag_filter_expire_timeout", SchedulingTagFilterExpireTimeout)
            .Default(TDuration::Seconds(10));

        RegisterParameter("suspicious_inactivity_timeout", SuspiciousInactivityTimeout)
            .Default(TDuration::Minutes(1));
        RegisterParameter("suspicious_cpu_usage_threshold", SuspiciousCpuUsageThreshold)
            .Default(300);
        RegisterParameter("suspicious_input_pipe_time_idle_fraction", SuspiciousInputPipeIdleTimeFraction)
            .Default(0.95);
        RegisterParameter("suspicious_jobs_update_period", SuspiciousJobsUpdatePeriod)
            .Default(TDuration::Seconds(5));

        RegisterParameter("operation_time_limit", OperationTimeLimit)
            .Default();
        RegisterParameter("operation_time_limit_check_period", OperationTimeLimitCheckPeriod)
            .Default(TDuration::Seconds(1));

        RegisterParameter("resource_demand_sanity_check_period", ResourceDemandSanityCheckPeriod)
            .Default(TDuration::Seconds(15));

        RegisterParameter("operation_initialization_timeout", OperationInitializationTimeout)
            .Default(TDuration::Seconds(60));
        RegisterParameter("operation_transaction_timeout", OperationTransactionTimeout)
            .Default(TDuration::Minutes(60));

        RegisterParameter("operation_progress_log_backoff", OperationLogProgressBackoff)
            .Default(TDuration::Seconds(1));

        RegisterParameter("task_update_period", TaskUpdatePeriod)
            .Default(TDuration::Seconds(3));

        RegisterParameter("operation_controller_fail_timeout", OperationControllerFailTimeout)
            .Default(TDuration::Seconds(120));

        RegisterParameter("available_exec_nodes_check_period", AvailableExecNodesCheckPeriod)
            .Default(TDuration::Seconds(5));

        RegisterParameter("operation_progress_analysis_period", OperationProgressAnalysisPeriod)
            .Default(TDuration::Seconds(10));

        RegisterParameter("operation_build_progress_period", OperationBuildProgressPeriod)
            .Default(TDuration::Seconds(3));

        RegisterParameter("max_available_exec_node_resources_update_period", MaxAvailableExecNodeResourcesUpdatePeriod)
            .Default(TDuration::Seconds(10));

        RegisterParameter("max_job_nodes_per_operation", MaxJobNodesPerOperation)
            .Default(200)
            .GreaterThanOrEqual(0)
            .LessThanOrEqual(250);

        RegisterParameter("max_chunks_per_fetch", MaxChunksPerFetch)
            .Default(100000)
            .GreaterThan(0);

        RegisterParameter("max_file_size", MaxFileSize)
            .Default(10_GB);

        RegisterParameter("max_input_table_count", MaxInputTableCount)
            .Default(1000)
            .GreaterThan(0);

        RegisterParameter("max_ranges_on_table", MaxRangesOnTable)
            .Default(1000)
            .GreaterThan(0);

        RegisterParameter("max_user_file_count", MaxUserFileCount)
            .Default(1000)
            .GreaterThan(0);

        RegisterParameter("safe_online_node_count", SafeOnlineNodeCount)
            .GreaterThanOrEqual(0)
            .Default(1);

        RegisterParameter("safe_scheduler_online_time", SafeSchedulerOnlineTime)
            .Default(TDuration::Minutes(10));

        RegisterParameter("controller_update_exec_nodes_information_delay", ControllerUpdateExecNodesInformationDelay)
            .Default(TDuration::Seconds(30));

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

        RegisterParameter("enable_controller_failure_spec_option", EnableControllerFailureSpecOption)
            .Default(false);

        RegisterParameter("enable_job_revival", EnableJobRevival)
            .Default(true);

        RegisterParameter("enable_locality", EnableLocality)
            .Default(true);

        RegisterParameter("fetcher", Fetcher)
            .DefaultNew();

        RegisterParameter("udf_registry_path", UdfRegistryPath)
            .Default(Null);

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

        RegisterParameter("heavy_job_spec_slice_count_threshold", HeavyJobSpecSliceCountThreshold)
            .Default(1000)
            .GreaterThan(0);

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

        RegisterParameter("iops_threshold", IopsThreshold)
            .Default(Null);
        RegisterParameter("iops_throttler_limit", IopsThrottlerLimit)
            .Default(Null);

        RegisterParameter("chunk_scraper", ChunkScraper)
            .DefaultNew();

        RegisterParameter("max_total_slice_count", MaxTotalSliceCount)
            .Default((i64) 10 * 1000 * 1000)
            .GreaterThan(0);

        RegisterParameter("operation_alerts", OperationAlerts)
            .DefaultNew();

        RegisterParameter("controller_row_buffer_chunk_size", ControllerRowBufferChunkSize)
            .Default(64_KB)
            .GreaterThan(0);

        RegisterParameter("testing_options", TestingOptions)
            .DefaultNew();

        RegisterParameter("job_spec_codec", JobSpecCodec)
            .Default(NCompression::ECodec::Lz4);

        RegisterParameter("job_metrics_delta_report_backoff", JobMetricsDeltaReportBackoff)
            .Default(TDuration::Seconds(15));

        RegisterParameter("system_layer_path", SystemLayerPath)
            .Default(Null);

        RegisterParameter("schedule_job_statistics_log_backoff", ScheduleJobStatisticsLogBackoff)
            .Default(TDuration::Seconds(1));

        RegisterInitializer([&] () {
            EventLog->MaxRowWeight = 128_MB;
            if (!EventLog->Path) {
                EventLog->Path = "//sys/scheduler/event_log";
            }

            ChunkLocationThrottler->Limit = 10000;

            // Value in options is an upper bound hint on uncompressed data size for merge jobs.
            OrderedMergeOperationOptions->DataWeightPerJob = 20_GB;
            OrderedMergeOperationOptions->MaxDataSlicesPerJob = 10000;

            SortedMergeOperationOptions->DataWeightPerJob = 20_GB;
            SortedMergeOperationOptions->MaxDataSlicesPerJob = 10000;

            UnorderedMergeOperationOptions->DataWeightPerJob = 20_GB;
            UnorderedMergeOperationOptions->MaxDataSlicesPerJob = 10000;
        });
    }

private:
    template <class TOptions>
    void UpdateOptions(TOptions* options, NYT::NYTree::INodePtr patch);

    virtual void OnLoaded() override;
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

#define CONFIG_INL_H_
#include "config-inl.h"
#undef CONFIG_INL_H_
