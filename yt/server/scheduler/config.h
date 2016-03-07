#pragma once

#include "private.h"

#include <yt/server/job_proxy/config.h>

#include <yt/ytlib/api/config.h>

#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/table_client/config.h>

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

    TNullable<TDuration> FairShareUpdatePeriod;
    TNullable<TDuration> FairShareLogPeriod;

    //! Any operation with usage less than this cannot be preempted.
    double MinPreemptableRatio;

    //! Limit on number of running operations.
    // TODO(ignat): Rename Operations to OperationCount.
    int MaxRunningOperations;
    int MaxRunningOperationsPerPool;

    //! Limit on number of operations started in pool.
    int MaxOperationsPerPool;

    //! Maximum number of operations that can be run concurrently.
    int MaxOperationCount;

    //! If enabled, pools will be able to starve and provoke preemption.
    bool EnablePoolStarvation;

    //! Default parent pool for operations with unknown pool.
    Stroka DefaultParentPool;

    // Preemption timeout for operations with small number of jobs will be
    // discounted proportionally to this coefficient.
    double JobCountPreemptionTimeoutCoefficient;

    //! Limit on number of concurrent calls to ScheduleJob of single controller.
    int MaxConcurrentControllerScheduleJobCalls;

    TFairShareStrategyConfig()
    {
        RegisterParameter("min_share_preemption_timeout", MinSharePreemptionTimeout)
            .Default(TDuration::Seconds(15));
        RegisterParameter("fair_share_preemption_timeout", FairSharePreemptionTimeout)
            .Default(TDuration::Seconds(30));
        RegisterParameter("fair_share_starvation_tolerance", FairShareStarvationTolerance)
            .InRange(0.0, 1.0)
            .Default(0.8);

        RegisterParameter("fair_share_update_period", FairShareUpdatePeriod)
            .Default(TDuration::MilliSeconds(1000));

        RegisterParameter("fair_share_log_period", FairShareLogPeriod)
            .Default(TDuration::MilliSeconds(1000));

        RegisterParameter("min_preemptable_ratio", MinPreemptableRatio)
            .InRange(0.0, 1.0)
            .Default(0.05);

        RegisterParameter("max_running_operations", MaxRunningOperations)
            .Default(200)
            .GreaterThan(0);

        RegisterParameter("max_running_operations_per_pool", MaxRunningOperationsPerPool)
            .Default(50)
            .GreaterThan(0);

        RegisterParameter("max_operations_per_pool", MaxOperationsPerPool)
            .Default(50)
            .GreaterThan(0);

        RegisterParameter("max_operation_count", MaxOperationCount)
            .Default(1000)
            .GreaterThan(0);

        RegisterParameter("enable_pool_starvation", EnablePoolStarvation)
            .Default(true);

        RegisterParameter("default_parent_pool", DefaultParentPool)
            .Default(RootPoolName);

        RegisterParameter("job_count_preemption_timeout_coefficient", JobCountPreemptionTimeoutCoefficient)
            .Default(1.0)
            .GreaterThanOrEqual(1.0);

        RegisterParameter("max_concurrent_controller_schedule_job_calls", MaxConcurrentControllerScheduleJobCalls)
            .Default(10)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategyConfig)

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

class TOperationOptions
    : public NYTree::TYsonSerializable
{
public:
    NYTree::INodePtr SpecTemplate;

    TOperationOptions()
    {
        RegisterParameter("spec_template", SpecTemplate)
            .Default();
    }
};

class TSimpleOperationOptions
    : public TOperationOptions
{
public:
    int MaxJobCount;
    i64 JobMaxSliceDataSize;

    TSimpleOperationOptions()
    {
        RegisterParameter("max_job_count", MaxJobCount)
            .Default(100000);

        RegisterParameter("job_max_slice_data_size", JobMaxSliceDataSize)
            .Default((i64)256 * 1024 * 1024)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TSimpleOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TMapOperationOptions
    : public TSimpleOperationOptions
{ };

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
{ };

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
    i64 SortJobMaxSliceDataSize;
    i64 PartitionJobMaxSliceDataSize;
    i32 MaxSampleSize;
    i64 CompressedBlockSize;

    TSortOperationOptionsBase()
    {
        RegisterParameter("max_partition_job_count", MaxPartitionJobCount)
            .Default(100000)
            .GreaterThan(0);

        RegisterParameter("partition_job_max_slice_data_size", PartitionJobMaxSliceDataSize)
            .Default((i64)256 * 1024 * 1024)
            .GreaterThan(0);

        RegisterParameter("sort_job_max_slice_data_size", SortJobMaxSliceDataSize)
            .Default((i64)256 * 1024 * 1024)
            .GreaterThan(0);

        RegisterParameter("max_sample_size", MaxSampleSize)
            .Default(10 * 1024)
            .GreaterThan(1024);

        RegisterParameter("compressed_block_size", CompressedBlockSize)
            .Default(1 * 1024 * 1024)
            .GreaterThan(1024);
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
    : public TOperationOptions
{
public:
    int MaxJobCount;

    TRemoteCopyOperationOptions()
    {
        RegisterParameter("max_job_count", MaxJobCount)
            .Default(100000)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TRemoteCopyOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerConfig
    : public TFairShareStrategyConfig
    , public NChunkClient::TChunkScraperConfig
    , public NChunkClient::TChunkTeleporterConfig
{
public:
    //! Number of threads for running controllers invokers.
    int ControllerThreadCount;

    //! Number of threads for building job specs.
    int JobSpecBuilderThreadCount;

    TDuration ConnectRetryBackoffTime;

    //! Timeout for node expiration.
    TDuration NodeHeartbeatTimeout;

    TDuration TransactionsRefreshPeriod;

    TDuration OperationsUpdatePeriod;

    TDuration WatchersUpdatePeriod;

    TDuration ClusterDirectoryUpdatePeriod;

    TDuration ResourceDemandSanityCheckPeriod;

    TDuration LockTransactionTimeout;

    TDuration OperationTransactionTimeout;

    TDuration JobProberRpcTimeout;

    TDuration ClusterInfoLoggingPeriod;

    TDuration OperationTimeLimitCheckPeriod;

    //! Jobs running on node are logged periodically or when they change their state.
    TDuration JobsLoggingPeriod;

    //! Maximum allowed running time of operation. Null value is interpreted as infinity.
    TNullable<TDuration> OperationTimeLimit;

    //! Maximum allowed time for single job scheduling.
    TDuration ControllerScheduleJobTimeLimit;

    //! Once this limit is reached the operation fails.
    int MaxFailedJobCount;

    //! Once this limit is reached the memory reserve is disabled.
    int MaxMemoryReserveAbortJobCount;

    //! Limits the number of stderrs the operation is allowed to produce.
    int MaxStderrCount;

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

    //! Maximum number of chunk stripes per job.
    int MaxChunkStripesPerJob;

    //! Maximum number of chunk trees to attach per request.
    int MaxChildrenPerAttachRequest;

    //! Controls finer initial slicing of input data to ensure even distribution of data split sizes among jobs.
    double SliceDataSizeMultiplier;

    //! Maximum size of file allowed to be passed to jobs.
    i64 MaxFileSize;

    //! Maximum number of output tables times job count an operation can have.
    int MaxOutputTablesTimesJobsCount;

    //! Maximum number of input tables an operation can have.
    int MaxInputTableCount;

    //! Maximum number of files per user job.
    int MaxUserFileCount;

    //! blkio.weight set on user job cgroup.
    TNullable<int> UserJobBlkioWeight;

    //! Maximum number of jobs to start within a single heartbeat.
    TNullable<int> MaxStartedJobsPerHeartbeat;

    //! Don't check resource demand for sanity if the number of online
    //! nodes is less than this bound.
    int SafeOnlineNodeCount;

    //! Time between two consecutive calls in operation controller to get exec nodes information from scheduler.
    TDuration GetExecNodesInformationDelay;

    //! Maximum number of foreign chunks to locate per request.
    int MaxChunksPerLocateRequest;

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
    yhash_map<Stroka, Stroka> Environment;

    //! Interval between consequent snapshots.
    TDuration SnapshotPeriod;

    //! Timeout for snapshot construction.
    TDuration SnapshotTimeout;

    //! If |true|, snapshots are periodically constructed and uploaded into the system.
    bool EnableSnapshotBuilding;

    //! If |true|, snapshots are loaded during revival.
    bool EnableSnapshotLoading;

    Stroka SnapshotTempPath;
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

    TSchedulerConfig()
    {
        RegisterParameter("controller_thread_count", ControllerThreadCount)
            .Default(4)
            .GreaterThan(0);
        RegisterParameter("job_spec_builder_thread_count", JobSpecBuilderThreadCount)
            .Default(8)
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
        RegisterParameter("cluster_directory_update_period", ClusterDirectoryUpdatePeriod)
            .Default(TDuration::Seconds(3));
        RegisterParameter("resource_demand_sanity_check_period", ResourceDemandSanityCheckPeriod)
            .Default(TDuration::Seconds(15));
        RegisterParameter("lock_transaction_timeout", LockTransactionTimeout)
            .Default(TDuration::Seconds(15));
        RegisterParameter("operation_transaction_timeout", OperationTransactionTimeout)
            .Default(TDuration::Minutes(60));
        RegisterParameter("job_prober_rpc_timeout", JobProberRpcTimeout)
            .Default(TDuration::Seconds(300));

        RegisterParameter("cluster_info_logging_period", ClusterInfoLoggingPeriod)
            .Default(TDuration::Seconds(1));

        RegisterParameter("operation_time_limit_check_period", OperationTimeLimitCheckPeriod)
            .Default(TDuration::Seconds(1));

        RegisterParameter("jobs_logging_period", JobsLoggingPeriod)
            .Default(TDuration::Seconds(30));

        RegisterParameter("operation_time_limit", OperationTimeLimit)
            .Default();

        RegisterParameter("schedule_job_time_limit", ControllerScheduleJobTimeLimit)
            .Default(TDuration::Seconds(5));

        RegisterParameter("max_failed_job_count", MaxFailedJobCount)
            .Default(100)
            .GreaterThanOrEqual(0);
        RegisterParameter("max_memory_reserve_abort_job_count", MaxMemoryReserveAbortJobCount)
            .Default(100)
            .GreaterThanOrEqual(0);
        RegisterParameter("max_stderr_count", MaxStderrCount)
            .Default(100)
            .GreaterThanOrEqual(0);

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

        RegisterParameter("max_chunk_stripes_per_job", MaxChunkStripesPerJob)
            .Default(50000)
            .GreaterThan(0);

        RegisterParameter("max_children_per_attach_request", MaxChildrenPerAttachRequest)
            .Default(10000)
            .GreaterThan(0);

        RegisterParameter("slice_data_size_multiplier", SliceDataSizeMultiplier)
            .Default(0.51)
            .GreaterThan(0.0);

        RegisterParameter("max_file_size", MaxFileSize)
            .Default((i64) 10 * 1024 * 1024 * 1024);

        RegisterParameter("max_input_table_count", MaxInputTableCount)
            .Default(1000)
            .GreaterThan(0);

        RegisterParameter("max_user_file_count", MaxUserFileCount)
            .Default(1000)
            .GreaterThan(0);

        RegisterParameter("user_job_blkio_weight", UserJobBlkioWeight)
            .Default(Null);

        RegisterParameter("max_output_tables_times_jobs_count", MaxOutputTablesTimesJobsCount)
            .Default(20 * 100000)
            .GreaterThanOrEqual(100000);

        RegisterParameter("max_started_jobs_per_heartbeat", MaxStartedJobsPerHeartbeat)
            .Default()
            .GreaterThan(0);

        RegisterParameter("safe_online_node_count", SafeOnlineNodeCount)
            .GreaterThanOrEqual(0)
            .Default(1);

        RegisterParameter("get_exec_nodes_information_delay", GetExecNodesInformationDelay)
            .Default(TDuration::Seconds(1));

        RegisterParameter("max_chunks_per_locate_request", MaxChunksPerLocateRequest)
            .GreaterThan(0)
            .Default(10000);

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
            .Default(yhash_map<Stroka, Stroka>());

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

        RegisterInitializer([&] () {
            ChunkLocationThrottler->Limit = 10000;
        });

        RegisterValidator([&] () {
            if (SoftConcurrentHeartbeatLimit > HardConcurrentHeartbeatLimit) {
                THROW_ERROR_EXCEPTION("Soft limit on concurrent heartbeats must be less than or equal to hard limit on concurrent heartbeats")
                    << TErrorAttribute("soft_limit", SoftConcurrentHeartbeatLimit)
                    << TErrorAttribute("hard_limit", HardConcurrentHeartbeatLimit);
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
