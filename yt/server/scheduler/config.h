#pragma once

#include "public.h"

#include <core/concurrency/throughput_throttler.h>

#include <core/rpc/config.h>

#include <ytlib/new_table_client/config.h>

#include <ytlib/chunk_client/config.h>

#include <ytlib/api/config.h>

#include <ytlib/ypath/public.h>

#include <core/ytree/yson_serializable.h>

#include <server/job_proxy/config.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TFairShareStrategyConfig
    : public NYTree::TYsonSerializable
{
public:
    // The following settings can be overridden in operation spec.
    TDuration MinSharePreemptionTimeout;
    TDuration FairSharePreemptionTimeout;
    double FairShareStarvationTolerance;

    TDuration FairShareUpdatePeriod;
    TDuration FairShareLogPeriod;

    //! Any operation with usage less than this cannot be preempted.
    double MinPreemptableRatio;

    TNullable<Stroka> SchedulingTag;

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
        
        RegisterParameter("scheduling_tag", SchedulingTag)
            .Default(Null);
    }
};

class TEventLogConfig
    : public NVersionedTableClient::TBufferedTableWriterConfig
{
public:
    NYPath::TYPath Path;

    TEventLogConfig()
    {
        RegisterParameter("path", Path)
            .Default("//sys/scheduler/event_log");
    }
};

class TSchedulerConfig
    : public TFairShareStrategyConfig
{
public:
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

    TDuration ChunkScratchPeriod;

    //! Number of chunks scratched per one LocateChunks.
    int MaxChunksPerScratch;

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

    //! Maximum number of chunks per single fetch.
    int MaxChunkCountPerFetch;

    //! Maximum number of chunk trees to attach per request.
    int MaxChildrenPerAttachRequest;

    //! Max size of data slice for different jobs.
    i64 MapJobMaxSliceDataSize;
    i64 MergeJobMaxSliceDataSize;
    i64 SortJobMaxSliceDataSize;
    i64 PartitionJobMaxSliceDataSize;

    //! Controls finer initial slicing of input data to ensure even distribution of data split sizes among jobs.
    double SliceDataSizeMultiplier;

    //! Maximum number of partitions during sort, ever.
    int MaxPartitionCount;

    //! Maximum number of jobs per operation (an approximation!).
    int MaxJobCount;

    //! Maximum number of partition jobs during map-reduce and sort operations.
    //! Refines #MaxJobCount.
    int MaxPartitionJobCount;

    //! Maximum number of operations that can be run concurrently.
    int MaxOperationCount;

    //! Maximum size of file allowed to be passed to jobs.
    i64 MaxFileSize;

    //! Maximum number of output tables an operation can have.
    int MaxOutputTableCount;

    //! Maximum number of input tables an operation can have.
    int MaxInputTableCount;

    //! Maximum number of files per user job.
    int MaxUserFileCount;

    //! Maximum number of jobs to start within a single heartbeat.
    TNullable<int> MaxStartedJobsPerHeartbeat;

    //! Whether to enable user job accounting.
    bool EnableAccounting;

    //! Don't check resource demand for sanity if the number of online
    //! nodes is less than this bound.
    int SafeOnlineNodeCount;

    NYTree::INodePtr MapOperationSpec;
    NYTree::INodePtr ReduceOperationSpec;
    NYTree::INodePtr EraseOperationSpec;
    NYTree::INodePtr OrderedMergeOperationSpec;
    NYTree::INodePtr UnorderedMergeOperationSpec;
    NYTree::INodePtr SortedMergeOperationSpec;
    NYTree::INodePtr MapReduceOperationSpec;
    NYTree::INodePtr SortOperationSpec;
    NYTree::INodePtr RemoteCopyOperationSpec;

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

    //! Limits the rate (measured in chunks) of location requests issued by all active chunk scratchers
    NConcurrency::TThroughputThrottlerConfigPtr ChunkLocationThrottler;

    TSchedulerConfig()
    {
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

        RegisterParameter("chunk_scratch_period", ChunkScratchPeriod)
            .Default(TDuration::Seconds(10));

        RegisterParameter("max_chunks_per_scratch", MaxChunksPerScratch)
            .Default(1000)
            .GreaterThan(0)
            .LessThan(100000);

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

        RegisterParameter("max_chunk_count_per_fetch", MaxChunkCountPerFetch)
            .Default(100000)
            .GreaterThan(0);

        RegisterParameter("max_children_per_attach_request", MaxChildrenPerAttachRequest)
            .Default(10000)
            .GreaterThan(0);

        RegisterParameter("slice_data_size_multiplier", SliceDataSizeMultiplier)
            .Default(0.51)
            .GreaterThan(0.0);

        RegisterParameter("map_job_max_slice_data_size", MapJobMaxSliceDataSize)
            .Default((i64)256 * 1024 * 1024)
            .GreaterThan(0);

        RegisterParameter("merge_job_max_slice_data_size", MergeJobMaxSliceDataSize)
            .Default((i64)256 * 1024 * 1024)
            .GreaterThan(0);

        RegisterParameter("partition_job_max_slice_data_size", PartitionJobMaxSliceDataSize)
            .Default((i64)256 * 1024 * 1024)
            .GreaterThan(0);

        RegisterParameter("sort_job_max_slice_data_size", SortJobMaxSliceDataSize)
            .Default((i64)256 * 1024 * 1024)
            .GreaterThan(0);

        RegisterParameter("max_partition_count", MaxPartitionCount)
            .Default(10000)
            .GreaterThan(0);

        RegisterParameter("max_file_size", MaxFileSize)
            .Default((i64) 10 * 1024 * 1024 * 1024);

        RegisterParameter("max_input_table_count", MaxInputTableCount)
            .Default(1000)
            .GreaterThan(0);

        RegisterParameter("max_user_file_count", MaxUserFileCount)
            .Default(1000)
            .GreaterThan(0);

        RegisterParameter("max_output_table_count", MaxOutputTableCount)
            .Default(20)
            .GreaterThan(1)
            .LessThan(1000);

        RegisterParameter("max_started_jobs_per_heartbeat", MaxStartedJobsPerHeartbeat)
            .Default()
            .GreaterThan(0);

        RegisterParameter("enable_accounting", EnableAccounting)
            .Default(false);

        RegisterParameter("safe_online_node_count", SafeOnlineNodeCount)
            .Default(1);

        RegisterParameter("map_operation_spec", MapOperationSpec)
            .Default(nullptr);
        RegisterParameter("reduce_operation_spec", ReduceOperationSpec)
            .Default(nullptr);
        RegisterParameter("erase_operation_spec", EraseOperationSpec)
            .Default(nullptr);
        RegisterParameter("ordered_merge_operation_spec", OrderedMergeOperationSpec)
            .Default(nullptr);
        RegisterParameter("unordered_merge_operation_spec", UnorderedMergeOperationSpec)
            .Default(nullptr);
        RegisterParameter("sorted_merge_operation_spec", SortedMergeOperationSpec)
            .Default(nullptr);
        RegisterParameter("map_reduce_operation_spec", MapReduceOperationSpec)
            .Default(nullptr);
        RegisterParameter("sort_operation_spec", SortOperationSpec)
            .Default(nullptr);
        RegisterParameter("remote_copy_operation_spec", RemoteCopyOperationSpec)
            .Default(nullptr);

        RegisterParameter("max_job_count", MaxJobCount)
            .Default(20000)
            .GreaterThan(0);
        RegisterParameter("max_partition_job_count", MaxPartitionJobCount)
            .Default(20000)
            .GreaterThan(0);
        RegisterParameter("max_operation_count", MaxOperationCount)
            .Default(100)
            .GreaterThan(0);

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

        RegisterInitializer([&] () {
            ChunkLocationThrottler->Limit = 10000;
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
