#pragma once

#include "public.h"

#include <ytlib/ytree/yson_serializable.h>

#include <ytlib/table_client/config.h>

#include <ytlib/file_client/config.h>

#include <ytlib/rpc/retrying_channel.h>

#include <server/job_proxy/config.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TFairShareStrategyConfig
    : public TYsonSerializable
{
    // The following settings can be overridden in operation spec.
    TDuration MinSharePreemptionTimeout;
    TDuration FairSharePreemptionTimeout;
    double FairShareStarvationTolerance;
    double FairSharePreemptionTolerance;

    TDuration FairShareUpdatePeriod;

    double NewOperationWeightBoostFactor;
    TDuration NewOperationWeightBoostPeriod;

    //! Any operation with usage less than this cannot be preempted.
    double MinPreemptableRatio;

    TFairShareStrategyConfig()
    {
        Register("min_share_preemption_timeout", MinSharePreemptionTimeout)
            .Default(TDuration::Seconds(15));
        Register("fair_share_preemption_timeout", FairSharePreemptionTimeout)
            .Default(TDuration::Seconds(30));
        Register("fair_share_starvation_tolerance", FairShareStarvationTolerance)
            .InRange(0.0, 1.0)
            .Default(0.8);
        Register("fair_share_preemption_tolerance", FairSharePreemptionTolerance)
            .GreaterThanOrEqual(0.0)
            .Default(1.05);

        Register("fair_share_update_period", FairShareUpdatePeriod)
            .Default(TDuration::MilliSeconds(1000));

        Register("new_operation_weight_boost_factor", NewOperationWeightBoostFactor)
            .GreaterThanOrEqual(1.0)
            .Default(1.0);
        Register("new_operation_weight_boost_period", NewOperationWeightBoostPeriod)
            .Default(TDuration::Minutes(0));

        Register("min_preemptable_ratio", MinPreemptableRatio)
            .InRange(0.0, 1.0)
            .Default(0.05);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerConfig
    : public TFairShareStrategyConfig
{
    TDuration ConnectRetryPeriod;

    TDuration NodeHearbeatTimeout;

    TDuration TransactionsRefreshPeriod;

    TDuration OperationsUpdatePeriod;

    TDuration WatchersUpdatePeriod;

    TDuration ResourceDemandSanityCheckPeriod;

    TDuration LockTransactionTimeout;

    TDuration OperationTransactionTimeout;

    //! Timeout used for direct RPC requests to nodes.
    TDuration NodeRpcTimeout;

    ESchedulerStrategy Strategy;

    //! Once this limit is reached the operation fails.
    int MaxFailedJobCount;

    //! Limits the number of stderrs the operation is allowed to produce.
    int MaxStdErrCount;

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

    //! Maximum number of chunk trees to attach per request.
    int MaxChildrenPerAttachRequest;

    //! Max size of data slice for different jobs.
    i64 MapJobMaxSliceDataSize;
    i64 MergeJobMaxSliceDataSize;
    i64 SortJobMaxSliceDataSize;
    i64 PartitionJobMaxSliceDataSize;

    //! Maximum number of partitions during sort, ever.
    int MaxPartitionCount;

    //! Maximum number of jobs per operation (an approximation!).
    int MaxJobCount;

    //! Maximum size of table allowed to be passed as a file to jobs.
    i64 MaxTableFileSize;

    //! Maximum number of output tables an operation can have.
    int MaxOutputTableCount;

    //! Maximum number of jobs to start within a single heartbeat.
    TNullable<int> MaxStartedJobsPerHeartbeat;

    NYTree::INodePtr MapOperationSpec;
    NYTree::INodePtr ReduceOperationSpec;
    NYTree::INodePtr EraseOperationSpec;
    NYTree::INodePtr OrderedMergeOperationSpec;
    NYTree::INodePtr UnorderedMergeOperationSpec;
    NYTree::INodePtr SortedMergeOperationSpec;
    NYTree::INodePtr MapReduceOperationSpec;
    NYTree::INodePtr SortOperationSpec;

    // Default environment variables set for every job.
    yhash_map<Stroka, Stroka> Environment;

    TDuration SnapshotPeriod;
    TDuration SnapshotTimeout;
    Stroka SnapshotTempPath;
    NFileClient::TFileReaderConfigPtr SnapshotReader;
    NFileClient::TFileWriterConfigPtr SnapshotWriter;

    NRpc::TRetryingChannelConfigPtr NodeChannel;

    TSchedulerConfig()
    {
        Register("connect_retry_period", ConnectRetryPeriod)
            .Default(TDuration::Seconds(15));
        Register("node_heartbeat_timeout", NodeHearbeatTimeout)
            .Default(TDuration::Seconds(60));
        Register("transactions_refresh_period", TransactionsRefreshPeriod)
            .Default(TDuration::Seconds(3));
        Register("operations_update_period", OperationsUpdatePeriod)
            .Default(TDuration::Seconds(3));
        Register("watchers_update_period", WatchersUpdatePeriod)
            .Default(TDuration::Seconds(15));
        Register("resource_demand_sanity_check_period", ResourceDemandSanityCheckPeriod)
            .Default(TDuration::Seconds(15));
        Register("lock_transaction_timeout", LockTransactionTimeout)
            .Default(TDuration::Seconds(15));
        Register("operation_transaction_timeout", OperationTransactionTimeout)
            .Default(TDuration::Minutes(60));
        Register("node_rpc_timeout", NodeRpcTimeout)
            .Default(TDuration::Seconds(15));

        Register("strategy", Strategy)
            .Default(ESchedulerStrategy::Null);

        Register("max_failed_job_count", MaxFailedJobCount)
            .Default(100)
            .GreaterThanOrEqual(0);
        Register("max_stderr_count", MaxStdErrCount)
            .Default(100)
            .GreaterThanOrEqual(0);

        Register("chunk_list_preallocation_count", ChunkListPreallocationCount)
            .Default(128)
            .GreaterThanOrEqual(0);
        Register("max_chunk_list_allocation_count", MaxChunkListAllocationCount)
            .Default(16384)
            .GreaterThanOrEqual(0);
        Register("chunk_list_watermark_count", ChunkListWatermarkCount)
            .Default(50)
            .GreaterThanOrEqual(0);
        Register("chunk_list_allocation_multiplier", ChunkListAllocationMultiplier)
            .Default(2.0)
            .GreaterThan(1.0);

        Register("max_children_per_attach_request", MaxChildrenPerAttachRequest)
            .Default(10000)
            .GreaterThan(0);

        Register("map_job_max_slice_data_size", MapJobMaxSliceDataSize)
            .Default((i64)256 * 1024 * 1024)
            .GreaterThan(0);

        Register("merge_job_max_slice_data_size", MergeJobMaxSliceDataSize)
            .Default((i64)256 * 1024 * 1024)
            .GreaterThan(0);

        Register("partition_job_max_slice_data_size", PartitionJobMaxSliceDataSize)
            .Default((i64)256 * 1024 * 1024)
            .GreaterThan(0);

        Register("sort_job_max_slice_data_size", SortJobMaxSliceDataSize)
            .Default((i64)256 * 1024 * 1024)
            .GreaterThan(0);

        Register("max_partition_count", MaxPartitionCount)
            .Default(2000)
            .GreaterThan(0);

        Register("table_file_size_limit", MaxTableFileSize)
            .Default((i64) 2 * 1024 * 1024 * 1024);

        Register("max_output_table_count", MaxOutputTableCount)
            .Default(20)
            .GreaterThan(1)
            .LessThan(1000);

        Register("max_started_jobs_per_heartbeat", MaxStartedJobsPerHeartbeat)
            .Default()
            .GreaterThan(0);

        Register("map_operation_spec", MapOperationSpec)
            .Default(nullptr);
        Register("reduce_operation_spec", ReduceOperationSpec)
            .Default(nullptr);
        Register("erase_operation_spec", EraseOperationSpec)
            .Default(nullptr);
        Register("ordered_merge_operation_spec", OrderedMergeOperationSpec)
            .Default(nullptr);
        Register("unordered_merge_operation_spec", UnorderedMergeOperationSpec)
            .Default(nullptr);
        Register("sorted_merge_operation_spec", SortedMergeOperationSpec)
            .Default(nullptr);
        Register("map_reduce_operation_spec", MapReduceOperationSpec)
            .Default(nullptr);
        Register("sort_operation_spec", SortOperationSpec)
            .Default(nullptr);

        Register("max_job_count", MaxJobCount)
            .Default(20000)
            .GreaterThan(0);

        Register("environment", Environment)
            .Default(yhash_map<Stroka, Stroka>());

        Register("node_channel", NodeChannel)
            .DefaultNew();

        Register("snapshot_timeout", SnapshotTimeout)
            .Default(TDuration::Seconds(60));
        Register("snapshot_period", SnapshotPeriod)
            .Default(TDuration::Seconds(300));
        Register("snapshot_temp_path", SnapshotTempPath)
            .NonEmpty();
        Register("snapshot_reader", SnapshotReader)
            .DefaultNew();
        Register("snapshot_writer", SnapshotWriter)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
