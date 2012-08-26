#pragma once

#include "public.h"

#include <ytlib/ytree/yson_serializable.h>

#include <ytlib/table_client/config.h>

#include <server/job_proxy/config.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerConfig
    : public TYsonSerializable
{
    TDuration StartupRetryPeriod;

    TDuration TransactionsRefreshPeriod;

    TDuration NodesRefreshPeriod;

    TDuration OperationsUpdatePeriod;

    ESchedulerStrategy Strategy;

    //! Timeout used for direct RPC requests to nodes.
    TDuration NodeRpcTimeout;

    //! Once this limit is reached the operation fails.
    int FailedJobsLimit;

    //! Number of chunk lists to be allocated when an operation starts.
    int ChunkListPreallocationCount;

    //! Better keep the number of spare chunk lists above this threshold.
    int ChunkListWatermarkCount;

    //! Each time the number of spare chunk lists drops below #ChunkListWatermarkCount or
    //! the controller requests more chunk lists than we currently have,
    //! another batch is allocated. Each time we allocate #ChunkListAllocationMultiplier times
    //! more chunk lists than previously.
    double ChunkListAllocationMultiplier;

    //! Maximum number of partitions during sort, ever.
    int MaxPartitionCount;

    NJobProxy::TJobIOConfigPtr MapJobIO;
    NJobProxy::TJobIOConfigPtr SortedMergeJobIO;
    NJobProxy::TJobIOConfigPtr OrderedMergeJobIO;
    NJobProxy::TJobIOConfigPtr UnorderedMergeJobIO;
    NJobProxy::TJobIOConfigPtr SortedReduceJobIO;
    NJobProxy::TJobIOConfigPtr PartitionReduceJobIO;
    NJobProxy::TJobIOConfigPtr PartitionJobIO;
    NJobProxy::TJobIOConfigPtr SimpleSortJobIO;
    NJobProxy::TJobIOConfigPtr PartitionSortJobIO;

    TSchedulerConfig()
    {
        Register("startup_retry_period", StartupRetryPeriod)
            .Default(TDuration::Seconds(15));
        Register("transactions_refresh_period", TransactionsRefreshPeriod)
            .Default(TDuration::Seconds(3));
        Register("nodes_refresh_period", NodesRefreshPeriod)
            .Default(TDuration::Seconds(15));
        Register("operations_update_period", OperationsUpdatePeriod)
            .Default(TDuration::Seconds(3));
        Register("strategy", Strategy)
            .Default(ESchedulerStrategy::Null);
        Register("node_rpc_timeout", NodeRpcTimeout)
            .Default(TDuration::Seconds(15));
        Register("failed_jobs_limit", FailedJobsLimit)
            .Default(100)
            .GreaterThanOrEqual(0);
        Register("chunk_list_preallocation_count", ChunkListPreallocationCount)
            .Default(100)
            .GreaterThanOrEqual(0);
        Register("chunk_list_watermark_count", ChunkListWatermarkCount)
            .Default(50)
            .GreaterThanOrEqual(0);
        Register("chunk_list_allocation_multiplier", ChunkListAllocationMultiplier)
            .Default(2.0)
            .GreaterThan(1.0);
        Register("max_partition_count", MaxPartitionCount)
            .Default(2000)
            .GreaterThan(0);
        Register("map_job_io", MapJobIO)
            .DefaultNew();
        Register("sorted_merge_job_io", SortedMergeJobIO)
            .DefaultNew();
        Register("ordered_merge_job_io", OrderedMergeJobIO)
            .DefaultNew();
        Register("unordered_merge_job_io", UnorderedMergeJobIO)
            .DefaultNew();
        Register("sorted_reduce_job_io", SortedReduceJobIO)
            .DefaultNew();
        Register("partition_reduce_job_io", PartitionReduceJobIO)
            .DefaultNew();
        Register("partition_job_io", PartitionJobIO)
            .DefaultNew();
        Register("simple_sort_job_io", SimpleSortJobIO)
            .DefaultNew();
        Register("partition_sort_job_io", PartitionSortJobIO)
            .DefaultNew();
        UnorderedMergeJobIO->TableReader->PrefetchWindow = 10;
        PartitionSortJobIO->TableReader->PrefetchWindow = 10;
        PartitionReduceJobIO->TableReader->PrefetchWindow = 10;

        PartitionJobIO->TableWriter->MaxBufferSize = 2l * 1024 * 1024 * 1024; // 2 GB
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
