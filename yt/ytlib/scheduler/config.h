#pragma once

#include "public.h"

#include <ytlib/misc/configurable.h>
#include <ytlib/job_proxy/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerConfig
    : public TConfigurable
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

    //! The additional number of chunk lists to preallocate during preparation phase.
    //! These chunk lists are used in case of job failures.
    int SpareChunkListCount;

    //! Each time we run out of free chunk lists and unable to provide another |count| chunk lists,
    //! job scheduling gets suspended until |count * ChunkListAllocationMultiplier| chunk lists are allocated.
    int ChunkListAllocationMultiplier;

    //! Controls the minimum data size of a partition during sort.
    //! This is only a hint, the controller may still produce smaller partitions, e.g.
    //! when the user sets the number of partitions explicitly.
    i64 MinSortPartitionSize;

    //! Maximum amount of (uncompressed) data to be given to a single sort job.
    i64 MaxSortJobDataSize;

    NJobProxy::TJobIOConfigPtr MapJobIO;
    NJobProxy::TJobIOConfigPtr MergeJobIO;
    NJobProxy::TJobIOConfigPtr PartitionJobIO;
    NJobProxy::TJobIOConfigPtr SortJobIO;

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
        Register("spare_chunk_list_count", SpareChunkListCount)
            .Default(20)
            .GreaterThanOrEqual(0);
        Register("chunk_list_allocation_multiplier", ChunkListAllocationMultiplier)
            .Default(20)
            .GreaterThan(0);
        Register("min_sort_partition_size", MinSortPartitionSize)
            .Default((i64) 4 * 1024 * 1024 * 1024)
            .GreaterThan(0);
        Register("max_sort_job_data_size", MaxSortJobDataSize)
            .Default((i64) 4 * 1024 * 1024 * 1024)
            .GreaterThan(0);
        Register("map_job_io", MapJobIO).DefaultNew();
        Register("merge_job_io", MergeJobIO).DefaultNew();
        Register("partition_job_io", PartitionJobIO).DefaultNew();
        Register("sort_job_io", SortJobIO).DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TOperationSpecBase
    : public TConfigurable
{
    TNullable<int> JobCount;

    TOperationSpecBase()
    {
        SetKeepOptions(true);
        Register("job_count", JobCount)
            .Default()
            .GreaterThan(0);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TMapOperationSpec
    : public TOperationSpecBase
{
    Stroka Mapper;
    
    yvector<NYTree::TYPath> FilePaths;
    yvector<NYTree::TYPath> InputTablePaths;
    yvector<NYTree::TYPath> OutputTablePaths;

    NYTree::INodePtr Format;
    NYTree::INodePtr InputFormat;
    NYTree::INodePtr OutputFormat;

    TMapOperationSpec()
    {
        Register("mapper", Mapper);
        Register("file_paths", FilePaths)
            .Default(yvector<NYTree::TYPath>());
        Register("input_table_paths", InputTablePaths);
        Register("output_table_paths", OutputTablePaths);
        Register("format", Format)
            .Default(NULL);
        Register("input_format", InputFormat)
            .Default(NULL);
        Register("output_format", OutputFormat)
            .Default(NULL);
    }
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EMergeMode,
    (Sorted)
    (Ordered)
    (Unordered)
);

struct TMergeOperationSpec
    : public TOperationSpecBase
{
    yvector<NYTree::TYPath> InputTablePaths;
    NYTree::TYPath OutputTablePath;
    EMergeMode Mode;
    bool CombineChunks;

    TMergeOperationSpec()
    {
        Register("input_table_paths", InputTablePaths);
        Register("output_table_path", OutputTablePath);
        Register("mode", Mode)
            .Default(EMergeMode::Unordered);
        Register("combine_chunks", CombineChunks)
            .Default(false);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TEraseOperationSpec
    : public TOperationSpecBase
{
    NYTree::TYPath InputTablePath;
    NYTree::TYPath OutputTablePath;
    bool CombineChunks;

    TEraseOperationSpec()
    {
        Register("input_table_path", InputTablePath);
        Register("output_table_path", OutputTablePath);
        Register("combine_chunks", CombineChunks)
            .Default(false);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSortOperationSpec
    : public TOperationSpecBase
{
    yvector<NYTree::TYPath> InputTablePaths;
    NYTree::TYPath OutputTablePath;
    yvector<Stroka> KeyColumns;
    TNullable<int> PartitionCount;
    TNullable<int> PartitionJobCount;
    //! Only used if not partitioning is done.
    TNullable<int> SortJobCount;

    TSortOperationSpec()
    {
        Register("input_table_paths", InputTablePaths);
        Register("output_table_path", OutputTablePath);
        Register("key_columns", KeyColumns)
            .NonEmpty();
        Register("partition_count", PartitionCount)
            .Default()
            .GreaterThan(0);
        Register("partition_job_count", PartitionJobCount)
            .Default()
            .GreaterThan(0);
        Register("sort_job_count", SortJobCount)
            .Default()
            .GreaterThan(0);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
