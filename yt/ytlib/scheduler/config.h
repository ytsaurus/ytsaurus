#pragma once

#include "public.h"

#include <ytlib/ytree/yson_serializable.h>
#include <ytlib/job_proxy/public.h>

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

    //! The additional number of chunk lists to preallocate during preparation phase.
    //! These chunk lists are used in case of job failures.
    int SpareChunkListCount;

    //! Each time we run out of free chunk lists and unable to provide another |count| chunk lists,
    //! job scheduling gets suspended until |count * ChunkListAllocationMultiplier| chunk lists are allocated.
    int ChunkListAllocationMultiplier;

    //! Maximum number of partitions during sort, ever.
    int MaxPartitionCount;

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
        // TODO(babenko): consider decreasing
        Register("max_partition_count", MaxPartitionCount)
            .Default(2000)
            .GreaterThan(0);
        Register("map_job_io", MapJobIO).DefaultNew();
        Register("merge_job_io", MergeJobIO).DefaultNew();
        Register("partition_job_io", PartitionJobIO).DefaultNew();
        Register("sort_job_io", SortJobIO).DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TOperationSpecBase
    : public TYsonSerializable
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

struct TUserJobSpec
    : public TYsonSerializable
{
    Stroka Command;
    yvector<NYTree::TYPath> FilePaths;
    NYTree::INodePtr Format;
    NYTree::INodePtr InputFormat;
    NYTree::INodePtr OutputFormat;

    TUserJobSpec()
    {
        Register("command", Command);
        Register("file_paths", FilePaths)
            .Default();
        Register("format", Format)
            .Default(NULL);
        Register("input_format", InputFormat)
            .Default(NULL);
        Register("output_format", OutputFormat)
            .Default(NULL);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TMapOperationSpec
    : public TOperationSpecBase
{
    TUserJobSpecPtr Mapper;   
    yvector<NYTree::TYPath> InputTablePaths;
    yvector<NYTree::TYPath> OutputTablePaths;

    TMapOperationSpec()
    {
        Register("mapper", Mapper);
        Register("input_table_paths", InputTablePaths);
        Register("output_table_paths", OutputTablePaths);
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
    TNullable< yvector<Stroka> > KeyColumns;

    //! During sorted merge the scheduler tries to ensure that large connected
    //! groups of chunks are partitioned into tasks of this or smaller size.
    //! This number, however, is merely an estimate, i.e. some tasks may still
    //! be larger.
    i64 MaxMergeJobWeight;

    TMergeOperationSpec()
    {
        Register("input_table_paths", InputTablePaths);
        Register("output_table_path", OutputTablePath);
        Register("mode", Mode)
            .Default(EMergeMode::Unordered);
        Register("combine_chunks", CombineChunks)
            .Default(false);
        Register("key_columns", KeyColumns)
            .Default();
        Register("max_merge_job_weight", MaxMergeJobWeight)
            .Default((i64) 1024 * 1024 * 1024)
            .GreaterThan(0);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TEraseOperationSpec
    : public TOperationSpecBase
{
    NYTree::TYPath TablePath;
    bool CombineChunks;
    i64 MaxMergeJobWeight;

    TEraseOperationSpec()
    {
        Register("table_path", TablePath);
        Register("combine_chunks", CombineChunks)
            .Default(false);
        Register("max_merge_job_weight", MaxMergeJobWeight)
            .Default((i64) 1024 * 1024 * 1024)
            .GreaterThan(0);
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
    
    //! Only used if no partitioning is done.
    TNullable<int> SortJobCount;

    //! Maximum amount of (uncompressed) data to be given to a single sort job.
    //! By default, the controller computes the number of partitions by dividing
    //! the total input size by this number. The user, however, may specify a custom
    //! number of partitions.
    i64 MaxSortJobWeight;

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
        // TODO(babenko): update when the sort gets optimized
        Register("max_sort_job_weight", MaxSortJobWeight)
            .Default((i64) 1024 * 1024 * 1024)
            .GreaterThan(0);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TReduceOperationSpec
    : public TOperationSpecBase
{
    TUserJobSpecPtr Reducer;
    yvector<NYTree::TYPath> InputTablePaths;
    yvector<NYTree::TYPath> OutputTablePaths;
    TNullable< yvector<Stroka> > KeyColumns;
    i64 MaxReduceJobWeight;

    TReduceOperationSpec()
    {
        Register("reducer", Reducer);
        Register("input_table_paths", InputTablePaths);
        Register("output_table_paths", OutputTablePaths);
        Register("key_columns", KeyColumns)
            .Default();
        Register("max_reduce_job_weight", MaxReduceJobWeight)
            .Default((i64) 1024 * 1024 * 1024)
            .GreaterThan(0);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
