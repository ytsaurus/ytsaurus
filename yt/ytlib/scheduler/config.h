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
        Register("max_partition_count", MaxPartitionCount)
            .Default(2000)
            .GreaterThan(0);
        Register("map_job_io", MapJobIO)
            .DefaultNew();
        Register("merge_job_io", MergeJobIO)
            .DefaultNew();
        Register("partition_job_io", PartitionJobIO)
            .DefaultNew();
        Register("sort_job_io", SortJobIO)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TOperationSpecBase
    : public TYsonSerializable
{
    TOperationSpecBase()
    {
        SetKeepOptions(true);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TUserJobSpec
    : public TYsonSerializable
{
    Stroka Command;
    
    std::vector<NYTree::TYPath> FilePaths;

    NYTree::INodePtr Format;
    NYTree::INodePtr InputFormat;
    NYTree::INodePtr OutputFormat;
    
    int CoresLimit;
    i64 MemoryLimit;

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
        Register("cores_limit", CoresLimit)
            .Default(1);
        Register("memory_limit", MemoryLimit)
            .Default((i64) 1024 * 1024 * 1024);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TMapOperationSpec
    : public TOperationSpecBase
{
    TUserJobSpecPtr Mapper;   
    std::vector<NYTree::TYPath> InputTablePaths;
    std::vector<NYTree::TYPath> OutputTablePaths;
    TNullable<int> JobCount;
    i64 JobSliceWeight;
    i64 MaxWeightPerJob;
    TDuration LocalityTimeout;

    TMapOperationSpec()
    {
        Register("mapper", Mapper);
        Register("input_table_paths", InputTablePaths);
        Register("output_table_paths", OutputTablePaths);
        Register("job_count", JobCount)
            .Default()
            .GreaterThan(0);
        Register("job_slice_weight", JobSliceWeight)
            .Default((i64) 256 * 1024 * 1024)
            .GreaterThan(0);
        Register("max_weight_per_job", MaxWeightPerJob)
            .Default((i64) 1024 * 1024 * 1024)
            .GreaterThan(0);
        Register("locality_timeout", LocalityTimeout)
            .Default(TDuration::Seconds(5));
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
    std::vector<NYTree::TYPath> InputTablePaths;
    NYTree::TYPath OutputTablePath;
    EMergeMode Mode;
    bool CombineChunks;
    TNullable< std::vector<Stroka> > KeyColumns;

    //! During sorted merge the scheduler tries to ensure that large connected
    //! groups of chunks are partitioned into tasks of this or smaller size.
    //! This number, however, is merely an estimate, i.e. some tasks may still
    //! be larger.
    i64 MaxWeightPerJob;

    TDuration LocalityTimeout;

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
        Register("max_weight_per_job", MaxWeightPerJob)
            .Default((i64) 1024 * 1024 * 1024)
            .GreaterThan(0);
        Register("locality_timeout", LocalityTimeout)
            .Default(TDuration::Seconds(5));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TEraseOperationSpec
    : public TOperationSpecBase
{
    NYTree::TYPath TablePath;
    bool CombineChunks;
    i64 MaxWeightPerJob;
    TDuration LocalityTimeout;

    TEraseOperationSpec()
    {
        Register("table_path", TablePath);
        Register("combine_chunks", CombineChunks)
            .Default(false);
        Register("max_weight_per_job", MaxWeightPerJob)
            .Default((i64) 1024 * 1024 * 1024)
            .GreaterThan(0);
        Register("locality_timeout", LocalityTimeout)
            .Default(TDuration::Seconds(5));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSortOperationSpec
    : public TOperationSpecBase
{
    std::vector<NYTree::TYPath> InputTablePaths;
    
    NYTree::TYPath OutputTablePath;
    
    std::vector<Stroka> KeyColumns;
    
    TNullable<int> PartitionCount;
    
    TNullable<int> PartitionJobCount;
    i64 PartitionJobSliceWeight;
    
    //! Only used if no partitioning is done.
    TNullable<int> SortJobCount;

    //! Only used if no partitioning is done.
    i64 SortJobSliceWeight;

    //! Maximum amount of (uncompressed) data to be given to a single partition job.
    i64 MaxWeightPerPartitionJob;

    //! Maximum amount of (uncompressed) data to be given to a single sort job.
    //! By default, the number of partitions is computed as follows:
    //! \code
    //! partitionCount = ceil(totalWeight / MaxWeightPerSortJob * PartitionCountBoostFactor)
    //! \endcode
    //! The user, however, may override this by specifying #PartitionCount explicitly.
    //! Here #PartitionCountBoostFactor accounts for uneven partition sizes and
    //! enables to fit most of sort jobs into #MaxWeightPerSortJob.
    i64 MaxWeightPerSortJob;

    //! See comments for #MaxWeightPerSortJob.
    double PartitionCountBoostFactor;

    //! Maximum amount of (uncompressed) data to be given to a single unordered merge job
    //! that takes care of a megalomaniac partition.
    i64 MaxWeightPerUnorderedMergeJob;

    double SortStartThreshold;

    TDuration PartitionLocalityTimeout;
    TDuration SortLocalityTimeout;
    TDuration MergeLocalityTimeout;

    int ShuffleNetworkLimit;
    double ShuffleNetworkReleaseThreshold;

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
        Register("partition_job_slice_weight", PartitionJobSliceWeight)
            .Default((i64) 256 * 1024 * 1024)
            .GreaterThan(0);
        Register("sort_job_count", SortJobCount)
            .Default()
            .GreaterThan(0);
        Register("sort_job_slice_weight", SortJobSliceWeight)
            .Default((i64) 256 * 1024 * 1024)
            .GreaterThan(0);
        Register("max_weight_per_partition_job", MaxWeightPerPartitionJob)
            .Default((i64) 1024 * 1024 * 1024)
            .GreaterThan(0);
        Register("max_weight_per_sort_job", MaxWeightPerSortJob)
            .Default((i64) 4 * 1024 * 1024 * 1024)
            .GreaterThan(0);
        Register("partition_count_boost_factor", PartitionCountBoostFactor)
            .Default(1.5)
            .GreaterThanOrEqual(1.0);
        Register("max_weight_per_unordered_merge_job", MaxWeightPerUnorderedMergeJob)
            .Default((i64) 1024 * 1024 * 1024)
            .GreaterThan(0);
        Register("sort_start_threshold", SortStartThreshold)
            .Default(0.75)
            .InRange(0.0, 1.0);
        Register("partition_locality_timeout", PartitionLocalityTimeout)
            .Default(TDuration::Seconds(5));
        Register("sort_locality_timeout", SortLocalityTimeout)
            .Default(TDuration::Seconds(30));
        Register("merge_locality_timeout", SortLocalityTimeout)
            .Default(TDuration::Seconds(30));
        Register("shuffle_network_limit", ShuffleNetworkLimit)
            .Default(20);
        Register("shuffle_network_release_threshold", ShuffleNetworkReleaseThreshold)
            .Default(0.8)
            .InRange(0.0, 1.0);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TReduceOperationSpec
    : public TOperationSpecBase
{
    TUserJobSpecPtr Reducer;
    std::vector<NYTree::TYPath> InputTablePaths;
    std::vector<NYTree::TYPath> OutputTablePaths;
    TNullable< std::vector<Stroka> > KeyColumns;
    i64 MaxWeightPerJob;
    TDuration LocalityTimeout;

    TReduceOperationSpec()
    {
        Register("reducer", Reducer);
        Register("input_table_paths", InputTablePaths);
        Register("output_table_paths", OutputTablePaths);
        Register("key_columns", KeyColumns)
            .Default();
        Register("max_weight_per_job", MaxWeightPerJob)
            .Default((i64) 1024 * 1024 * 1024)
            .GreaterThan(0);
        Register("locality_timeout", LocalityTimeout)
            .Default(TDuration::Seconds(5));
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
