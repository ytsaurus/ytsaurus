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
    TDuration TransactionsRefreshPeriod;
    TDuration NodesRefreshPeriod;
    TDuration OperationsUpdatePeriod;

    ESchedulerStrategy Strategy;
    //! Once this limit is reached the operation fails.
    int FailedJobsLimit;
    //! The additional number of chunk lists to preallocate during preparation phase.
    //! These chunk lists are used in case of job failures.
    int SpareChunkListCount;
    //! Each time we run out of free chunk lists and unable to provide another |count| chunk lists,
    //! job scheduling gets suspended until |count * ChunkListAllocationMultiplier| chunk lists are allocated.
    int ChunkListAllocationMultiplier;

    TSchedulerConfig()
    {
        Register("transactions_refresh_period", TransactionsRefreshPeriod)
            .Default(TDuration::Seconds(3));
        Register("nodes_refresh_period", NodesRefreshPeriod)
            .Default(TDuration::Seconds(15));
        Register("operations_update_period", OperationsUpdatePeriod)
            .Default(TDuration::Seconds(3));
        Register("strategy", Strategy)
            .Default(ESchedulerStrategy::Null);
        Register("failed_jobs_limit", FailedJobsLimit)
            .Default(10)
            .GreaterThanOrEqual(0);
        Register("spare_chunk_list_count", SpareChunkListCount)
            .Default(5)
            .GreaterThanOrEqual(0);
        Register("chunk_list_allocation_multiplier", ChunkListAllocationMultiplier)
            .Default(3)
            .GreaterThan(0);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TOperationSpecBase
    : public TConfigurable
{
    TNullable<int> JobCount;
    NJobProxy::TJobIOConfigPtr JobIO;

    TOperationSpecBase()
    {
        SetKeepOptions(true);
        // TODO(babenko): validate > 0
        Register("job_count", JobCount)
            .Default();
        Register("job_io", JobIO)
            .DefaultNew();
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

    TMapOperationSpec()
    {
        Register("mapper", Mapper);
        Register("file_paths", FilePaths)
            .Default(yvector<NYTree::TYPath>());
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

} // namespace NScheduler
} // namespace NYT
