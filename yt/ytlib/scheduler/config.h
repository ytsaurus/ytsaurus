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
    ESchedulerStrategy Strategy;
    TMapControllerConfigPtr Map;
    TMergeControllerConfigPtr Merge;

    TSchedulerConfig()
    {
        Register("transactions_refresh_period", TransactionsRefreshPeriod)
            .Default(TDuration::Seconds(15));
        Register("nodes_refresh_period", NodesRefreshPeriod)
            .Default(TDuration::Seconds(15));
        Register("strategy", Strategy)
            .Default(ESchedulerStrategy::Null);
        Register("map", Map)
            .DefaultNew();
        Register("merge", Merge)
            .DefaultNew();
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

struct TOperationControllerConfigBase
    : public TConfigurable
{
    //! Once this limit is reached the operation fails.
    int MaxFailedJobCount;
    //! The additional number of chunk lists to preallocate during preparation phase.
    //! These chunk lists are used in case of job failures.
    int SpareChunkListCount;
    //! Each time we run out of free chunk lists and unable to provide another |count| chunk lists,
    //! job scheduling gets suspended until |count * ChunkListAllocationMultiplier| chunk lists are allocated.
    int ChunkListAllocationMultiplier;

    TOperationControllerConfigBase()
    {
        Register("max_failed_job_count", MaxFailedJobCount)
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

struct TMapControllerConfig
    : public TOperationControllerConfigBase
{ };

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

struct TMergeControllerConfig
    : public TOperationControllerConfigBase
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
