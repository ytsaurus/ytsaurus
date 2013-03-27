
#pragma once

#include "public.h"

#include <ytlib/ypath/rich.h>

#include <ytlib/ytree/yson_serializable.h>

#include <ytlib/table_client/config.h>
#include <ytlib/file_client/config.h>

#include <ytlib/formats/format.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TJobIOConfig
    : public TYsonSerializable
{
    NTableClient::TTableReaderConfigPtr TableReader;
    NTableClient::TTableWriterConfigPtr TableWriter;
    NFileClient::TFileWriterConfigPtr ErrorFileWriter;

    TJobIOConfig()
    {
        Register("table_reader", TableReader)
            .DefaultNew();
        Register("table_writer", TableWriter)
            .DefaultNew();
        Register("error_file_writer", ErrorFileWriter)
            .DefaultNew();

        // We do not provide much fault tolerance for stderr by default.
        ErrorFileWriter->ReplicationFactor = 1;
        ErrorFileWriter->UploadReplicationFactor = 1;
        ErrorFileWriter->ChunkVital = false;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TOperationSpecBase
    : public TYsonSerializable
{
    Stroka IntermediateDataAccount;

    bool IgnoreLostChunks;

    TNullable<int> MaxFailedJobCount;
    TNullable<int> MaxStdErrCount;

    TOperationSpecBase()
    {
        Register("intermediate_data_account", IntermediateDataAccount)
            .Default("tmp");

        Register("ignore_lost_chunks", IgnoreLostChunks)
            .Default(false);

        Register("max_failed_job_count", MaxFailedJobCount)
            .Default(Null);
        Register("max_stderr_count", MaxStdErrCount)
            .Default(Null);

        SetKeepOptions(true);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TUserJobSpec
    : public TYsonSerializable
{
    Stroka Command;

    std::vector<NYPath::TRichYPath> FilePaths;

    TNullable<NFormats::TFormat> Format;
    TNullable<NFormats::TFormat> InputFormat;
    TNullable<NFormats::TFormat> OutputFormat;

    yhash_map<Stroka, Stroka> Environment;

    int CpuLimit;
    i64 MemoryLimit;
    double MemoryReserveFactor;

    bool EnableTableIndex;
    bool UseYamrDescriptors;

    TUserJobSpec()
    {
        Register("command", Command)
            .NonEmpty();
        Register("file_paths", FilePaths)
            .Default();
        Register("format", Format)
            .Default();
        Register("input_format", InputFormat)
            .Default();
        Register("output_format", OutputFormat)
            .Default();
        Register("environment", Environment)
            .Default();
        Register("cpu_limit", CpuLimit)
            .Default(1);
        Register("memory_limit", MemoryLimit)
            .Default((i64) 512 * 1024 * 1024);
        Register("memory_reserve_factor", MemoryReserveFactor)
            .Default(0.5)
            .GreaterThan(0.)
            .LessThanOrEqual(1.);
        Register("enable_table_index", EnableTableIndex)
            .Default(false);
        Register("use_yamr_descriptors", UseYamrDescriptors)
            .Default(false);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TMapOperationSpec
    : public TOperationSpecBase
{
    TUserJobSpecPtr Mapper;
    std::vector<NYPath::TRichYPath> InputTablePaths;
    std::vector<NYPath::TRichYPath> OutputTablePaths;
    TNullable<int> JobCount;
    i64 JobSliceDataSize;
    i64 DataSizePerJob;
    TDuration LocalityTimeout;
    TJobIOConfigPtr JobIO;

    TMapOperationSpec()
    {
        Register("mapper", Mapper)
            .DefaultNew();
        Register("input_table_paths", InputTablePaths)
            .NonEmpty();
        Register("output_table_paths", OutputTablePaths);
        Register("job_count", JobCount)
            .Default()
            .GreaterThan(0);
        Register("data_size_per_job", DataSizePerJob)
            .Default((i64) 32 * 1024 * 1024)
            .GreaterThan(0);
        Register("locality_timeout", LocalityTimeout)
            .Default(TDuration::Seconds(5));
        Register("job_io", JobIO)
            .DefaultNew();

        JobIO->TableReader->PrefetchWindow = 10;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TMergeOperationSpecBase
    : public TOperationSpecBase
{
    //! During sorted merge the scheduler tries to ensure that large connected
    //! groups of chunks are partitioned into tasks of this or smaller size.
    //! This number, however, is merely an estimate, i.e. some tasks may still
    //! be larger.
    i64 DataSizePerJob;

    TNullable<int> JobCount;

    i64 JobSliceDataSize;

    TDuration LocalityTimeout;
    TJobIOConfigPtr JobIO;

    TMergeOperationSpecBase()
    {
        Register("job_count", JobCount)
            .Default()
            .GreaterThan(0);
        Register("locality_timeout", LocalityTimeout)
            .Default(TDuration::Seconds(5));
        Register("job_io", JobIO)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EMergeMode,
    (Sorted)
    (Ordered)
    (Unordered)
);

struct TMergeOperationSpec
    : public TMergeOperationSpecBase
{
    std::vector<NYPath::TRichYPath> InputTablePaths;
    NYPath::TRichYPath OutputTablePath;
    EMergeMode Mode;
    bool CombineChunks;
    bool AllowPassthroughChunks;
    TNullable< std::vector<Stroka> > MergeBy;

    TMergeOperationSpec()
    {
        Register("data_size_per_job", DataSizePerJob)
            .Default((i64) 1024 * 1024 * 1024)
            .GreaterThan(0);
        Register("input_table_paths", InputTablePaths)
            .NonEmpty();
        Register("output_table_path", OutputTablePath);
        Register("combine_chunks", CombineChunks)
            .Default(false);
        Register("mode", Mode)
            .Default(EMergeMode::Unordered);
        Register("allow_passthrough_chunks", AllowPassthroughChunks)
            .Default(true);
        Register("merge_by", MergeBy)
            .Default();
    }
};

struct TUnorderedMergeOperationSpec
    : public TMergeOperationSpec
{
    TUnorderedMergeOperationSpec()
    {
        JobIO->TableReader->PrefetchWindow = 10;
    }
};

struct TOrderedMergeOperationSpec
    : public TMergeOperationSpec
{

};

struct TSortedMergeOperationSpec
    : public TMergeOperationSpec
{ };

////////////////////////////////////////////////////////////////////////////////

struct TEraseOperationSpec
    : public TMergeOperationSpecBase
{
    NYPath::TRichYPath TablePath;
    bool CombineChunks;

    TEraseOperationSpec()
    {
        Register("table_path", TablePath);
        Register("combine_chunks", CombineChunks)
            .Default(false);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TReduceOperationSpec
    : public TMergeOperationSpecBase
{
    TUserJobSpecPtr Reducer;
    std::vector<NYPath::TRichYPath> InputTablePaths;
    std::vector<NYPath::TRichYPath> OutputTablePaths;
    TNullable< std::vector<Stroka> > ReduceBy;

    TReduceOperationSpec()
    {
        Register("data_size_per_job", DataSizePerJob)
            .Default((i64) 32 * 1024 * 1024)
            .GreaterThan(0);
        Register("reducer", Reducer)
            .DefaultNew();
        Register("input_table_paths", InputTablePaths)
            .NonEmpty();
        Register("output_table_paths", OutputTablePaths);
        Register("reduce_by", ReduceBy)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSortOperationSpecBase
    : public TOperationSpecBase
{
    std::vector<NYPath::TRichYPath> InputTablePaths;

    //! Amount of (uncompressed) data to be distributed to one partition.
    //! It used only to determine partition count.
    TNullable<i64> PartitionDataSize;
    TNullable<int> PartitionCount;

    //! Amount of (uncompressed) data to be given to a single partition job.
    //! It used only to determine partition job count.
    TNullable<i64> DataSizePerPartitionJob;
    TNullable<int> PartitionJobCount;

    //! Data size per sort job.
    i64 DataSizePerSortJob;

    //! Ratio of data size after partition to data size before partition.
    //! It always equals 1.0 for sort operation.
    double MapSelectivityFactor;

    double ShuffleStartThreshold;
    double MergeStartThreshold;

    TDuration SimpleSortLocalityTimeout;
    TDuration SimpleMergeLocalityTimeout;

    TDuration PartitionLocalityTimeout;
    TDuration SortLocalityTimeout;
    TDuration SortAssignmentTimeout;
    TDuration MergeLocalityTimeout;

    int ShuffleNetworkLimit;

    TSortOperationSpecBase()
    {
        Register("input_table_paths", InputTablePaths)
            .NonEmpty();
        Register("partition_count", PartitionCount)
            .Default()
            .GreaterThan(0);
        Register("partition_data_size", PartitionDataSize)
            .Default()
            .GreaterThan(0);
        Register("data_size_per_sort_job", DataSizePerSortJob)
            .Default((i64)1024 * 1024 * 1024)
            .GreaterThan(0);
        Register("shuffle_start_threshold", ShuffleStartThreshold)
            .Default(0.75)
            .InRange(0.0, 1.0);
        Register("merge_start_threshold", MergeStartThreshold)
            .Default(0.9)
            .InRange(0.0, 1.0);
        Register("sort_locality_timeout", SortLocalityTimeout)
            .Default(TDuration::Minutes(1));
        Register("sort_assignment_timeout", SortAssignmentTimeout)
            .Default(TDuration::Seconds(5));
        Register("shuffle_network_limit", ShuffleNetworkLimit)
            .Default(10);
    }

};

////////////////////////////////////////////////////////////////////////////////

struct TSortOperationSpec
    : public TSortOperationSpecBase
{
    NYPath::TRichYPath OutputTablePath;

    std::vector<Stroka> SortBy;

    // Desired number of samples per partition.
    int SamplesPerPartition;

    TJobIOConfigPtr PartitionJobIO;
    TJobIOConfigPtr SortJobIO;
    TJobIOConfigPtr MergeJobIO;

    TSortOperationSpec()
    {
        Register("output_table_path", OutputTablePath);
        Register("sort_by", SortBy)
            .NonEmpty();
        Register("samples_per_partition", SamplesPerPartition)
            .Default(10)
            .GreaterThan(1);
        Register("partition_job_io", PartitionJobIO)
            .DefaultNew();
        Register("sort_job_io", SortJobIO)
            .DefaultNew();
        Register("merge_job_io", MergeJobIO)
            .DefaultNew();

        // Provide custom names for shared settings.
        Register("partition_job_count", PartitionJobCount)
            .Default()
            .GreaterThan(0);
        Register("data_size_per_partition_job", DataSizePerPartitionJob)
            .Default()
            .GreaterThan(0);
        Register("simple_sort_locality_timeout", SimpleSortLocalityTimeout)
            .Default(TDuration::Seconds(5));
        Register("simple_merge_locality_timeout", SimpleMergeLocalityTimeout)
            .Default(TDuration::Seconds(5));
        Register("partition_locality_timeout", PartitionLocalityTimeout)
            .Default(TDuration::Seconds(5));
        Register("merge_locality_timeout", MergeLocalityTimeout)
            .Default(TDuration::Minutes(1));

        PartitionJobIO->TableReader->PrefetchWindow = 10;
        PartitionJobIO->TableWriter->MaxBufferSize = (i64) 2 * 1024 * 1024 * 1024; // 2 GB

        SortJobIO->TableReader->PrefetchWindow = 10;

        MapSelectivityFactor = 1.0;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TMapReduceOperationSpec
    : public TSortOperationSpecBase
{
    std::vector<NYPath::TRichYPath> OutputTablePaths;

    std::vector<Stroka> SortBy;
    std::vector<Stroka> ReduceBy;

    TUserJobSpecPtr Mapper;
    TUserJobSpecPtr Reducer;

    TJobIOConfigPtr MapJobIO;
    TJobIOConfigPtr SortJobIO;
    TJobIOConfigPtr ReduceJobIO;

    TMapReduceOperationSpec()
    {
        Register("output_table_paths", OutputTablePaths);
        Register("sort_by", SortBy)
            .NonEmpty();
        Register("reduce_by", ReduceBy)
            .Default();
        // Mapper can be absent - leave it NULL by default.
        Register("mapper", Mapper)
            .Default();
        Register("reducer", Reducer)
            .DefaultNew();
        Register("map_job_io", MapJobIO)
            .DefaultNew();
        Register("sort_job_io", SortJobIO)
            .DefaultNew();
        Register("reduce_job_io", ReduceJobIO)
            .DefaultNew();

        // Provide custom names for shared settings.
        Register("map_job_count", PartitionJobCount)
            .Default()
            .GreaterThan(0);
        Register("data_size_per_map_job", DataSizePerPartitionJob)
            .Default()
            .GreaterThan(0);
        Register("map_locality_timeout", PartitionLocalityTimeout)
            .Default(TDuration::Seconds(5));
        Register("reduce_locality_timeout", MergeLocalityTimeout)
            .Default(TDuration::Minutes(1));
        Register("map_selectivity_factor", MapSelectivityFactor)
            .Default(1.0)
            .GreaterThan(0);


        // The following settings are inherited from base but make no sense for map-reduce:
        //   JobSliceDataSize
        //   DataSizePerUnorderedMergeJob
        //   SimpleSortLocalityTimeout
        //   SimpleMergeLocalityTimeout
        //   MapSelectivityFactor

        MapJobIO->TableReader->PrefetchWindow = 10;
        MapJobIO->TableWriter->MaxBufferSize = (i64) 2 * 1024 * 1024 * 1024; // 2 GB

        SortJobIO->TableReader->PrefetchWindow = 10;
    }

    virtual void OnLoaded() override
    {
        if (ReduceBy.empty()) {
            ReduceBy = SortBy;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ESchedulingMode,
    (Fifo)
    (FairShare)
);

struct TPoolConfig
    : public TYsonSerializable
{
    double Weight;
    double MinShareRatio;

    ESchedulingMode Mode;

    TNullable<int> MaxSlots;
    TNullable<int> MaxCpu;
    TNullable<i64> MaxMemory;

    TPoolConfig()
    {
        Register("weight", Weight)
            .Default(1.0)
            .GreaterThanOrEqual(1.0);
        Register("min_share_ratio", MinShareRatio)
            .Default(0.0)
            .InRange(0.0, 1.0);

        Register("mode", Mode)
            .Default(ESchedulingMode::FairShare);

        Register("max_slots", MaxSlots)
            .Default(Null)
            .GreaterThanOrEqual(0);
        Register("max_cpu", MaxCpu)
            .Default(Null)
            .GreaterThanOrEqual(0);
        Register("max_memory", MaxMemory)
            .Default(Null)
            .GreaterThanOrEqual(0);
    }
};

////////////////////////////////////////////////////////////////////

struct TPooledOperationSpec
    : public TYsonSerializable
{
    TNullable<Stroka> Pool;
    double Weight;

    TDuration MinSharePreemptionTimeout;
    double MinShareRatio;

    TDuration FairSharePreemptionTimeout;
    double FairShareStarvationTolerance;
    double FairSharePreemptionTolerance;

    TPooledOperationSpec()
    {
        Register("pool", Pool)
            .Default(TNullable<Stroka>())
            .NonEmpty();
        Register("weight", Weight)
            .Default(1.0)
            .GreaterThanOrEqual(1.0);

        Register("min_share_preemption_timeout", MinSharePreemptionTimeout)
            .Default(TDuration::Seconds(15));
        Register("min_share_ratio", MinShareRatio)
            .Default(1.0)
            .InRange(0.0, 1.0);

        Register("fair_share_preemption_timeout", FairSharePreemptionTimeout)
            .Default(TDuration::Seconds(30));
        Register("fair_share_starvation_tolerance", FairShareStarvationTolerance)
            .InRange(0.0, 1.0)
            .Default(0.8);
        Register("fair_share_preemption_tolerance", FairSharePreemptionTolerance)
            .InRange(0.0, 1.0)
            .Default(0.9);
    }
};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
