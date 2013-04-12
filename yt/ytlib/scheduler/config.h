
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

class TJobIOConfig
    : public TYsonSerializable
{
public:
    NTableClient::TTableReaderConfigPtr TableReader;
    NTableClient::TTableWriterConfigPtr TableWriter;
    NFileClient::TFileWriterConfigPtr ErrorFileWriter;

    TJobIOConfig()
    {
        RegisterParameter("table_reader", TableReader)
            .DefaultNew();
        RegisterParameter("table_writer", TableWriter)
            .DefaultNew();
        RegisterParameter("error_file_writer", ErrorFileWriter)
            .DefaultNew();

        RegisterInitializer([&] () {
            ErrorFileWriter->ReplicationFactor = 1;
            ErrorFileWriter->UploadReplicationFactor = 1;
            ErrorFileWriter->ChunkVital = false;
        });
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
        RegisterParameter("intermediate_data_account", IntermediateDataAccount)
            .Default("tmp");

        RegisterParameter("ignore_lost_chunks", IgnoreLostChunks)
            .Default(false);

        RegisterParameter("max_failed_job_count", MaxFailedJobCount)
            .Default(Null);
        RegisterParameter("max_stderr_count", MaxStdErrCount)
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

    i64 MaxStderrSize;

    TUserJobSpec()
    {
        RegisterParameter("command", Command)
            .NonEmpty();
        RegisterParameter("file_paths", FilePaths)
            .Default();
        RegisterParameter("format", Format)
            .Default();
        RegisterParameter("input_format", InputFormat)
            .Default();
        RegisterParameter("output_format", OutputFormat)
            .Default();
        RegisterParameter("environment", Environment)
            .Default();
        RegisterParameter("cpu_limit", CpuLimit)
            .Default(1);
        RegisterParameter("memory_limit", MemoryLimit)
            .Default((i64) 512 * 1024 * 1024);
        RegisterParameter("memory_reserve_factor", MemoryReserveFactor)
            .Default(0.5)
            .GreaterThan(0.)
            .LessThanOrEqual(1.);
        RegisterParameter("enable_table_index", EnableTableIndex)
            .Default(false);
        RegisterParameter("use_yamr_descriptors", UseYamrDescriptors)
            .Default(false);
        RegisterParameter("max_stderr_size", MaxStderrSize)
            .Default((i64)5 * 1024 * 1024) // 5MB
            .GreaterThan(0)
            .LessThanOrEqual((i64)1024 * 1024 * 1024);
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
    i64 DataSizePerJob;
    TDuration LocalityTimeout;
    TJobIOConfigPtr JobIO;

    TMapOperationSpec()
    {
        RegisterParameter("mapper", Mapper)
            .DefaultNew();
        RegisterParameter("input_table_paths", InputTablePaths)
            .NonEmpty();
        RegisterParameter("output_table_paths", OutputTablePaths);
        RegisterParameter("job_count", JobCount)
            .Default()
            .GreaterThan(0);
        RegisterParameter("data_size_per_job", DataSizePerJob)
            .Default((i64) 32 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("locality_timeout", LocalityTimeout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("job_io", JobIO)
            .DefaultNew();

        RegisterInitializer([&] () {
            JobIO->TableReader->MaxBufferSize = 1024L * 1024 * 1024;
        });
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

    TDuration LocalityTimeout;
    TJobIOConfigPtr JobIO;

    TMergeOperationSpecBase()
        : DataSizePerJob(-1)
    {
        RegisterParameter("data_size_per_job", DataSizePerJob)
            .Default((i64) 1024 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("job_count", JobCount)
            .Default()
            .GreaterThan(0);
        RegisterParameter("locality_timeout", LocalityTimeout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("job_io", JobIO)
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
        RegisterParameter("input_table_paths", InputTablePaths)
            .NonEmpty();
        RegisterParameter("output_table_path", OutputTablePath);
        RegisterParameter("mode", Mode)
            .Default(EMergeMode::Unordered);
        RegisterParameter("combine_chunks", CombineChunks)
            .Default(false);
        RegisterParameter("allow_passthrough_chunks", AllowPassthroughChunks)
            .Default(true);
        RegisterParameter("merge_by", MergeBy)
            .Default();
    }
};

struct TUnorderedMergeOperationSpec
    : public TMergeOperationSpec
{
    TUnorderedMergeOperationSpec()
    {
        RegisterInitializer([&] () {
            JobIO->TableReader->MaxBufferSize = 1024L * 1024 * 1024;
        });
    }
};

struct TOrderedMergeOperationSpec
    : public TMergeOperationSpec
{ };

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
        RegisterParameter("table_path", TablePath);
        RegisterParameter("combine_chunks", CombineChunks)
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
        RegisterParameter("reducer", Reducer)
            .DefaultNew();
        RegisterParameter("input_table_paths", InputTablePaths)
            .NonEmpty();
        RegisterParameter("output_table_paths", OutputTablePaths);
        RegisterParameter("reduce_by", ReduceBy)
            .Default();

        RegisterInitializer([&] () {
            DataSizePerJob = (i64) 32 * 1024 * 1024;
        });
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
        RegisterParameter("input_table_paths", InputTablePaths)
            .NonEmpty();
        RegisterParameter("partition_count", PartitionCount)
            .Default()
            .GreaterThan(0);
        RegisterParameter("partition_data_size", PartitionDataSize)
            .Default()
            .GreaterThan(0);
        RegisterParameter("data_size_per_sort_job", DataSizePerSortJob)
            .Default((i64)2 * 1024 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("shuffle_start_threshold", ShuffleStartThreshold)
            .Default(0.75)
            .InRange(0.0, 1.0);
        RegisterParameter("merge_start_threshold", MergeStartThreshold)
            .Default(0.9)
            .InRange(0.0, 1.0);
        RegisterParameter("sort_locality_timeout", SortLocalityTimeout)
            .Default(TDuration::Minutes(1));
        RegisterParameter("sort_assignment_timeout", SortAssignmentTimeout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("shuffle_network_limit", ShuffleNetworkLimit)
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
        RegisterParameter("output_table_path", OutputTablePath);
        RegisterParameter("sort_by", SortBy)
            .NonEmpty();
        RegisterParameter("samples_per_partition", SamplesPerPartition)
            .Default(10)
            .GreaterThan(1);
        RegisterParameter("partition_job_io", PartitionJobIO)
            .DefaultNew();
        RegisterParameter("sort_job_io", SortJobIO)
            .DefaultNew();
        RegisterParameter("merge_job_io", MergeJobIO)
            .DefaultNew();

        // Provide custom names for shared settings.
        RegisterParameter("partition_job_count", PartitionJobCount)
            .Default()
            .GreaterThan(0);
        RegisterParameter("data_size_per_partition_job", DataSizePerPartitionJob)
            .Default()
            .GreaterThan(0);
        RegisterParameter("simple_sort_locality_timeout", SimpleSortLocalityTimeout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("simple_merge_locality_timeout", SimpleMergeLocalityTimeout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("partition_locality_timeout", PartitionLocalityTimeout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("merge_locality_timeout", MergeLocalityTimeout)
            .Default(TDuration::Minutes(1));

        RegisterInitializer([&] () {
            PartitionJobIO->TableReader->MaxBufferSize = (i64) 1024 * 1024 * 1024;
            PartitionJobIO->TableWriter->MaxBufferSize = (i64) 2 * 1024 * 1024 * 1024; // 2 GB

            SortJobIO->TableReader->MaxBufferSize = (i64) 1024 * 1024 * 1024;

            MapSelectivityFactor = 1.0;
        });
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
        RegisterParameter("output_table_paths", OutputTablePaths);
        RegisterParameter("sort_by", SortBy)
            .NonEmpty();
        RegisterParameter("reduce_by", ReduceBy)
            .Default();
        // Mapper can be absent - leave it NULL by default.
        RegisterParameter("mapper", Mapper)
            .Default();
        RegisterParameter("reducer", Reducer)
            .DefaultNew();
        RegisterParameter("map_job_io", MapJobIO)
            .DefaultNew();
        RegisterParameter("sort_job_io", SortJobIO)
            .DefaultNew();
        RegisterParameter("reduce_job_io", ReduceJobIO)
            .DefaultNew();

        // Provide custom names for shared settings.
        RegisterParameter("map_job_count", PartitionJobCount)
            .Default()
            .GreaterThan(0);
        RegisterParameter("data_size_per_map_job", DataSizePerPartitionJob)
            .Default()
            .GreaterThan(0);
        RegisterParameter("map_locality_timeout", PartitionLocalityTimeout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("reduce_locality_timeout", MergeLocalityTimeout)
            .Default(TDuration::Minutes(1));
        RegisterParameter("map_selectivity_factor", MapSelectivityFactor)
            .Default(1.0)
            .GreaterThan(0);

        // The following settings are inherited from base but make no sense for map-reduce:
        //   DataSizePerUnorderedMergeJob
        //   SimpleSortLocalityTimeout
        //   SimpleMergeLocalityTimeout
        //   MapSelectivityFactor

        RegisterInitializer([&] () {
            MapJobIO->TableReader->MaxBufferSize = (i64) 1024 * 1024 * 1024;
            MapJobIO->TableWriter->MaxBufferSize = (i64) 2 * 1024 * 1024 * 1024; // 2 GB

            SortJobIO->TableReader->MaxBufferSize = (i64) 1024 * 1024 * 1024;
        });
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

class TPoolConfig
    : public TYsonSerializable
{
public:
    double Weight;
    double MinShareRatio;

    ESchedulingMode Mode;

    TNullable<int> MaxSlots;
    TNullable<int> MaxCpu;
    TNullable<i64> MaxMemory;

    TPoolConfig()
    {
        RegisterParameter("weight", Weight)
            .Default(1.0)
            .GreaterThanOrEqual(1.0);
        RegisterParameter("min_share_ratio", MinShareRatio)
            .Default(0.0)
            .InRange(0.0, 1.0);

        RegisterParameter("mode", Mode)
            .Default(ESchedulingMode::FairShare);

        RegisterParameter("max_slots", MaxSlots)
            .Default(Null)
            .GreaterThanOrEqual(0);
        RegisterParameter("max_cpu", MaxCpu)
            .Default(Null)
            .GreaterThanOrEqual(0);
        RegisterParameter("max_memory", MaxMemory)
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

    double MinShareRatio;

    // The following settings override schedule configuration.
    TNullable<TDuration> MinSharePreemptionTimeout;
    TNullable<TDuration> FairSharePreemptionTimeout;
    TNullable<double> FairShareStarvationTolerance;
    TNullable<double> FairSharePreemptionTolerance;

    TPooledOperationSpec()
    {
        RegisterParameter("pool", Pool)
            .Default(TNullable<Stroka>())
            .NonEmpty();
        RegisterParameter("weight", Weight)
            .Default(1.0)
            .GreaterThanOrEqual(1.0);

        RegisterParameter("min_share_ratio", MinShareRatio)
            .Default(1.0)
            .InRange(0.0, 1.0);

        RegisterParameter("min_share_preemption_timeout", MinSharePreemptionTimeout)
            .Default(Null);
        RegisterParameter("fair_share_preemption_timeout", FairSharePreemptionTimeout)
            .Default(Null);
        RegisterParameter("fair_share_starvation_tolerance", FairShareStarvationTolerance)
            .InRange(0.0, 1.0)
            .Default(Null);
        RegisterParameter("fair_share_preemption_tolerance", FairSharePreemptionTolerance)
            .GreaterThanOrEqual(0.0)
            .Default(Null);
    }
};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
