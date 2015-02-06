#pragma once

#include "public.h"

#include <ytlib/ypath/rich.h>

#include <ytlib/table_client/config.h>
#include <ytlib/file_client/config.h>

#include <ytlib/meta_state/public.h>
#include <ytlib/meta_state/config.h>

#include <ytlib/node_tracker_client/public.h>

#include <server/security_server/acl.h>

#include <core/formats/format.h>

#include <core/rpc/config.h>
#include <core/rpc/retrying_channel.h>

#include <core/ytree/fluent.h>
#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TJobIOConfig
    : public NYTree::TYsonSerializable
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
            ErrorFileWriter->UploadReplicationFactor = 1;
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

class TOperationSpecBase
    : public NYTree::TYsonSerializable
{
public:
    //! Account holding intermediate data produces by the operation.
    Stroka IntermediateDataAccount;

    //! Codec used for compressing intermediate output during shuffle.
    NCompression::ECodec IntermediateCompressionCodec;

    //! Acl used for intermediate tables and stderrs.
    NYTree::INodePtr IntermediateDataAcl;

    //! What to do during initialization if some chunks are unavailable.
    EUnavailableChunkAction UnavailableChunkStrategy;

    //! What to do during operation progress when some chunks get unavailable.
    EUnavailableChunkAction UnavailableChunkTactics;

    TNullable<int> MaxFailedJobCount;
    TNullable<int> MaxStdErrCount;

    bool JobProxyMemoryControl;

    bool EnableSortVerification;

    TNullable<Stroka> Title;

    TNullable<Stroka> SchedulingTag;

    TOperationSpecBase()
    {
        RegisterParameter("intermediate_data_account", IntermediateDataAccount)
            .Default("tmp");
        RegisterParameter("intermediate_compression_codec", IntermediateCompressionCodec)
            .Default(NCompression::ECodec::Lz4);
        RegisterParameter("intermediate_data_acl", IntermediateDataAcl)
            .Default(NYTree::BuildYsonNodeFluently()
                .BeginList()
                    .Item().BeginMap()
                        .Item("action").Value("allow")
                        .Item("subjects").BeginList()
                            .Item().Value("everyone")
                        .EndList()
                        .Item("permissions").BeginList()
                            .Item().Value("read")
                        .EndList()
                    .EndMap()
                .EndList());

        RegisterParameter("unavailable_chunk_strategy", UnavailableChunkStrategy)
            .Default(EUnavailableChunkAction::Wait);
        RegisterParameter("unavailable_chunk_tactics", UnavailableChunkTactics)
            .Default(EUnavailableChunkAction::Wait);

        RegisterParameter("max_failed_job_count", MaxFailedJobCount)
            .Default(Null);
        RegisterParameter("max_stderr_count", MaxStdErrCount)
            .Default(Null);

        RegisterParameter("job_proxy_memory_control", JobProxyMemoryControl)
            .Default(true);

        RegisterParameter("enable_sort_verification", EnableSortVerification)
            .Default(true);

        RegisterParameter("title", Title)
            .Default(Null);
        
        RegisterParameter("scheduling_tag", SchedulingTag)
            .Default(Null);

        SetKeepOptions(true);

        RegisterValidator([&] () {
            if (UnavailableChunkStrategy == EUnavailableChunkAction::Wait &&
                UnavailableChunkTactics == EUnavailableChunkAction::Skip)
            {
                THROW_ERROR_EXCEPTION("Your tactics conflicts with your strategy, Luke!");
            }
        });

    }
};

////////////////////////////////////////////////////////////////////////////////

class TUserJobSpec
    : public NYTree::TYsonSerializable
{
public:
    Stroka Command;

    std::vector<NYPath::TRichYPath> FilePaths;

    TNullable<NFormats::TFormat> Format;
    TNullable<NFormats::TFormat> InputFormat;
    TNullable<NFormats::TFormat> OutputFormat;

    yhash_map<Stroka, Stroka> Environment;

    int CpuLimit;
    i64 MemoryLimit;
    double MemoryReserveFactor;

    bool UseYamrDescriptors;
    bool CheckInputStreamFullyConsumed;
    bool EnableCoreDump;
    bool EnableIOPrio;

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
        RegisterParameter("use_yamr_descriptors", UseYamrDescriptors)
            .Default(false);
        RegisterParameter("check_input_fully_consumed", CheckInputStreamFullyConsumed)
            .Default(false);
        RegisterParameter("enable_core_dump", EnableCoreDump)
            .Default(false);
        RegisterParameter("enable_io_prio", EnableIOPrio)
            .Default(true);
        RegisterParameter("max_stderr_size", MaxStderrSize)
            .Default((i64)5 * 1024 * 1024) // 5MB
            .GreaterThan(0)
            .LessThanOrEqual((i64)1024 * 1024 * 1024);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMapOperationSpec
    : public TOperationSpecBase
{
public:
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
        RegisterParameter("output_table_paths", OutputTablePaths)
            .NonEmpty();
        RegisterParameter("job_count", JobCount)
            .Default()
            .GreaterThan(0);
        RegisterParameter("data_size_per_job", DataSizePerJob)
            .Default((i64) 128 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("locality_timeout", LocalityTimeout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("job_io", JobIO)
            .DefaultNew();

        RegisterInitializer([&] () {
            JobIO->TableReader->MaxBufferSize = (i64) 256 * 1024 * 1024;
        });
    }

    virtual void OnLoaded() override
    {
        TOperationSpecBase::OnLoaded();

        InputTablePaths = NYT::NYPath::Simplify(InputTablePaths);
        OutputTablePaths = NYT::NYPath::Simplify(OutputTablePaths);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMergeOperationSpecBase
    : public TOperationSpecBase
{
public:
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
            .Default((i64) 256 * 1024 * 1024)
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

class TMergeOperationSpec
    : public TMergeOperationSpecBase
{
public:
    std::vector<NYPath::TRichYPath> InputTablePaths;
    NYPath::TRichYPath OutputTablePath;
    EMergeMode Mode;
    bool CombineChunks;
    bool ForceTransform;
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
        RegisterParameter("force_transform", ForceTransform)
            .Default(false);
        RegisterParameter("merge_by", MergeBy)
            .Default();
    }

    virtual void OnLoaded() override
    {
        TMergeOperationSpecBase::OnLoaded();

        InputTablePaths = NYT::NYPath::Simplify(InputTablePaths);
        OutputTablePath = OutputTablePath.Simplify();
    }
};

class TUnorderedMergeOperationSpec
    : public TMergeOperationSpec
{ };

class TOrderedMergeOperationSpec
    : public TMergeOperationSpec
{ };

class TSortedMergeOperationSpec
    : public TMergeOperationSpec
{ };

////////////////////////////////////////////////////////////////////////////////

class TEraseOperationSpec
    : public TMergeOperationSpecBase
{
public:
    NYPath::TRichYPath TablePath;
    bool CombineChunks;

    TEraseOperationSpec()
    {
        RegisterParameter("table_path", TablePath);
        RegisterParameter("combine_chunks", CombineChunks)
            .Default(false);
    }

    virtual void OnLoaded() override
    {
        TMergeOperationSpecBase::OnLoaded();

        TablePath = TablePath.Simplify();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReduceOperationSpec
    : public TMergeOperationSpecBase
{
public:
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
        RegisterParameter("output_table_paths", OutputTablePaths)
            .NonEmpty();
        RegisterParameter("reduce_by", ReduceBy)
            .Default();

        RegisterInitializer([&] () {
            DataSizePerJob = (i64) 128 * 1024 * 1024;
        });
    }

    virtual void OnLoaded() override
    {
        TMergeOperationSpecBase::OnLoaded();

        InputTablePaths = NYT::NYPath::Simplify(InputTablePaths);
        OutputTablePaths = NYT::NYPath::Simplify(OutputTablePaths);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSortOperationSpecBase
    : public TOperationSpecBase
{
public:
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

    std::vector<Stroka> SortBy;

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

        RegisterParameter("sort_by", SortBy)
            .NonEmpty();
    }

    virtual void OnLoaded() override
    {
        TOperationSpecBase::OnLoaded();

        InputTablePaths = NYT::NYPath::Simplify(InputTablePaths);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSortOperationSpec
    : public TSortOperationSpecBase
{
public:
    NYPath::TRichYPath OutputTablePath;

    // Desired number of samples per partition.
    int SamplesPerPartition;

    TJobIOConfigPtr PartitionJobIO;
    TJobIOConfigPtr SortJobIO;
    TJobIOConfigPtr MergeJobIO;

    TSortOperationSpec()
    {
        RegisterParameter("output_table_path", OutputTablePath);
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

    virtual void OnLoaded() override
    {
        TSortOperationSpecBase::OnLoaded();

        OutputTablePath = OutputTablePath.Simplify();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMapReduceOperationSpec
    : public TSortOperationSpecBase
{
public:
    std::vector<NYPath::TRichYPath> OutputTablePaths;

    std::vector<Stroka> ReduceBy;

    TUserJobSpecPtr Mapper;
    TUserJobSpecPtr ReduceCombiner;
    TUserJobSpecPtr Reducer;

    TJobIOConfigPtr MapJobIO;
    // Also works for ReduceCombiner if present.
    TJobIOConfigPtr SortJobIO;
    TJobIOConfigPtr ReduceJobIO;

    TMapReduceOperationSpec()
    {
        RegisterParameter("output_table_paths", OutputTablePaths)
            .NonEmpty();
        RegisterParameter("reduce_by", ReduceBy)
            .Default();
        // Mapper can be absent -- leave it Null by default.
        RegisterParameter("mapper", Mapper)
            .Default();
        // ReduceCombiner can be absent -- leave it Null by default.
        RegisterParameter("reduce_combiner", ReduceCombiner)
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
            MapJobIO->TableReader->MaxBufferSize = (i64) 256 * 1024 * 1024;
            MapJobIO->TableWriter->MaxBufferSize = (i64) 2 * 1024 * 1024 * 1024; // 2 GBs

            SortJobIO->TableReader->MaxBufferSize = (i64) 1024 * 1024 * 1024;
        });
    }

    virtual void OnLoaded() override
    {
        TSortOperationSpecBase::OnLoaded();

        if (ReduceBy.empty()) {
            ReduceBy = SortBy;
        }

        OutputTablePaths = NYT::NYPath::Simplify(OutputTablePaths);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRemoteCopyOperationSpec
    : public TOperationSpecBase
{
public:
    Stroka ClusterName;
    Stroka NetworkName;
    std::vector<NYPath::TRichYPath> InputTablePaths;
    NYPath::TRichYPath OutputTablePath;
    TNullable<int> JobCount;
    i64 DataSizePerJob;
    TJobIOConfigPtr JobIO;
    int MaxChunkCountPerJob;

    TRemoteCopyOperationSpec()
    {
        RegisterParameter("cluster_name", ClusterName);
        RegisterParameter("input_table_paths", InputTablePaths)
            .NonEmpty();
        RegisterParameter("output_table_path", OutputTablePath);
        RegisterParameter("job_count", JobCount)
            .Default()
            .GreaterThan(0);
        RegisterParameter("data_size_per_job", DataSizePerJob)
            .Default((i64) 1024 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("job_io", JobIO)
            .DefaultNew();
        RegisterParameter("network_name", NetworkName)
            .Default(NNodeTrackerClient::DefaultNetworkName);
        RegisterParameter("max_chunk_count_per_job", MaxChunkCountPerJob)
            .Default(100);
    }

    virtual void OnLoaded() override
    {
        TOperationSpecBase::OnLoaded();

        InputTablePaths = NYT::NYPath::Simplify(InputTablePaths);
        OutputTablePath.Simplify();
    }
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ESchedulingMode,
    (Fifo)
    (FairShare)
);

class TPoolResourceLimitsConfig
    : public NYTree::TYsonSerializable
{
public:
    TNullable<int> UserSlots;
    TNullable<int> Cpu;
    TNullable<i64> Memory;

    TPoolResourceLimitsConfig()
    {
        RegisterParameter("user_slots", UserSlots)
            .Default(Null)
            .GreaterThanOrEqual(0);
        RegisterParameter("cpu", Cpu)
            .Default(Null)
            .GreaterThanOrEqual(0);
        RegisterParameter("memory", Memory)
            .Default(Null)
            .GreaterThanOrEqual(0);
    }
};

class TPoolConfig
    : public NYTree::TYsonSerializable
{
public:
    double Weight;
    double MinShareRatio;
    double MaxShareRatio;

    ESchedulingMode Mode;

    TPoolResourceLimitsConfigPtr ResourceLimits;

    TNullable<Stroka> SchedulingTag;

    TPoolConfig()
    {
        RegisterParameter("weight", Weight)
            .Default(1.0)
            .GreaterThanOrEqual(0.0);
        RegisterParameter("min_share_ratio", MinShareRatio)
            .Default(0.0)
            .InRange(0.0, 1.0);
        RegisterParameter("max_share_ratio", MaxShareRatio)
            .Default(1.0)
            .InRange(0.0, 1.0);

        RegisterParameter("mode", Mode)
            .Default(ESchedulingMode::FairShare);

        RegisterParameter("resource_limits", ResourceLimits)
            .DefaultNew();
        
        RegisterParameter("scheduling_tag", SchedulingTag)
            .Default(Null);
    }
};

////////////////////////////////////////////////////////////////////

class TStrategyOperationSpec
    : public NYTree::TYsonSerializable
{
public:
    TNullable<Stroka> Pool;
    TNullable<Stroka> SchedulingTag;
    double Weight;
    double MinShareRatio;
    double MaxShareRatio;

    // The following settings override schedule configuration.
    TNullable<TDuration> MinSharePreemptionTimeout;
    TNullable<TDuration> FairSharePreemptionTimeout;
    TNullable<double> FairShareStarvationTolerance;

    TStrategyOperationSpec()
    {
        RegisterParameter("pool", Pool)
            .Default()
            .NonEmpty();
        RegisterParameter("weight", Weight)
            .Default(1.0)
            .GreaterThanOrEqual(0.0);
        RegisterParameter("min_share_ratio", MinShareRatio)
            .Default(0.0)
            .InRange(0.0, 1.0);
        RegisterParameter("max_share_ratio", MaxShareRatio)
            .Default(1.0)
            .InRange(0.0, 1.0);

        RegisterParameter("scheduling_tag", SchedulingTag)
            .Default();

        RegisterParameter("min_share_preemption_timeout", MinSharePreemptionTimeout)
            .Default();
        RegisterParameter("fair_share_preemption_timeout", FairSharePreemptionTimeout)
            .Default();
        RegisterParameter("fair_share_starvation_tolerance", FairShareStarvationTolerance)
            .InRange(0.0, 1.0)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////

class TOperationRuntimeParams
    : public NYTree::TYsonSerializable
{
public:
    double Weight;

    TOperationRuntimeParams()
    {
        RegisterParameter("weight", Weight)
            .Default(1.0)
            .GreaterThanOrEqual(0.0);
    }
};

////////////////////////////////////////////////////////////////////

class TSchedulerConnectionConfig
    : public NRpc::TRetryingChannelConfig
{
public:
    //! Timeout for RPC requests to schedulers.
    TDuration RpcTimeout;

    TSchedulerConnectionConfig()
    {
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(60));
    }
};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
