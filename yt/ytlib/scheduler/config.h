#pragma once

#include "public.h"
#include "helpers.h"

#include <yt/ytlib/api/config.h>

#include <yt/ytlib/formats/format.h>
#include <yt/ytlib/formats/config.h>

#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/helpers.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/ytlib/ypath/rich.h>

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

// Ratio of MaxWeight and MinWeight shouldn't lose precision.
const double MinSchedulableWeight = sqrt(std::numeric_limits<double>::epsilon());
const double MaxSchedulableWeight = 1.0 / MinSchedulableWeight;

////////////////////////////////////////////////////////////////////////////////

class TJobIOConfig
    : public NYTree::TYsonSerializable
{
public:
    NTableClient::TTableReaderConfigPtr TableReader;
    NTableClient::TTableWriterConfigPtr TableWriter;

    NFormats::TControlAttributesConfigPtr ControlAttributes;

    NApi::TFileWriterConfigPtr ErrorFileWriter;

    i64 BufferRowCount;

    int PipeIOPoolSize;

    TJobIOConfig()
    {
        RegisterParameter("table_reader", TableReader)
            .DefaultNew();
        RegisterParameter("table_writer", TableWriter)
            .DefaultNew();

        RegisterParameter("control_attributes", ControlAttributes)
            .DefaultNew();

        RegisterParameter("error_file_writer", ErrorFileWriter)
            .DefaultNew();

        RegisterParameter("buffer_row_count", BufferRowCount)
            .Default((i64) 10000)
            .GreaterThan(0);

        RegisterParameter("pipe_io_pool_size", PipeIOPoolSize)
            .Default(1)
            .GreaterThan(0);

        RegisterInitializer([&] () {
            ErrorFileWriter->UploadReplicationFactor = 1;
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestingOperationOptions
    : public NYTree::TYsonSerializable
{
public:
    TDuration SchedulingDelay;

    TTestingOperationOptions()
    {
        RegisterParameter("scheduling_delay", SchedulingDelay)
            .Default(TDuration::Seconds(0));
    };
};

DEFINE_REFCOUNTED_TYPE(TTestingOperationOptions);

////////////////////////////////////////////////////////////////////////////////

class TOperationSpecBase
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Account holding intermediate data produces by the operation.
    Stroka IntermediateDataAccount;

    //! Codec used for compressing intermediate output during shuffle.
    NCompression::ECodec IntermediateCompressionCodec;

    //! Replication factor for intermediate data.
    int IntermediateDataReplicationFactor;

    //! Acl used for intermediate tables and stderrs.
    NYTree::INodePtr IntermediateDataAcl;

    //! Account for job nodes and operation files (stderrs and input contexts of failed jobs).
    Stroka JobNodeAccount;

    //! What to do during initialization if some chunks are unavailable.
    EUnavailableChunkAction UnavailableChunkStrategy;

    //! What to do during operation progress when some chunks get unavailable.
    EUnavailableChunkAction UnavailableChunkTactics;

    i64 MaxDataSizePerJob;

    //! Once this limit is reached the operation fails.
    int MaxFailedJobCount;

    //! Maximum number of saved stderr per job type.
    int MaxStderrCount;

    TNullable<i64> JobProxyMemoryOvercommitLimit;

    TDuration JobProxyRefCountedTrackerLogPeriod;

    bool EnableSortVerification;

    TNullable<Stroka> Title;

    TNullable<Stroka> SchedulingTag;

    //! Limit on operation execution time.
    TNullable<TDuration> TimeLimit;

    bool CheckMultichunkFiles;

    TTestingOperationOptionsPtr TestingOperationOptions;

    //! Users that can change operation parameters, e.g abort or suspend it.
    std::vector<Stroka> Owners;

    //! A storage keeping YSON map that is hidden under ACL in Cypress. It will be exported
    //! to all user jobs via environment variables.
    NYTree::IMapNodePtr SecureVault;

    //! Intentionally fails the operation controller. Used only for testing purposes.
    bool FailController;

    TOperationSpecBase()
    {
        RegisterParameter("intermediate_data_account", IntermediateDataAccount)
            .Default("intermediate");
        RegisterParameter("intermediate_compression_codec", IntermediateCompressionCodec)
            .Default(NCompression::ECodec::Lz4);
        RegisterParameter("intermediate_data_replication_factor", IntermediateDataReplicationFactor)
            .Default(1);
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

        RegisterParameter("job_node_account", JobNodeAccount)
            .Default(NSecurityClient::TmpAccountName);

        RegisterParameter("unavailable_chunk_strategy", UnavailableChunkStrategy)
            .Default(EUnavailableChunkAction::Wait);
        RegisterParameter("unavailable_chunk_tactics", UnavailableChunkTactics)
            .Default(EUnavailableChunkAction::Wait);

        RegisterParameter("max_data_size_per_job", MaxDataSizePerJob)
            .Default((i64) 200 * 1024 * 1024 * 1024)
            .GreaterThan(0);

        RegisterParameter("max_failed_job_count", MaxFailedJobCount)
            .Default(100)
            .GreaterThanOrEqual(0)
            .LessThanOrEqual(10000);
        RegisterParameter("max_stderr_count", MaxStderrCount)
            .Default(100)
            .GreaterThanOrEqual(0)
            .LessThanOrEqual(100);

        RegisterParameter("job_proxy_memory_overcommit_limit", JobProxyMemoryOvercommitLimit)
            .Default()
            .GreaterThanOrEqual(0);

        RegisterParameter("job_proxy_ref_counted_tracker_log_period", JobProxyRefCountedTrackerLogPeriod)
            .Default(TDuration::Seconds(5));

        RegisterParameter("enable_sort_verification", EnableSortVerification)
            .Default(true);

        RegisterParameter("title", Title)
            .Default();

        RegisterParameter("scheduling_tag", SchedulingTag)
            .Default();

        RegisterParameter("check_multichunk_files", CheckMultichunkFiles)
            .Default(true);

        RegisterParameter("time_limit", TimeLimit)
            .Default();

        RegisterParameter("testing", TestingOperationOptions)
            .Default();

        RegisterParameter("owners", Owners)
            .Default();

        RegisterParameter("secure_vault", SecureVault)
            .Default();

        RegisterParameter("fail_controller", FailController)
            .Default(false);

        RegisterValidator([&] () {
            if (UnavailableChunkStrategy == EUnavailableChunkAction::Wait &&
                UnavailableChunkTactics == EUnavailableChunkAction::Skip)
            {
                THROW_ERROR_EXCEPTION("Your tactics conflicts with your strategy, Luke!");
            }
        });

        RegisterValidator([&] () {
            if (SecureVault) {
                for (const auto& name : SecureVault->GetKeys()) {
                    ValidateEnvironmentVariableName(name);
                }
            }
        });

        // XXX(ignat): it seems that GetOptions is not used for this config.
        // Should we delete this line?
        SetKeepOptions(true);
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

    TNullable<bool> EnableInputTableIndex;

    yhash_map<Stroka, Stroka> Environment;

    int CpuLimit;
    TNullable<TDuration> JobTimeLimit;
    i64 MemoryLimit;
    double MemoryReserveFactor;

    bool IncludeMemoryMappedFiles;

    bool UseYamrDescriptors;
    bool CheckInputFullyConsumed;

    i64 MaxStderrSize;

    i64 CustomStatisticsCountLimit;

    TNullable<i64> TmpfsSize;
    TNullable<Stroka> TmpfsPath;

    bool CopyFiles;

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
        RegisterParameter("enable_input_table_index", EnableInputTableIndex)
            .Default();
        RegisterParameter("environment", Environment)
            .Default();
        RegisterParameter("cpu_limit", CpuLimit)
            .Default(1);
        RegisterParameter("job_time_limit", JobTimeLimit)
            .Default()
            .GreaterThanOrEqual(TDuration::Seconds(1));
        RegisterParameter("memory_limit", MemoryLimit)
            .Default((i64) 512 * 1024 * 1024)
            .GreaterThan(0)
            .LessThanOrEqual((i64)1024 * 1024 * 1024 * 1024);
        RegisterParameter("memory_reserve_factor", MemoryReserveFactor)
            .Default(0.5)
            .GreaterThan(0.)
            .LessThanOrEqual(1.);
        RegisterParameter("include_memory_mapped_files", IncludeMemoryMappedFiles)
            .Default(true);
        RegisterParameter("use_yamr_descriptors", UseYamrDescriptors)
            .Default(false);
        RegisterParameter("check_input_fully_consumed", CheckInputFullyConsumed)
            .Default(false);
        RegisterParameter("max_stderr_size", MaxStderrSize)
            .Default((i64)5 * 1024 * 1024) // 5MB
            .GreaterThan(0)
            .LessThanOrEqual((i64)1024 * 1024 * 1024);
        RegisterParameter("custom_statistics_count_limit", CustomStatisticsCountLimit)
            .Default(128)
            .GreaterThan(0)
            .LessThanOrEqual(1024);
        RegisterParameter("tmpfs_size", TmpfsSize)
            .Default(Null)
            .GreaterThan(0);
        RegisterParameter("tmpfs_path", TmpfsPath)
            .Default(Null);
        RegisterParameter("copy_files", CopyFiles)
            .Default(false);

        RegisterValidator([&] () {
            if (TmpfsSize && *TmpfsSize > MemoryLimit) {
                THROW_ERROR_EXCEPTION("Size of tmpfs must be less than or equal to memory limit")
                    << TErrorAttribute("tmpfs_size", *TmpfsSize)
                    << TErrorAttribute("memory_limit", MemoryLimit);
            }
            // Memory reserve should greater than or equal to tmpfs_size (see YT-5518 for more details).
            if (TmpfsPath) {
                i64 tmpfsSize = TmpfsSize ? *TmpfsSize : MemoryLimit;
                MemoryReserveFactor = std::min(1.0, std::max(MemoryReserveFactor, double(tmpfsSize) / MemoryLimit));
            }
        });

        RegisterValidator([&] () {
            for (const auto& pair : Environment) {
                ValidateEnvironmentVariableName(pair.first);
            }
        });
    }

    void InitEnableInputTableIndex(int inputTableCount, TJobIOConfigPtr jobIOConfig)
    {
        if (!EnableInputTableIndex) {
            EnableInputTableIndex = (inputTableCount != 1);
        }

        jobIOConfig->ControlAttributes->EnableTableIndex = *EnableInputTableIndex;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TInputlyQueryableSpec
    : public virtual NYTree::TYsonSerializable
{
public:
    TNullable<Stroka> InputQuery;
    TNullable<NTableClient::TTableSchema> InputSchema;

    TInputlyQueryableSpec()
    {
        RegisterParameter("input_query", InputQuery)
            .Default();
        RegisterParameter("input_schema", InputSchema)
            .Default();

        RegisterValidator([&] () {
            if (InputQuery && !InputSchema) {
                THROW_ERROR_EXCEPTION("Expected to see \"input_schema\" in operation spec");
            }
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

class TOperationWithUserJobSpec
    : public virtual NYTree::TYsonSerializable
{
public:
    TNullable<NYPath::TRichYPath> StderrTablePath;
    NTableClient::TBlobTableWriterConfigPtr StderrTableWriterConfig;

    TNullable<NYPath::TRichYPath> CoreTablePath;
    NTableClient::TBlobTableWriterConfigPtr CoreTableWriterConfig;

    TOperationWithUserJobSpec()
    {
        RegisterParameter("stderr_table_path", StderrTablePath)
            .Default();
        RegisterParameter("stderr_table_writer_config", StderrTableWriterConfig)
            .DefaultNew();

        RegisterParameter("core_table_path", CoreTablePath)
            .Default();
        RegisterParameter("core_table_writer_config", CoreTableWriterConfig)
            .DefaultNew();
    }

    virtual void OnLoaded() override
    {
        if (StderrTablePath) {
            *StderrTablePath = StderrTablePath->Normalize();
        }

        if (CoreTablePath) {
            *CoreTablePath = CoreTablePath->Normalize();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleOperationSpecBase
    : public TOperationSpecBase
{
public:
    //! During sorted merge the scheduler tries to ensure that large connected
    //! groups of chunks are partitioned into tasks of this or smaller size.
    //! This number, however, is merely an estimate, i.e. some tasks may still
    //! be larger.
    TNullable<i64> DataSizePerJob;

    TNullable<int> JobCount;
    TNullable<int> MaxJobCount;

    TDuration LocalityTimeout;
    TJobIOConfigPtr JobIO;

    // Operations inherited from this class produce the only kind
    // of jobs. This option corresponds to jobs of this kind.
    TLogDigestConfigPtr JobProxyMemoryDigest;

    TSimpleOperationSpecBase()
    {
        RegisterParameter("data_size_per_job", DataSizePerJob)
            .Default()
            .GreaterThan(0);
        RegisterParameter("job_count", JobCount)
            .Default()
            .GreaterThan(0);
        RegisterParameter("max_job_count", MaxJobCount)
            .Default()
            .GreaterThan(0);
        RegisterParameter("locality_timeout", LocalityTimeout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("job_io", JobIO)
            .DefaultNew();

        RegisterParameter("job_proxy_memory_digest", JobProxyMemoryDigest)
            .Default(New<TLogDigestConfig>(0.5, 2.0, 1.0));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TUnorderedOperationSpecBase
    : public TSimpleOperationSpecBase
    , public TInputlyQueryableSpec
{
public:
    std::vector<NYPath::TRichYPath> InputTablePaths;

    TUnorderedOperationSpecBase()
    {
        RegisterParameter("input_table_paths", InputTablePaths)
            .NonEmpty();

        RegisterInitializer([&] () {
            JobIO->TableReader->MaxBufferSize = (i64) 256 * 1024 * 1024;
        });
    }

    virtual void OnLoaded() override
    {
        TSimpleOperationSpecBase::OnLoaded();

        InputTablePaths = NYT::NYPath::Normalize(InputTablePaths);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMapOperationSpec
    : public TUnorderedOperationSpecBase
    , public TOperationWithUserJobSpec
{
public:
    TUserJobSpecPtr Mapper;
    std::vector<NYPath::TRichYPath> OutputTablePaths;
    bool Ordered;

    TMapOperationSpec()
    {
        RegisterParameter("mapper", Mapper)
            .DefaultNew();
        RegisterParameter("output_table_paths", OutputTablePaths)
            .NonEmpty();
        RegisterParameter("ordered", Ordered)
            .Default(false);
    }

    virtual void OnLoaded() override
    {
        TUnorderedOperationSpecBase::OnLoaded();

        OutputTablePaths = NYT::NYPath::Normalize(OutputTablePaths);

        Mapper->InitEnableInputTableIndex(InputTablePaths.size(), JobIO);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TUnorderedMergeOperationSpec
    : public TUnorderedOperationSpecBase
{
public:
    NYPath::TRichYPath OutputTablePath;
    bool CombineChunks;
    bool ForceTransform;
    ESchemaInferenceMode SchemaInferenceMode;

    TUnorderedMergeOperationSpec()
    {
        RegisterParameter("output_table_path", OutputTablePath);
        RegisterParameter("combine_chunks", CombineChunks)
            .Default(false);
        RegisterParameter("force_transform", ForceTransform)
            .Default(false);
        RegisterParameter("schema_inference_mode", SchemaInferenceMode)
            .Default(ESchemaInferenceMode::Auto);
    }

    virtual void OnLoaded() override
    {
        TUnorderedOperationSpecBase::OnLoaded();

        OutputTablePath = OutputTablePath.Normalize();
    }
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMergeMode,
    (Sorted)
    (Ordered)
    (Unordered)
);

class TMergeOperationSpec
    : public TSimpleOperationSpecBase
{
public:
    std::vector<NYPath::TRichYPath> InputTablePaths;
    NYPath::TRichYPath OutputTablePath;
    EMergeMode Mode;
    bool CombineChunks;
    bool ForceTransform;
    NTableClient::TKeyColumns MergeBy;

    ESchemaInferenceMode SchemaInferenceMode;

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
        RegisterParameter("schema_inference_mode", SchemaInferenceMode)
            .Default(ESchemaInferenceMode::Auto);
    }

    virtual void OnLoaded() override
    {
        TSimpleOperationSpecBase::OnLoaded();

        InputTablePaths = NYT::NYPath::Normalize(InputTablePaths);
        OutputTablePath = OutputTablePath.Normalize();
    }
};

class TOrderedMergeOperationSpec
    : public TMergeOperationSpec
    , public TInputlyQueryableSpec
{ };

class TSortedMergeOperationSpec
    : public TMergeOperationSpec
{ };

////////////////////////////////////////////////////////////////////////////////

class TEraseOperationSpec
    : public TSimpleOperationSpecBase
{
public:
    NYPath::TRichYPath TablePath;
    bool CombineChunks;
    ESchemaInferenceMode SchemaInferenceMode;

    TEraseOperationSpec()
    {
        RegisterParameter("table_path", TablePath);
        RegisterParameter("combine_chunks", CombineChunks)
            .Default(false);
        RegisterParameter("schema_inference_mode", SchemaInferenceMode)
            .Default(ESchemaInferenceMode::Auto);
    }

    virtual void OnLoaded() override
    {
        TSimpleOperationSpecBase::OnLoaded();

        TablePath = TablePath.Normalize();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReduceOperationSpecBase
    : public TSimpleOperationSpecBase
    , public TOperationWithUserJobSpec
{
public:
    TUserJobSpecPtr Reducer;
    std::vector<NYPath::TRichYPath> InputTablePaths;
    std::vector<NYPath::TRichYPath> OutputTablePaths;
    NTableClient::TKeyColumns JoinBy;

    TReduceOperationSpecBase()
    {
        RegisterParameter("reducer", Reducer)
            .DefaultNew();
        RegisterParameter("input_table_paths", InputTablePaths)
            .NonEmpty();
        RegisterParameter("output_table_paths", OutputTablePaths)
            .NonEmpty();

        RegisterValidator([&] () {
            if (!JoinBy.empty()) {
                NTableClient::ValidateKeyColumns(JoinBy);
            }
        });
    }

    virtual void OnLoaded() override
    {
        TSimpleOperationSpecBase::OnLoaded();

        InputTablePaths = NYT::NYPath::Normalize(InputTablePaths);
        OutputTablePaths = NYT::NYPath::Normalize(OutputTablePaths);

        Reducer->InitEnableInputTableIndex(InputTablePaths.size(), JobIO);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReduceOperationSpec
    : public TReduceOperationSpecBase
{
public:
    NTableClient::TKeyColumns ReduceBy;
    NTableClient::TKeyColumns SortBy;

    TReduceOperationSpec()
    {
        RegisterParameter("join_by", JoinBy)
            .Default();
        RegisterParameter("reduce_by", ReduceBy)
            .NonEmpty();
        RegisterParameter("sort_by", SortBy)
            .Default();

        RegisterValidator([&] () {
            if (!ReduceBy.empty()) {
                NTableClient::ValidateKeyColumns(ReduceBy);
            }

            if (!SortBy.empty()) {
                NTableClient::ValidateKeyColumns(SortBy);
            }
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

class TJoinReduceOperationSpec
    : public TReduceOperationSpecBase
{
public:
    TJoinReduceOperationSpec()
    {
        RegisterParameter("join_by", JoinBy)
            .NonEmpty();
    }

    virtual void OnLoaded() override
    {
        TReduceOperationSpecBase::OnLoaded();
        bool hasPrimary = false;
        for (const auto& path: InputTablePaths) {
            hasPrimary |= path.GetPrimary();
        }
        if (hasPrimary) {
            for (auto& path: InputTablePaths) {
                path.Attributes().Set("foreign", !path.GetPrimary());
                path.Attributes().Remove("primary");
            }
        }
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

    //! The expected ratio of data size after partitioning to data size before partitioning.
    //! For sort operations, this is always 1.0.
    double MapSelectivityFactor;

    double ShuffleStartThreshold;
    double MergeStartThreshold;

    TDuration SimpleSortLocalityTimeout;
    TDuration SimpleMergeLocalityTimeout;

    TDuration PartitionLocalityTimeout;
    TDuration SortLocalityTimeout;
    TDuration SortAssignmentTimeout;
    TDuration MergeLocalityTimeout;

    TJobIOConfigPtr PartitionJobIO;
    // Also works for ReduceCombiner if present.
    TJobIOConfigPtr SortJobIO;
    TJobIOConfigPtr MergeJobIO;

    int ShuffleNetworkLimit;

    std::vector<Stroka> SortBy;

    //! If |true| then the scheduler attempts to distribute partition jobs evenly
    //! (w.r.t. the uncompressed input data size) across the cluster to balance IO
    //! load during the subsequent shuffle stage.
    bool EnablePartitionedDataBalancing;

    //! When #EnablePartitionedDataBalancing is |true| the scheduler tries to maintain the following
    //! invariant regarding (uncompressed) |DataSize(i)| assigned to each node |i|:
    //! |max_i DataSize(i) <= avg_i DataSize(i) + DataSizePerJob * PartitionedDataBalancingTolerance|
    double PartitionedDataBalancingTolerance;

    // For all kinds of sort jobs: simple_sort, intermediate_sort, final_sort.
    TLogDigestConfigPtr SortJobProxyMemoryDigest;
    // For partition and partition_map jobs.
    TLogDigestConfigPtr PartitionJobProxyMemoryDigest;

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
            .Default(0);
        RegisterParameter("sort_by", SortBy)
            .NonEmpty();
        RegisterParameter("enable_partitioned_data_balancing", EnablePartitionedDataBalancing)
            .Default(true);
        RegisterParameter("partitioned_data_balancing_tolerance", PartitionedDataBalancingTolerance)
            .Default(3.0);

        RegisterParameter("sort_job_proxy_memory_digest", SortJobProxyMemoryDigest)
            .Default(New<TLogDigestConfig>(0.5, 1.0, 1.0));
        RegisterParameter("partition_job_proxy_memory_digest", PartitionJobProxyMemoryDigest)
            .Default(New<TLogDigestConfig>(0.5, 2.0, 1.0));

        RegisterValidator([&] () {
            NTableClient::ValidateKeyColumns(SortBy);
        });
    }

    virtual void OnLoaded() override
    {
        TOperationSpecBase::OnLoaded();

        InputTablePaths = NYT::NYPath::Normalize(InputTablePaths);
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

    // For sorted_merge and unordered_merge jobs.
    TLogDigestConfigPtr MergeJobProxyMemoryDigest;

    ESchemaInferenceMode SchemaInferenceMode;

    TSortOperationSpec()
    {
        RegisterParameter("output_table_path", OutputTablePath);
        RegisterParameter("samples_per_partition", SamplesPerPartition)
            .Default(1000)
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

        RegisterParameter("merge_job_proxy_memory_digest", MergeJobProxyMemoryDigest)
            .Default(New<TLogDigestConfig>(0.5, 2.0, 1.0));
        RegisterParameter("schema_inference_mode", SchemaInferenceMode)
            .Default(ESchemaInferenceMode::Auto);

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

        OutputTablePath = OutputTablePath.Normalize();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMapReduceOperationSpec
    : public TSortOperationSpecBase
    , public TInputlyQueryableSpec
    , public TOperationWithUserJobSpec
{
public:
    std::vector<NYPath::TRichYPath> OutputTablePaths;

    std::vector<Stroka> ReduceBy;

    TUserJobSpecPtr Mapper;
    TUserJobSpecPtr ReduceCombiner;
    TUserJobSpecPtr Reducer;

    // For sorted_reduce jobs.
    TLogDigestConfigPtr SortedReduceJobProxyMemoryDigest;
    // For partition_reduce jobs.
    TLogDigestConfigPtr PartitionReduceJobProxyMemoryDigest;
    // For reduce_combiner jobs.
    TLogDigestConfigPtr ReduceCombinerJobProxyMemoryDigest;

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
        RegisterParameter("map_job_io", PartitionJobIO)
            .DefaultNew();
        RegisterParameter("sort_job_io", SortJobIO)
            .DefaultNew();
        RegisterParameter("reduce_job_io", MergeJobIO)
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

        RegisterParameter("sorted_reduce_job_proxy_memory_digest", SortedReduceJobProxyMemoryDigest)
            .Default(New<TLogDigestConfig>(0.5, 2.0, 1.0));
        RegisterParameter("partition_reduce_job_proxy_memory_digest", PartitionReduceJobProxyMemoryDigest)
            .Default(New<TLogDigestConfig>(0.5, 1.0, 1.0));
        RegisterParameter("reduce_combiner_job_proxy_memory_digest", ReduceCombinerJobProxyMemoryDigest)
            .Default(New<TLogDigestConfig>(0.5, 1.0, 1.0));

        // The following settings are inherited from base but make no sense for map-reduce:
        //   SimpleSortLocalityTimeout
        //   SimpleMergeLocalityTimeout
        //   MapSelectivityFactor

        RegisterInitializer([&] () {
            PartitionJobIO->TableReader->MaxBufferSize = (i64) 256 * 1024 * 1024;
            PartitionJobIO->TableWriter->MaxBufferSize = (i64) 2 * 1024 * 1024 * 1024; // 2 GBs

            SortJobIO->TableReader->MaxBufferSize = (i64) 1024 * 1024 * 1024;
        });

        RegisterValidator([&] () {
            auto throwError = [] (NTableClient::EControlAttribute attribute, const Stroka& jobType) {
                THROW_ERROR_EXCEPTION(
                    "%Qlv contol attribute is not supported by %v jobs in map-reduce operation",
                    attribute,
                    jobType);
            };
            auto validateControlAttributes = [&] (const NFormats::TControlAttributesConfigPtr& attributes, const Stroka& jobType) {
                if (attributes->EnableTableIndex) {
                    throwError(NTableClient::EControlAttribute::TableIndex, jobType);
                }
                if (attributes->EnableRowIndex) {
                    throwError(NTableClient::EControlAttribute::RowIndex, jobType);
                }
                if (attributes->EnableRangeIndex) {
                    throwError(NTableClient::EControlAttribute::RangeIndex, jobType);
                }
            };
            validateControlAttributes(MergeJobIO->ControlAttributes, "reduce");
            validateControlAttributes(SortJobIO->ControlAttributes, "reduce_combiner");

            if (!ReduceBy.empty()) {
                NTableClient::ValidateKeyColumns(ReduceBy);
            }
        });
    }

    virtual void OnLoaded() override
    {
        TSortOperationSpecBase::OnLoaded();

        if (ReduceBy.empty()) {
            ReduceBy = SortBy;
        }

        OutputTablePaths = NYT::NYPath::Normalize(OutputTablePaths);

        if (Mapper) {
            Mapper->InitEnableInputTableIndex(InputTablePaths.size(), PartitionJobIO);
        }
        Reducer->InitEnableInputTableIndex(1, MergeJobIO);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRemoteCopyOperationSpec
    : public TSimpleOperationSpecBase
{
public:
    TNullable<Stroka> ClusterName;
    TNullable<Stroka> NetworkName;
    TNullable<NApi::TConnectionConfigPtr> ClusterConnection;
    std::vector<NYPath::TRichYPath> InputTablePaths;
    NYPath::TRichYPath OutputTablePath;
    int MaxChunkCountPerJob;
    bool CopyAttributes;
    TNullable<std::vector<Stroka>> AttributeKeys;

    ESchemaInferenceMode SchemaInferenceMode;

    TRemoteCopyOperationSpec()
    {
        RegisterParameter("cluster_name", ClusterName)
            .Default();
        RegisterParameter("input_table_paths", InputTablePaths)
            .NonEmpty();
        RegisterParameter("output_table_path", OutputTablePath);
        RegisterParameter("network_name", NetworkName)
            .Default();
        RegisterParameter("cluster_connection", ClusterConnection)
            .Default();
        RegisterParameter("max_chunk_count_per_job", MaxChunkCountPerJob)
            .Default(100);
        RegisterParameter("copy_attributes", CopyAttributes)
            .Default(false);
        RegisterParameter("attribute_keys", AttributeKeys)
            .Default();
        RegisterParameter("schema_inference_mode", SchemaInferenceMode)
            .Default(ESchemaInferenceMode::Auto);
    }

    virtual void OnLoaded() override
    {
        TOperationSpecBase::OnLoaded();

        InputTablePaths = NYPath::Normalize(InputTablePaths);
        OutputTablePath = OutputTablePath.Normalize();

        if (!ClusterName && !ClusterConnection) {
            THROW_ERROR_EXCEPTION("Neither cluster name nor cluster connection specified.");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TResourceLimitsConfig
    : public NYTree::TYsonSerializable
{
public:
    TNullable<int> UserSlots;
    TNullable<int> Cpu;
    TNullable<int> Network;
    TNullable<i64> Memory;

    TResourceLimitsConfig()
    {
        RegisterParameter("user_slots", UserSlots)
            .Default()
            .GreaterThanOrEqual(0);
        RegisterParameter("cpu", Cpu)
            .Default()
            .GreaterThanOrEqual(0);
        RegisterParameter("network", Network)
            .Default()
            .GreaterThanOrEqual(0);
        RegisterParameter("memory", Memory)
            .Default()
            .GreaterThanOrEqual(0);
    }
};

class TSchedulableConfig
    : public NYTree::TYsonSerializable
{
public:
    double Weight;

    // Specifies resource limits in terms of a share of all cluster resources.
    double MaxShareRatio;
    // Specifies resource limits in absolute values.
    TResourceLimitsConfigPtr ResourceLimits;

    // Specifies guaranteed resources in terms of a share of all cluster resources.
    double MinShareRatio;
    // Specifies guaranteed resources in absolute values.
    TResourceLimitsConfigPtr MinShareResources;

    TNullable<Stroka> SchedulingTag;

    // The following settings override scheduler configuration.
    TNullable<TDuration> MinSharePreemptionTimeout;
    TNullable<TDuration> FairSharePreemptionTimeout;
    TNullable<double> FairShareStarvationTolerance;

    TNullable<TDuration> MinSharePreemptionTimeoutLimit;
    TNullable<TDuration> FairSharePreemptionTimeoutLimit;
    TNullable<double> FairShareStarvationToleranceLimit;

    TSchedulableConfig()
    {
        RegisterParameter("weight", Weight)
            .Default(1.0)
            .InRange(MinSchedulableWeight, MaxSchedulableWeight);

        RegisterParameter("max_share_ratio", MaxShareRatio)
            .Default(1.0)
            .InRange(0.0, 1.0);
        RegisterParameter("resource_limits", ResourceLimits)
            .DefaultNew();

        RegisterParameter("min_share_ratio", MinShareRatio)
            .Default(0.0)
            .InRange(0.0, 1.0);
        RegisterParameter("min_share_resources", MinShareResources)
            .DefaultNew();

        RegisterParameter("scheduling_tag", SchedulingTag)
            .Default();

        RegisterParameter("min_share_preemption_timeout", MinSharePreemptionTimeout)
            .Default();
        RegisterParameter("fair_share_preemption_timeout", FairSharePreemptionTimeout)
            .Default();
        RegisterParameter("fair_share_starvation_tolerance", FairShareStarvationTolerance)
            .InRange(0.0, 1.0)
            .Default();

        RegisterParameter("min_share_preemption_timeout_limit", MinSharePreemptionTimeoutLimit)
            .Default();
        RegisterParameter("fair_share_preemption_timeout_limit", FairSharePreemptionTimeoutLimit)
            .Default();
        RegisterParameter("fair_share_starvation_tolerance_limit", FairShareStarvationToleranceLimit)
            .InRange(0.0, 1.0)
            .Default();
    }
};

class TPoolConfig
    : public TSchedulableConfig
{
public:
    ESchedulingMode Mode;

    TNullable<int> MaxRunningOperationCount;
    TNullable<int> MaxOperationCount;

    std::vector<EFifoSortParameter> FifoSortParameters;

    bool EnableAggressiveStarvation;

    TPoolConfig()
    {
        RegisterParameter("mode", Mode)
            .Default(ESchedulingMode::FairShare);

        RegisterParameter("max_running_operation_count", MaxRunningOperationCount)
            .Alias("max_running_operations")
            .Default();

        RegisterParameter("max_operation_count", MaxOperationCount)
            .Alias("max_operations")
            .Default();

        RegisterParameter("fifo_sort_parameters", FifoSortParameters)
            .Default({EFifoSortParameter::Weight, EFifoSortParameter::StartTime})
            .NonEmpty();

        RegisterParameter("enable_aggressive_starvation", EnableAggressiveStarvation)
            .Alias("aggressive_starvation_enabled")
            .Default(false);
    }
};

////////////////////////////////////////////////////////////////////

class TStrategyOperationSpec
    : public TSchedulableConfig
{
public:
    TNullable<Stroka> Pool;

    TStrategyOperationSpec()
    {
        RegisterParameter("pool", Pool)
            .Default()
            .NonEmpty();
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
            .InRange(MinSchedulableWeight, MaxSchedulableWeight);
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

DEFINE_REFCOUNTED_TYPE(TSchedulerConnectionConfig)

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
