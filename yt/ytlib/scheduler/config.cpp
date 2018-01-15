#include "config.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

TJobIOConfig::TJobIOConfig()
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

    RegisterPreprocessor([&] () {
        ErrorFileWriter->UploadReplicationFactor = 1;
    });
}

TTestingOperationOptions::TTestingOperationOptions()
{
    RegisterParameter("scheduling_delay", SchedulingDelay)
        .Default(Null);
    RegisterParameter("scheduling_delay_type", SchedulingDelayType)
        .Default(ESchedulingDelayType::Sync);
    RegisterParameter("delay_inside_revive", DelayInsideRevive)
        .Default(Null);
    RegisterParameter("delay_inside_suspend", DelayInsideSuspend)
        .Default(Null);
    RegisterParameter("delay_inside_operation_commit", DelayInsideOperationCommit)
        .Default(Null);
    RegisterParameter("delay_inside_operation_commit_stage", DelayInsideOperationCommitStage)
        .Default(Null);
    RegisterParameter("controller_failure", ControllerFailure)
        .Default(EControllerFailureType::None);
    RegisterParameter("fail_get_job_spec", FailGetJobSpec)
        .Default(false);
    RegisterParameter("cypress_storage_mode", CypressStorageMode)
        .Default(EOperationCypressStorageMode::Compatible);
}

TAutoMergeConfig::TAutoMergeConfig()
{
    RegisterParameter("job_io", JobIO)
        .DefaultNew();
    RegisterParameter("max_intermediate_chunk_count", MaxIntermediateChunkCount)
        .Default(Null)
        .GreaterThanOrEqual(1);
    RegisterParameter("chunk_count_per_merge_job", ChunkCountPerMergeJob)
        .Default(Null)
        .GreaterThanOrEqual(1);
    RegisterParameter("chunk_size_threshold", ChunkSizeThreshold)
        .Default(128_MB)
        .GreaterThanOrEqual(1);
    RegisterParameter("mode", Mode)
        .Default(EAutoMergeMode::Disabled);

    RegisterPostprocessor([&] {
        if (Mode == EAutoMergeMode::Manual) {
            if (!MaxIntermediateChunkCount || !ChunkCountPerMergeJob) {
                THROW_ERROR_EXCEPTION(
                    "Maximum intermediate chunk count and chunk count per merge job "
                    "should both be present when using relaxed mode of auto merge");
            }
            if (*MaxIntermediateChunkCount < *ChunkCountPerMergeJob) {
                THROW_ERROR_EXCEPTION("Maximum intermediate chunk count cannot be less than chunk count per merge job")
                    << TErrorAttribute("max_intermediate_chunk_count", *MaxIntermediateChunkCount)
                    << TErrorAttribute("chunk_count_per_merge_job", *ChunkCountPerMergeJob);
            }
        }
    });
}

TSupportsSchedulingTagsConfig::TSupportsSchedulingTagsConfig()
{
    RegisterParameter("scheduling_tag_filter", SchedulingTagFilter)
        .Alias("scheduling_tag")
        .Default();

    RegisterPostprocessor([&] {
        if (SchedulingTagFilter.Size() > MaxSchedulingTagRuleCount) {
            THROW_ERROR_EXCEPTION("Specifying more than %v tokens in scheduling tag filter is not allowed",
                 MaxSchedulingTagRuleCount);
        }
    });
}

TOperationSpecBase::TOperationSpecBase()
{
    SetUnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);

    RegisterParameter("intermediate_data_account", IntermediateDataAccount)
        .Default("intermediate");
    RegisterParameter("intermediate_compression_codec", IntermediateCompressionCodec)
        .Default(NCompression::ECodec::Lz4);
    RegisterParameter("intermediate_data_replication_factor", IntermediateDataReplicationFactor)
        .Default(1);
    RegisterParameter("intermediate_data_medium", IntermediateDataMediumName)
        .Default(NChunkClient::DefaultStoreMediumName);
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
            .EndList()->AsList());

    RegisterParameter("job_node_account", JobNodeAccount)
        .Default(NSecurityClient::TmpAccountName);

    RegisterParameter("unavailable_chunk_strategy", UnavailableChunkStrategy)
        .Default(EUnavailableChunkAction::Wait);
    RegisterParameter("unavailable_chunk_tactics", UnavailableChunkTactics)
        .Default(EUnavailableChunkAction::Wait);

    RegisterParameter("max_data_weight_per_job", MaxDataWeightPerJob)
        .Alias("max_data_size_per_job")
        .Default(200_GB)
        .GreaterThan(0);

    RegisterParameter("max_failed_job_count", MaxFailedJobCount)
        .Default(10)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(10000);
    RegisterParameter("max_stderr_count", MaxStderrCount)
        .Default(10)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(150);

    RegisterParameter("job_proxy_memory_overcommit_limit", JobProxyMemoryOvercommitLimit)
        .Default()
        .GreaterThanOrEqual(0);

    RegisterParameter("job_proxy_ref_counted_tracker_log_period", JobProxyRefCountedTrackerLogPeriod)
        .Default(TDuration::Seconds(5));

    RegisterParameter("title", Title)
        .Default();

    RegisterParameter("time_limit", TimeLimit)
        .Default();

    RegisterParameter("testing", TestingOperationOptions)
        .DefaultNew();

    RegisterParameter("owners", Owners)
        .Default();

    RegisterParameter("secure_vault", SecureVault)
        .Default();

    RegisterParameter("available_nodes_missing_timeout", AvailableNodesMissingTimeout)
        .Default(TDuration::Hours(1));

    RegisterParameter("suspend_operation_if_account_limit_exceeded", SuspendOperationIfAccountLimitExceeded)
        .Default(false);

    RegisterParameter("nightly_options", NightlyOptions)
        .Default();

    RegisterParameter("min_locality_input_data_weight", MinLocalityInputDataWeight)
        .GreaterThanOrEqual(0)
        .Default(1_GB);

    RegisterParameter("auto_merge", AutoMerge)
        .DefaultNew();

    RegisterParameter("started_by", StartedBy)
        .Default();

    RegisterParameter("job_proxy_memory_digest", JobProxyMemoryDigest)
        .Default(New<TLogDigestConfig>(0.5, 2.0, 1.0));

    RegisterParameter("fail_on_job_restart", FailOnJobRestart)
        .Default(false);

    RegisterPostprocessor([&] () {
        if (UnavailableChunkStrategy == EUnavailableChunkAction::Wait &&
            UnavailableChunkTactics == EUnavailableChunkAction::Skip)
        {
            THROW_ERROR_EXCEPTION("Your tactics conflicts with your strategy, Luke!");
        }
    });

    RegisterPostprocessor([&] () {
        if (SecureVault) {
            for (const auto& name : SecureVault->GetKeys()) {
                ValidateEnvironmentVariableName(name);
            }
        }
    });
}

TUserJobSpec::TUserJobSpec()
{
    RegisterParameter("command", Command)
        .NonEmpty();
    RegisterParameter("task_title", TaskTitle)
        .Default();
    RegisterParameter("file_paths", FilePaths)
        .Default();
    RegisterParameter("layer_paths", LayerPaths)
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
        .Default(1)
        .GreaterThanOrEqual(0);
    RegisterParameter("job_time_limit", JobTimeLimit)
        .Default()
        .GreaterThanOrEqual(TDuration::Seconds(1));
    RegisterParameter("memory_limit", MemoryLimit)
        .Default(512_MB)
        .GreaterThan(0)
        .LessThanOrEqual(1_TB);
    RegisterParameter("user_job_memory_digest_default_value", UserJobMemoryDigestDefaultValue)
        .Alias("memory_reserve_factor")
        .Default(0.5)
        .GreaterThan(0.)
        .LessThanOrEqual(1.);
    RegisterParameter("user_job_memory_digest_lower_bound", UserJobMemoryDigestLowerBound)
        .Default(0.05)
        .GreaterThan(0.)
        .LessThanOrEqual(1.);
    RegisterParameter("include_memory_mapped_files", IncludeMemoryMappedFiles)
        .Default(true);
    RegisterParameter("use_yamr_descriptors", UseYamrDescriptors)
        .Default(false);
    RegisterParameter("check_input_fully_consumed", CheckInputFullyConsumed)
        .Default(false);
    RegisterParameter("max_stderr_size", MaxStderrSize)
        .Default(5_MB)
        .GreaterThan(0)
        .LessThanOrEqual(1_GB);
    RegisterParameter("custom_statistics_count_limit", CustomStatisticsCountLimit)
        .Default(128)
        .GreaterThan(0)
        .LessThanOrEqual(1024);
    RegisterParameter("tmpfs_size", TmpfsSize)
        .Default(Null)
        .GreaterThan(0);
    RegisterParameter("tmpfs_path", TmpfsPath)
        .Default(Null);
    RegisterParameter("disk_space_limit", DiskSpaceLimit)
        .Default(Null)
        .GreaterThanOrEqual(0);
    RegisterParameter("inode_limit", InodeLimit)
        .Default(Null)
        .GreaterThanOrEqual(0);
    RegisterParameter("copy_files", CopyFiles)
        .Default(false);
    RegisterParameter("deterministic", Deterministic)
        .Default(false);

    RegisterPostprocessor([&] () {
        if (TmpfsSize && *TmpfsSize > MemoryLimit) {
            THROW_ERROR_EXCEPTION("Size of tmpfs must be less than or equal to memory limit")
                << TErrorAttribute("tmpfs_size", *TmpfsSize)
                << TErrorAttribute("memory_limit", MemoryLimit);
        }
        // Memory reserve should greater than or equal to tmpfs_size (see YT-5518 for more details).
        if (TmpfsPath) {
            i64 tmpfsSize = TmpfsSize ? *TmpfsSize : MemoryLimit;
            UserJobMemoryDigestDefaultValue = std::min(1.0, std::max(UserJobMemoryDigestDefaultValue, double(tmpfsSize) / MemoryLimit));
            UserJobMemoryDigestLowerBound = std::min(1.0, std::max(UserJobMemoryDigestLowerBound, double(tmpfsSize) / MemoryLimit));
        }
        UserJobMemoryDigestDefaultValue = std::max(UserJobMemoryDigestLowerBound, UserJobMemoryDigestDefaultValue);
    });

    RegisterPostprocessor([&] () {
        for (const auto& pair : Environment) {
            ValidateEnvironmentVariableName(pair.first);
        }
    });
}

void TUserJobSpec::InitEnableInputTableIndex(int inputTableCount, TJobIOConfigPtr jobIOConfig)
{
    if (!EnableInputTableIndex) {
        EnableInputTableIndex = (inputTableCount != 1);
    }

    jobIOConfig->ControlAttributes->EnableTableIndex = *EnableInputTableIndex;
}

TVanillaTaskSpec::TVanillaTaskSpec()
{
    RegisterParameter("job_count", JobCount)
        .GreaterThanOrEqual(1);
    RegisterParameter("job_io", JobIO)
        .DefaultNew();
}

TInputlyQueryableSpec::TInputlyQueryableSpec()
{
    RegisterParameter("input_query", InputQuery)
        .Default();
    RegisterParameter("input_schema", InputSchema)
        .Default();

    RegisterPostprocessor([&] () {
        if (InputSchema && !InputQuery) {
            THROW_ERROR_EXCEPTION("Found \"input_schema\" without \"input_query\" in operation spec");
        }
    });
}

TOperationWithUserJobSpec::TOperationWithUserJobSpec()
{
    RegisterParameter("stderr_table_path", StderrTablePath)
        .Default();
    RegisterParameter("stderr_table_writer_config", StderrTableWriterConfig)
        .DefaultNew();

    RegisterParameter("core_table_path", CoreTablePath)
        .Default();
    RegisterParameter("core_table_writer_config", CoreTableWriterConfig)
        .DefaultNew();

    RegisterParameter("enable_job_splitting", EnableJobSplitting)
        .Default(true);

    RegisterPostprocessor([&] {
        if (StderrTablePath) {
            *StderrTablePath = StderrTablePath->Normalize();
        }

        if (CoreTablePath) {
            *CoreTablePath = CoreTablePath->Normalize();
        }
    });
}

TSimpleOperationSpecBase::TSimpleOperationSpecBase()
{
    RegisterParameter("data_weight_per_job", DataWeightPerJob)
        .Alias("data_size_per_job")
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
}

TUnorderedOperationSpecBase::TUnorderedOperationSpecBase()
{
    RegisterParameter("input_table_paths", InputTablePaths)
        .NonEmpty();

    RegisterPreprocessor([&] () {
        JobIO->TableReader->MaxBufferSize = 256_MB;
    });

    RegisterPostprocessor([&] {
        InputTablePaths = NYT::NYPath::Normalize(InputTablePaths);
    });
}

TMapOperationSpec::TMapOperationSpec()
{
    RegisterParameter("mapper", Mapper)
        .DefaultNew();
    RegisterParameter("output_table_paths", OutputTablePaths)
        .NonEmpty();
    RegisterParameter("ordered", Ordered)
        .Default(false);

    RegisterPostprocessor([&] {
        OutputTablePaths = NYT::NYPath::Normalize(OutputTablePaths);

        Mapper->InitEnableInputTableIndex(InputTablePaths.size(), JobIO);
        Mapper->TaskTitle = "Mapper";
    });
}

TUnorderedMergeOperationSpec::TUnorderedMergeOperationSpec()
{
    RegisterParameter("output_table_path", OutputTablePath);
    RegisterParameter("combine_chunks", CombineChunks)
        .Default(false);
    RegisterParameter("force_transform", ForceTransform)
        .Default(false);
    RegisterParameter("schema_inference_mode", SchemaInferenceMode)
        .Default(ESchemaInferenceMode::Auto);

    RegisterPostprocessor([&] {
        OutputTablePath = OutputTablePath.Normalize();
    });
}

TMergeOperationSpec::TMergeOperationSpec()
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

    RegisterPostprocessor([&] {
        InputTablePaths = NYT::NYPath::Normalize(InputTablePaths);
        OutputTablePath = OutputTablePath.Normalize();
    });
}

TEraseOperationSpec::TEraseOperationSpec()
{
    RegisterParameter("table_path", TablePath);
    RegisterParameter("combine_chunks", CombineChunks)
        .Default(false);
    RegisterParameter("schema_inference_mode", SchemaInferenceMode)
        .Default(ESchemaInferenceMode::Auto);

    RegisterPostprocessor([&] {
        TablePath = TablePath.Normalize();
    });
}

TReduceOperationSpecBase::TReduceOperationSpecBase()
{
    RegisterParameter("reducer", Reducer)
        .DefaultNew();
    RegisterParameter("input_table_paths", InputTablePaths)
        .NonEmpty();
    RegisterParameter("output_table_paths", OutputTablePaths)
        .NonEmpty();
    RegisterParameter("consider_only_primary_size", ConsiderOnlyPrimarySize)
        .Default(false);

    RegisterPostprocessor([&] () {
        if (!JoinBy.empty()) {
            NTableClient::ValidateKeyColumns(JoinBy);
        }

        InputTablePaths = NYT::NYPath::Normalize(InputTablePaths);
        OutputTablePaths = NYT::NYPath::Normalize(OutputTablePaths);

        Reducer->InitEnableInputTableIndex(InputTablePaths.size(), JobIO);
        Reducer->TaskTitle = "Reducer";
    });
}

TReduceOperationSpec::TReduceOperationSpec()
{
    RegisterParameter("join_by", JoinBy)
        .Default();
    RegisterParameter("reduce_by", ReduceBy)
        .NonEmpty();
    RegisterParameter("sort_by", SortBy)
        .Default();
    RegisterParameter("pivot_keys", PivotKeys)
        .Default();

    RegisterPostprocessor([&] () {
        if (!ReduceBy.empty()) {
            NTableClient::ValidateKeyColumns(ReduceBy);
        }

        if (!SortBy.empty()) {
            NTableClient::ValidateKeyColumns(SortBy);
        }
    });
}

TJoinReduceOperationSpec::TJoinReduceOperationSpec()
{
    RegisterParameter("join_by", JoinBy)
        .NonEmpty();

    RegisterPostprocessor([&] {
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
    });
}

TSortOperationSpecBase::TSortOperationSpecBase()
{
    RegisterParameter("input_table_paths", InputTablePaths)
        .NonEmpty();
    RegisterParameter("partition_count", PartitionCount)
        .Default()
        .GreaterThan(0);
    RegisterParameter("partition_data_weight", PartitionDataWeight)
        .Alias("partition_data_size")
        .Default()
        .GreaterThan(0);
    RegisterParameter("data_weight_per_sort_job", DataWeightPerShuffleJob)
        .Alias("data_size_per_sort_job")
        .Default(2_GB)
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
    RegisterParameter("enable_intermediate_output_recalculation", EnableIntermediateOutputRecalculation)
        .Default(true);

    RegisterPostprocessor([&] () {
        NTableClient::ValidateKeyColumns(SortBy);

        InputTablePaths = NYT::NYPath::Normalize(InputTablePaths);
    });
}

TSortOperationSpec::TSortOperationSpec()
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
    RegisterParameter("data_weight_per_partition_job", DataWeightPerPartitionJob)
        .Alias("data_size_per_partition_job")
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

    // For sorted_merge and sorted_reduce jobs.
    TLogDigestConfigPtr SortedMergeJobProxyMemoryDigest;
    // For final_sort and partition_reduce jobs.
    TLogDigestConfigPtr FinalSortJobProxyMemoryDigest;

    RegisterParameter("schema_inference_mode", SchemaInferenceMode)
        .Default(ESchemaInferenceMode::Auto);

    RegisterParameter("data_weight_per_sorted_merge_job", DataWeightPerSortedJob)
        .Alias("data_size_per_sorted_merge_job")
        .Default(Null);

    RegisterPreprocessor([&] () {
        PartitionJobIO->TableReader->MaxBufferSize = 1_GB;
        PartitionJobIO->TableWriter->MaxBufferSize = 2_GB;

        SortJobIO->TableReader->MaxBufferSize = 1_GB;
        SortJobIO->TableReader->RetryCount = 3;

        // Output slices must be small enough to make reasonable jobs in sorted chunk pool.
        SortJobIO->TableWriter->DesiredChunkWeight = 256_MB;
        MergeJobIO->TableReader->RetryCount = 3;

        MapSelectivityFactor = 1.0;
    });

    RegisterPostprocessor([&] {
        OutputTablePath = OutputTablePath.Normalize();
    });
}

TMapReduceOperationSpec::TMapReduceOperationSpec()
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

    RegisterParameter("mapper_output_table_count", MapperOutputTableCount)
        .Default(0)
        .GreaterThanOrEqual(0);

    // Provide custom names for shared settings.
    RegisterParameter("map_job_count", PartitionJobCount)
        .Default()
        .GreaterThan(0);
    RegisterParameter("data_weight_per_map_job", DataWeightPerPartitionJob)
        .Alias("data_size_per_map_job")
        .Default()
        .GreaterThan(0);
    RegisterParameter("map_locality_timeout", PartitionLocalityTimeout)
        .Default(TDuration::Seconds(5));
    RegisterParameter("reduce_locality_timeout", MergeLocalityTimeout)
        .Default(TDuration::Minutes(1));
    RegisterParameter("map_selectivity_factor", MapSelectivityFactor)
        .Default(1.0)
        .GreaterThan(0);

    RegisterParameter("data_weight_per_reduce_job", DataWeightPerSortedJob)
        .Alias("data_size_per_reduce_job")
        .Default(Null);

    RegisterParameter("force_reduce_combiners", ForceReduceCombiners)
        .Default(false);

    // The following settings are inherited from base but make no sense for map-reduce:
    //   SimpleSortLocalityTimeout
    //   SimpleMergeLocalityTimeout
    //   MapSelectivityFactor

    RegisterPreprocessor([&] () {
        PartitionJobIO->TableReader->MaxBufferSize = 256_MB;
        PartitionJobIO->TableWriter->MaxBufferSize = 2_GB;

        SortJobIO->TableReader->MaxBufferSize = 1_GB;
        // Output slices must be small enough to make reasonable jobs in sorted chunk pool.
        SortJobIO->TableWriter->DesiredChunkWeight = 256_MB;

        SortJobIO->TableReader->RetryCount = 3;
        MergeJobIO->TableReader->RetryCount = 3;
    });

    RegisterPostprocessor([&] () {
        auto throwError = [] (NTableClient::EControlAttribute attribute, const TString& jobType) {
            THROW_ERROR_EXCEPTION(
                "%Qlv control attribute is not supported by %Qlv jobs in map-reduce operation",
                attribute,
                jobType);
        };
        auto validateControlAttributes = [&] (const NFormats::TControlAttributesConfigPtr& attributes, const TString& jobType) {
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
        if (ForceReduceCombiners && !ReduceCombiner) {
            THROW_ERROR_EXCEPTION("Found \"force_reduce_combiners\" without \"reduce_combiner\" in operation spec");
        }
        validateControlAttributes(MergeJobIO->ControlAttributes, "reduce");
        validateControlAttributes(SortJobIO->ControlAttributes, "reduce_combiner");

        if (!ReduceBy.empty()) {
            NTableClient::ValidateKeyColumns(ReduceBy);
        }

        if (MapperOutputTableCount >= OutputTablePaths.size()) {
            THROW_ERROR_EXCEPTION(
                "There should be at least one non-mapper output table; maybe you need Map operation instead?")
                << TErrorAttribute("mapper_output_table_count", MapperOutputTableCount)
                << TErrorAttribute("output_table_count", OutputTablePaths.size());
        }

        if (ReduceBy.empty()) {
            ReduceBy = SortBy;
        }

        InputTablePaths = NYT::NYPath::Normalize(InputTablePaths);
        OutputTablePaths = NYT::NYPath::Normalize(OutputTablePaths);

        if (Mapper) {
            Mapper->InitEnableInputTableIndex(InputTablePaths.size(), PartitionJobIO);
            Mapper->TaskTitle = "Mapper";
        }
        if (ReduceCombiner) {
            ReduceCombiner->TaskTitle = "Reduce combiner";
        }
        Reducer->TaskTitle = "Reducer";
        // NB(psushin): don't init input table index for reduce jobs,
        // they cannot have table index.
    });
}

TRemoteCopyOperationSpec::TRemoteCopyOperationSpec()
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
        .Default(1000);
    RegisterParameter("copy_attributes", CopyAttributes)
        .Default(false);
    RegisterParameter("attribute_keys", AttributeKeys)
        .Default();
    RegisterParameter("concurrency", Concurrency)
        .Default(4);
    RegisterParameter("block_buffer_size", BlockBufferSize)
        .Default(64_MB);
    RegisterParameter("schema_inference_mode", SchemaInferenceMode)
        .Default(ESchemaInferenceMode::Auto);

    RegisterPostprocessor([&] {
        InputTablePaths = NYPath::Normalize(InputTablePaths);
        OutputTablePath = OutputTablePath.Normalize();

        if (!ClusterName && !ClusterConnection) {
            THROW_ERROR_EXCEPTION("Neither cluster name nor cluster connection specified.");
        }
    });
}

TVanillaOperationSpec::TVanillaOperationSpec()
{
    RegisterParameter("tasks", Tasks)
        .NonEmpty();

    RegisterPostprocessor([&] {
        for (auto& pair : Tasks) {
            const auto& taskName = pair.first;
            auto& taskSpec = pair.second;
            if (taskName.empty()) {
                THROW_ERROR_EXCEPTION("Empty task names are not allowed");
            }

            taskSpec->TaskTitle = taskName;
        }
    });
}

TResourceLimitsConfig::TResourceLimitsConfig()
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

TSchedulableConfig::TSchedulableConfig()
{
    RegisterParameter("weight", Weight)
        .Default()
        .InRange(MinSchedulableWeight, MaxSchedulableWeight);

    RegisterParameter("max_share_ratio", MaxShareRatio)
        .Default()
        .InRange(0.0, 1.0);
    RegisterParameter("resource_limits", ResourceLimits)
        .DefaultNew();

    RegisterParameter("min_share_ratio", MinShareRatio)
        .Default()
        .InRange(0.0, 1.0);
    RegisterParameter("min_share_resources", MinShareResources)
        .DefaultNew();

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

    RegisterParameter("allow_aggressive_starvation_preemption", AllowAggressiveStarvationPreemption)
        .Default(true);
}

TExtendedSchedulableConfig::TExtendedSchedulableConfig()
{
    RegisterParameter("pool", Pool)
        .Default();
}

TPoolConfig::TPoolConfig()
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

    RegisterParameter("forbid_immediate_operations", ForbidImmediateOperations)
        .Default(false);
}

void TPoolConfig::Validate()
{
    if (MaxOperationCount && MaxRunningOperationCount && *MaxOperationCount < *MaxRunningOperationCount) {
        THROW_ERROR_EXCEPTION("%Qv must be greater that or equal to %Qv, but %v < %v",
            "max_operation_count",
            "max_running_operation_count",
            *MaxOperationCount,
            *MaxRunningOperationCount);
    }
}

TStrategyOperationSpec::TStrategyOperationSpec()
{
    RegisterParameter("pool", Pool)
        .Default();
    RegisterParameter("fair_share_options_per_pool_tree", FairShareOptionsPerPoolTree)
        .Default();
    RegisterParameter("pool_trees", PoolTrees)
        .Default();
}

TOperationStrategyRuntimeParams::TOperationStrategyRuntimeParams()
{
    RegisterParameter("weight", Weight)
        .Default(1.0)
        .InRange(MinSchedulableWeight, MaxSchedulableWeight);
    RegisterParameter("resource_limits", ResourceLimits)
        .DefaultNew();
}

TOperationRuntimeParams::TOperationRuntimeParams()
{
    RegisterParameter("owners", Owners)
        .Default();
}

TSchedulerConnectionConfig::TSchedulerConnectionConfig()
{
    RegisterParameter("rpc_timeout", RpcTimeout)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_DYNAMIC_PHOENIX_TYPE(TEraseOperationSpec);
DEFINE_DYNAMIC_PHOENIX_TYPE(TJoinReduceOperationSpec);
DEFINE_DYNAMIC_PHOENIX_TYPE(TMapOperationSpec);
DEFINE_DYNAMIC_PHOENIX_TYPE(TMapReduceOperationSpec);
DEFINE_DYNAMIC_PHOENIX_TYPE(TMergeOperationSpec);
DEFINE_DYNAMIC_PHOENIX_TYPE(TOperationSpecBase);
DEFINE_DYNAMIC_PHOENIX_TYPE(TOrderedMergeOperationSpec);
DEFINE_DYNAMIC_PHOENIX_TYPE(TReduceOperationSpec);
DEFINE_DYNAMIC_PHOENIX_TYPE(TReduceOperationSpecBase);
DEFINE_DYNAMIC_PHOENIX_TYPE(TRemoteCopyOperationSpec);
DEFINE_DYNAMIC_PHOENIX_TYPE(TSimpleOperationSpecBase);
DEFINE_DYNAMIC_PHOENIX_TYPE(TSortedMergeOperationSpec);
DEFINE_DYNAMIC_PHOENIX_TYPE(TSortOperationSpec);
DEFINE_DYNAMIC_PHOENIX_TYPE(TSortOperationSpecBase);
DEFINE_DYNAMIC_PHOENIX_TYPE(TStrategyOperationSpec);
DEFINE_DYNAMIC_PHOENIX_TYPE(TUnorderedMergeOperationSpec);
DEFINE_DYNAMIC_PHOENIX_TYPE(TUnorderedOperationSpecBase);
DEFINE_DYNAMIC_PHOENIX_TYPE(TVanillaOperationSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
