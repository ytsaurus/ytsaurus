#include "config.h"

#include <yt/ytlib/chunk_client/medium_directory.h>

#include <yt/client/security_client/acl.h>
#include <yt/client/security_client/helpers.h>

#include <yt/client/scheduler/operation_id_or_alias.h>

#include <yt/core/ytree/convert.h>

#include <yt/core/misc/fs.h>

#include <util/string/split.h>
#include <util/folder/path.h>

namespace NYT::NScheduler {

using namespace NYson;
using namespace NYTree;
using namespace NSecurityClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static void ValidateOperationAcl(const TSerializableAccessControlList& acl)
{
    for (const auto& ace : acl.Entries) {
        if (ace.Action != ESecurityAction::Allow && ace.Action != ESecurityAction::Deny) {
            THROW_ERROR_EXCEPTION("Action %Qlv is forbidden to specify in operation ACE",
                ace.Action)
                << TErrorAttribute("ace", ace);
        }
        if (Any(ace.Permissions & ~(EPermission::Read | EPermission::Manage))) {
            THROW_ERROR_EXCEPTION("Only \"read\" and \"manage\" permissions are allowed in operation ACL, got %v",
                ConvertToYsonString(ace.Permissions))
                << TErrorAttribute("ace", ace);
        }
    }
}

void ProcessAclAndOwnersParameters(TSerializableAccessControlList* acl, std::vector<TString>* owners)
{
    if (!acl->Entries.empty() && !owners->empty()) {
        // COMPAT(levysotsky): Priority is given to |acl| currently.
        // An error should be thrown here when all the clusters are updated.
    } else if (!owners->empty()) {
        acl->Entries.emplace_back(
            ESecurityAction::Allow,
            *owners,
            EPermissionSet(EPermission::Read | EPermission::Manage));
        owners->clear();
    }
}

////////////////////////////////////////////////////////////////////////////////

static const int MaxAllowedProfilingTagCount = 200;

TPoolName::TPoolName(TString pool, std::optional<TString> parent)
{
    if (parent) {
        Pool = *parent +  Delimiter + pool;
        ParentPool = std::move(parent);
    } else {
        Pool = std::move(pool);
    }
}

const char TPoolName::Delimiter = '$';

const TString& TPoolName::GetPool() const
{
    return Pool;
}

const std::optional<TString>& TPoolName::GetParentPool() const
{
    return ParentPool;
}

TPoolName TPoolName::FromString(const TString& value)
{
    std::vector<TString> parts;
    StringSplitter(value).Split(Delimiter).AddTo(&parts);
    switch (parts.size()) {
        case 1:
            return TPoolName(value, std::nullopt);
        case 2:
            return TPoolName(parts[1], parts[0]);
        default:
            THROW_ERROR_EXCEPTION("Malformed pool name: delimiter %Qv is found more than once in %Qv",
                TPoolName::Delimiter,
                value);
    }
}

TString TPoolName::ToString() const
{
    return Pool;
}

void Deserialize(TPoolName& value, NYTree::INodePtr node)
{
    value = TPoolName::FromString(node->AsString()->GetValue());
}

void Serialize(const TPoolName& value, NYson::IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value.ToString());
}

////////////////////////////////////////////////////////////////////////////////

TJobIOConfig::TJobIOConfig()
{
    RegisterParameter("table_reader", TableReader)
        .DefaultNew();
    RegisterParameter("table_writer", TableWriter)
        .DefaultNew();
    RegisterParameter("dynamic_table_writer", DynamicTableWriter)
        .DefaultNew();

    RegisterParameter("control_attributes", ControlAttributes)
        .DefaultNew();

    RegisterParameter("error_file_writer", ErrorFileWriter)
        .DefaultNew();

    RegisterParameter("buffer_row_count", BufferRowCount)
        .Default(10 * 1000)
        .GreaterThan(0);

    RegisterParameter("pipe_io_pool_size", PipeIOPoolSize)
        .Default(1)
        .GreaterThan(0);

    RegisterParameter("testing_options", Testing)
        .DefaultNew();

    RegisterPreprocessor([&] () {
        ErrorFileWriter->UploadReplicationFactor = 1;

        DynamicTableWriter->DesiredChunkSize = 256_MB;
        DynamicTableWriter->BlockSize = 256_KB;
    });
}

////////////////////////////////////////////////////////////////////////////////

TTestingOperationOptions::TTestingOperationOptions()
{
    RegisterParameter("scheduling_delay", SchedulingDelay)
        .Default();
    RegisterParameter("scheduling_delay_type", SchedulingDelayType)
        .Default(ESchedulingDelayType::Sync);
    RegisterParameter("delay_inside_revive", DelayInsideRevive)
        .Default();
    RegisterParameter("delay_inside_initialize", DelayInsideInitialize)
        .Default();
    RegisterParameter("delay_inside_prepare", DelayInsidePrepare)
        .Default();
    RegisterParameter("delay_inside_suspend", DelayInsideSuspend)
        .Default();
    RegisterParameter("delay_inside_materialize", DelayInsideMaterialize)
        .Default();
    RegisterParameter("delay_inside_operation_commit", DelayInsideOperationCommit)
        .Default();
    RegisterParameter("delay_after_materialize", DelayAfterMaterialize)
        .Default();
    RegisterParameter("delay_inside_abort", DelayInsideAbort)
        .Default();
    RegisterParameter("delay_inside_register_jobs_from_revived_operation", DelayInsideRegisterJobsFromRevivedOperation)
        .Default();
    RegisterParameter("delay_inside_operation_commit_stage", DelayInsideOperationCommitStage)
        .Default();
    RegisterParameter("no_delay_on_second_entrance_to_commit", NoDelayOnSecondEntranceToCommit)
        .Default(false);
    RegisterParameter("controller_failure", ControllerFailure)
        .Default();
    RegisterParameter("get_job_spec_delay", GetJobSpecDelay)
        .Default();
    RegisterParameter("fail_get_job_spec", FailGetJobSpec)
        .Default(false);
    RegisterParameter("testing_speculative_launch_mode", TestingSpeculativeLaunchMode)
        .Default(ETestingSpeculativeLaunchMode::None);
    RegisterParameter("allocation_size", AllocationSize)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(1_GB)
        .Default();
    RegisterParameter("cancellation_stage", CancelationStage)
        .Default();
    RegisterParameter("log_residual_custom_job_metrics_on_termination", LogResidualCustomJobMetricsOnTermination)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

TAutoMergeConfig::TAutoMergeConfig()
{
    RegisterParameter("job_io", JobIO)
        .DefaultNew();
    RegisterParameter("max_intermediate_chunk_count", MaxIntermediateChunkCount)
        .Default()
        .GreaterThanOrEqual(1);
    RegisterParameter("chunk_count_per_merge_job", ChunkCountPerMergeJob)
        .Default()
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

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

TTentativeTreeEligibilityConfig::TTentativeTreeEligibilityConfig()
{
    RegisterParameter("sample_job_count", SampleJobCount)
        .Default(10)
        .GreaterThan(0);

    RegisterParameter("max_tentative_job_duration_ratio", MaxTentativeJobDurationRatio)
        .Default(10.0)
        .GreaterThan(0.0);

    RegisterParameter("min_job_duration", MinJobDuration)
        .Default(TDuration::Seconds(30));

    RegisterParameter("ignore_missing_pool_trees", IgnoreMissingPoolTrees)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

TSamplingConfig::TSamplingConfig()
{
    RegisterParameter("sampling_rate", SamplingRate)
        .Default();

    RegisterParameter("max_total_slice_count", MaxTotalSliceCount)
        .Default();

    RegisterParameter("io_block_size", IOBlockSize)
        .Default(16_MB);

    RegisterPostprocessor([&] {
        if (SamplingRate && (*SamplingRate < 0.0 || *SamplingRate > 1.0)) {
            THROW_ERROR_EXCEPTION("Sampling rate should be in range [0.0, 1.0]")
                << TErrorAttribute("sampling_rate", SamplingRate);
        }
        if (MaxTotalSliceCount && *MaxTotalSliceCount <= 0) {
            THROW_ERROR_EXCEPTION("max_total_slice_count should be positive")
                << TErrorAttribute("max_total_slice_count", MaxTotalSliceCount);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TTmpfsVolumeConfig::TTmpfsVolumeConfig()
{
    RegisterParameter("size", Size);
    RegisterParameter("path", Path);
}

void ToProto(NScheduler::NProto::TTmpfsVolume* protoTmpfsVolume, const TTmpfsVolumeConfig& tmpfsVolumeConfig)
{
    protoTmpfsVolume->set_size(tmpfsVolumeConfig.Size);
    protoTmpfsVolume->set_path(tmpfsVolumeConfig.Path);
}

void FromProto(TTmpfsVolumeConfig* tmpfsVolumeConfig, const NScheduler::NProto::TTmpfsVolume& protoTmpfsVolume)
{
    tmpfsVolumeConfig->Size = protoTmpfsVolume.size();
    tmpfsVolumeConfig->Path = protoTmpfsVolume.path();
}

////////////////////////////////////////////////////////////////////////////////

TDiskRequestConfig::TDiskRequestConfig()
{
    RegisterParameter("disk_space", DiskSpace);
    RegisterParameter("inode_count", InodeCount)
        .Default();
    RegisterParameter("medium_name", MediumName)
        .Default(NChunkClient::DefaultSlotsMediumName);
}

void ToProto(
    NProto::TDiskRequest* protoDiskRequest,
    const TDiskRequestConfig& diskRequestConfig)
{
    YT_VERIFY(diskRequestConfig.MediumIndex);
    protoDiskRequest->set_disk_space(diskRequestConfig.DiskSpace);
    if (diskRequestConfig.InodeCount) {
        protoDiskRequest->set_inode_count(*diskRequestConfig.InodeCount);
    }
    protoDiskRequest->set_medium_index(*diskRequestConfig.MediumIndex);
}

////////////////////////////////////////////////////////////////////////////////

TOperationSpecBase::TOperationSpecBase()
{
    SetUnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);

    RegisterParameter("intermediate_data_account", IntermediateDataAccount)
        .Default("intermediate");
    RegisterParameter("intermediate_compression_codec", IntermediateCompressionCodec)
        .Default(NCompression::ECodec::Lz4);
    RegisterParameter("intermediate_data_replication_factor", IntermediateDataReplicationFactor)
        .Default(2);
    RegisterParameter("intermediate_data_medium", IntermediateDataMediumName)
        .Default(NChunkClient::DefaultStoreMediumName);

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
    RegisterParameter("max_primary_data_weight_per_job", MaxPrimaryDataWeightPerJob)
        .Default(std::numeric_limits<i64>::max())
        .GreaterThan(0);

    RegisterParameter("max_failed_job_count", MaxFailedJobCount)
        .Default(10)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(10000);
    RegisterParameter("max_stderr_count", MaxStderrCount)
        .Default(10)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(150);
    RegisterParameter("max_core_info_count", MaxCoreInfoCount)
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

    RegisterParameter("time_limit_job_fail_timeout", TimeLimitJobFailTimeout)
        .Default(TDuration::Minutes(2));

    RegisterParameter("testing", TestingOperationOptions)
        .DefaultNew();

    RegisterParameter("owners", Owners)
        .Default();

    RegisterParameter("acl", AclNode)
        .Default();

    RegisterParameter("add_authenticated_user_to_acl", AddAuthenticatedUserToAcl)
        .Default(true);

    RegisterParameter("secure_vault", SecureVault)
        .Default();

    RegisterParameter("enable_secure_vault_variables_in_job_shell", EnableSecureVaultVariablesInJobShell)
        .Default(true);

    RegisterParameter("suspend_operation_if_account_limit_exceeded", SuspendOperationIfAccountLimitExceeded)
        .Default(false);

    RegisterParameter("suspend_operation_after_materialization", SuspendOperationAfterMaterialization)
        .Default(false);

    RegisterParameter("nightly_options", NightlyOptions)
        .Default();

    RegisterParameter("min_locality_input_data_weight", MinLocalityInputDataWeight)
        .GreaterThanOrEqual(0)
        .Default(1_GB);

    RegisterParameter("auto_merge", AutoMerge)
        .DefaultNew();

    RegisterParameter("job_proxy_memory_digest", JobProxyMemoryDigest)
        .DefaultNew(0.5, 2.0, 1.0);

    RegisterParameter("fail_on_job_restart", FailOnJobRestart)
        .Default(false);

    RegisterParameter("enable_job_splitting", EnableJobSplitting)
        .Default(true);

    RegisterParameter("slice_erasure_chunks_by_parts", SliceErasureChunksByParts)
        .Default(false);

    RegisterParameter("enable_compatible_storage_mode", EnableCompatibleStorageMode)
        .Default(false);

    RegisterParameter("enable_legacy_live_preview", EnableLegacyLivePreview)
        .Default();

    RegisterParameter("started_by", StartedBy)
        .Default();
    RegisterParameter("annotations", Annotations)
        .Default();

    // COMPAT(gritukan): Drop it in favor of `Annotations["description"]'.
    RegisterParameter("description", Description)
        .Default();

    RegisterParameter("use_columnar_statistics", UseColumnarStatistics)
        .Default(false);

    RegisterParameter("ban_nodes_with_failed_jobs", BanNodesWithFailedJobs)
        .Default(false);
    RegisterParameter("ignore_job_failures_at_banned_nodes", IgnoreJobFailuresAtBannedNodes)
        .Default(false);
    RegisterParameter("fail_on_all_nodes_banned", FailOnAllNodesBanned)
        .Default(true);

    RegisterParameter("sampling", Sampling)
        .DefaultNew();

    RegisterParameter("alias", Alias)
        .Default();

    RegisterParameter("omit_inaccessible_columns", OmitInaccessibleColumns)
        .Default(false);

    RegisterParameter("additional_security_tags", AdditionalSecurityTags)
        .Default();

    RegisterParameter("waiting_job_timeout", WaitingJobTimeout)
        .Default(std::nullopt)
        .GreaterThanOrEqual(TDuration::Seconds(10))
        .LessThanOrEqual(TDuration::Minutes(10));

    RegisterParameter("job_speculation_timeout", JobSpeculationTimeout)
        .Default()
        .GreaterThan(TDuration::Zero());

    RegisterParameter("atomicity", Atomicity)
        .Default(EAtomicity::Full);

    RegisterParameter("job_cpu_monitor", JobCpuMonitor)
        .DefaultNew();

    RegisterPostprocessor([&] () {
        if (UnavailableChunkStrategy == EUnavailableChunkAction::Wait &&
            UnavailableChunkTactics == EUnavailableChunkAction::Skip)
        {
            THROW_ERROR_EXCEPTION("Your tactics conflicts with your strategy, Luke!");
        }

        if (SecureVault) {
            for (const auto& name : SecureVault->GetKeys()) {
                ValidateEnvironmentVariableName(name);
            }
        }

        if (Alias && !Alias->StartsWith(OperationAliasPrefix)) {
            THROW_ERROR_EXCEPTION("Operation alias should start with %Qv", OperationAliasPrefix)
                << TErrorAttribute("operation_alias", Alias);
        }

        constexpr int MaxAnnotationsYsonTextLength = 10_KB;
        if (ConvertToYsonString(Annotations, EYsonFormat::Text).GetData().size() > MaxAnnotationsYsonTextLength) {
            THROW_ERROR_EXCEPTION("Length of annotations YSON text representation should not exceed %v",
                MaxAnnotationsYsonTextLength);
        }

        if (AclNode) {
            try {
                Acl = ConvertTo<NSecurityClient::TSerializableAccessControlList>(AclNode);
            } catch (const TErrorException& error) {
                // Intentionally do nothing for backward compatibility.
            }
        }

        ValidateOperationAcl(Acl);
        ProcessAclAndOwnersParameters(&Acl, &Owners);
        ValidateSecurityTags(AdditionalSecurityTags);
    });
}

////////////////////////////////////////////////////////////////////////////////

TUserJobSpec::TUserJobSpec()
{
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
    RegisterParameter("gpu_limit", GpuLimit)
        .Default(0)
        .GreaterThanOrEqual(0);
    RegisterParameter("port_count", PortCount)
        .Default(0)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(50);
    RegisterParameter("job_time_limit", JobTimeLimit)
        .Alias("exec_time_limit")
        .Default()
        .GreaterThanOrEqual(TDuration::Seconds(1));
    RegisterParameter("prepare_time_limit", PrepareTimeLimit)
        .Default(TDuration::Minutes(45))
        .GreaterThanOrEqual(TDuration::Minutes(1));
    RegisterParameter("memory_limit", MemoryLimit)
        .Default(512_MB)
        .GreaterThan(0)
        .LessThanOrEqual(1_TB);
    RegisterParameter("memory_reserve_factor", MemoryReserveFactor)
        .Default();
    RegisterParameter("user_job_memory_digest_default_value", UserJobMemoryDigestDefaultValue)
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
    RegisterParameter("enable_profiling", EnableProfiling)
        .Default(false);
    RegisterParameter("max_profile_size", MaxProfileSize)
        .Default(2_MB)
        .GreaterThan(0)
        .LessThanOrEqual(2_MB);
    RegisterParameter("custom_statistics_count_limit", CustomStatisticsCountLimit)
        .Default(128)
        .GreaterThan(0)
        .LessThanOrEqual(1024);
    RegisterParameter("tmpfs_size", TmpfsSize)
        .Default()
        .GreaterThan(0);
    RegisterParameter("tmpfs_path", TmpfsPath)
        .Default();
    RegisterParameter("tmpfs_volumes", TmpfsVolumes)
        .Default();
    RegisterParameter("disk_space_limit", DiskSpaceLimit)
        .Default()
        .GreaterThanOrEqual(0);
    RegisterParameter("inode_limit", InodeLimit)
        .Default()
        .GreaterThanOrEqual(0);
    RegisterParameter("disk_request", DiskRequest)
        .Default();
    RegisterParameter("copy_files", CopyFiles)
        .Default(false);
    RegisterParameter("deterministic", Deterministic)
        .Default(false);
    RegisterParameter("use_porto_memory_tracking", UsePortoMemoryTracking)
        .Default(false);
    RegisterParameter("set_container_cpu_limit", SetContainerCpuLimit)
        .Default(false);
    RegisterParameter("force_core_dump", ForceCoreDump)
        .Default(false);
    RegisterParameter("interruption_signal", InterruptionSignal)
        .Default();
    RegisterParameter("enable_setup_commands", EnableSetupCommands)
        .Default(true);
    RegisterParameter("enable_gpu_layers", EnableGpuLayers)
        .Default(true);
    RegisterParameter("cuda_toolkit_version", CudaToolkitVersion)
        .Default();
    RegisterParameter("job_speculation_timeout", JobSpeculationTimeout)
        .Default()
        .GreaterThan(TDuration::Zero());
    RegisterParameter("network_project", NetworkProject)
        .Default();
    RegisterParameter("enable_porto", EnablePorto)
        .Default();
    RegisterParameter("fail_job_on_core_dump", FailJobOnCoreDump)
        .Default(true);

    RegisterPostprocessor([&] () {
        if ((TmpfsSize || TmpfsPath) && !TmpfsVolumes.empty()) {
            THROW_ERROR_EXCEPTION(
                "Options \"tmpfs_size\" and \"tmpfs_path\" cannot be specified "
                "simultaneously with \"tmpfs_volumes\"")
                << TErrorAttribute("tmpfs_size", TmpfsSize)
                << TErrorAttribute("tmpfs_path", TmpfsPath)
                << TErrorAttribute("tmpfs_volumes", TmpfsVolumes);
        }

        if (TmpfsPath) {
            auto volume = New<TTmpfsVolumeConfig>();
            volume->Size = TmpfsSize ? *TmpfsSize : MemoryLimit;
            volume->Path = *TmpfsPath;
            TmpfsVolumes.push_back(volume);
            TmpfsPath = std::nullopt;
            TmpfsSize = std::nullopt;
        }

        i64 totalTmpfsSize = 0;
        for (const auto& volume : TmpfsVolumes) {
            if (!NFS::IsPathRelativeAndInvolvesNoTraversal(volume->Path)) {
                THROW_ERROR_EXCEPTION("Tmpfs path %v does not point inside the sandbox directory",
                    volume->Path);
            }
            totalTmpfsSize += volume->Size;
        }

        // Memory reserve should greater than or equal to tmpfs_size (see YT-5518 for more details).
        if (totalTmpfsSize > MemoryLimit) {
            THROW_ERROR_EXCEPTION("Total size of tmpfs volumes must be less than or equal to memory limit")
                << TErrorAttribute("tmpfs_size", totalTmpfsSize)
                << TErrorAttribute("memory_limit", MemoryLimit);
        }

        for (int i = 0; i < TmpfsVolumes.size(); ++i) {
            for (int j = 0; j < TmpfsVolumes.size(); ++j) {
                if (i == j) {
                    continue;
                }

                auto lhsFsPath = TFsPath(TmpfsVolumes[i]->Path);
                auto rhsFsPath = TFsPath(TmpfsVolumes[j]->Path);
                if (lhsFsPath.IsSubpathOf(rhsFsPath)) {
                    THROW_ERROR_EXCEPTION("Path of tmpfs volume %Qv is prefix of other tmpfs volume %Qv",
                        TmpfsVolumes[i]->Path,
                        TmpfsVolumes[j]->Path);
                }
            }
        }

        if (MemoryReserveFactor) {
            UserJobMemoryDigestLowerBound = UserJobMemoryDigestDefaultValue = *MemoryReserveFactor;
        }

        auto memoryDigestLowerLimit = static_cast<double>(totalTmpfsSize) / MemoryLimit;
        UserJobMemoryDigestDefaultValue = std::min(
            1.0,
            std::max(UserJobMemoryDigestDefaultValue, memoryDigestLowerLimit)
        );
        UserJobMemoryDigestLowerBound = std::min(
            1.0,
            std::max(UserJobMemoryDigestLowerBound, memoryDigestLowerLimit)
        );
        UserJobMemoryDigestDefaultValue = std::max(UserJobMemoryDigestLowerBound, UserJobMemoryDigestDefaultValue);

        for (const auto& [variableName, _] : Environment) {
            ValidateEnvironmentVariableName(variableName);
        }

        for (auto& path : FilePaths) {
            path = path.Normalize();
        }

        if (!DiskSpaceLimit && InodeLimit) {
            THROW_ERROR_EXCEPTION("Option \"inode_limit\" can be specified only with \"disk_space_limit\"");
        }

        if (DiskSpaceLimit && DiskRequest) {
            THROW_ERROR_EXCEPTION(
                "Options \"disk_space_limit\" and \"inode_limit\" cannot be specified "
                "together with \"disk_request\"")
                << TErrorAttribute("disk_space_limit", DiskSpaceLimit)
                << TErrorAttribute("inode_limit", InodeLimit)
                << TErrorAttribute("disk_request", DiskRequest);
        }

        if (DiskSpaceLimit) {
            DiskRequest = New<TDiskRequestConfig>();
            DiskRequest->DiskSpace = *DiskSpaceLimit;
            DiskRequest->InodeCount = InodeLimit;
            DiskSpaceLimit = std::nullopt;
            InodeLimit = std::nullopt;
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

////////////////////////////////////////////////////////////////////////////////

TOptionalUserJobSpec::TOptionalUserJobSpec()
{
    RegisterParameter("command", Command)
        .Default();
}

bool TOptionalUserJobSpec::IsNontrivial() const
{
    return Command != TString();
}

////////////////////////////////////////////////////////////////////////////////

TMandatoryUserJobSpec::TMandatoryUserJobSpec()
{
    RegisterParameter("command", Command);
}

////////////////////////////////////////////////////////////////////////////////

TVanillaTaskSpec::TVanillaTaskSpec()
{
    RegisterParameter("job_count", JobCount)
        .GreaterThanOrEqual(1);
    RegisterParameter("job_io", JobIO)
        .DefaultNew();
    RegisterParameter("output_table_paths", OutputTablePaths)
        .Default();
    RegisterParameter("restart_completed_jobs", RestartCompletedJobs)
        .Default(false);

    RegisterPostprocessor([&] {
        OutputTablePaths = NYT::NYPath::Normalize(OutputTablePaths);
    });
}

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

TOperationWithUserJobSpec::TOperationWithUserJobSpec()
{
    RegisterParameter("stderr_table_path", StderrTablePath)
        .Default();
    RegisterParameter("stderr_table_writer", StderrTableWriter)
        // TODO(babenko): deprecate this
        .Alias("stderr_table_writer_config")
        .DefaultNew();

    RegisterParameter("core_table_path", CoreTablePath)
        .Default();
    RegisterParameter("core_table_writer", CoreTableWriter)
        // TODO(babenko): deprecate this
        .Alias("core_table_writer_config")
        .DefaultNew();

    RegisterParameter("enable_cuda_gpu_core_dump", EnableCudaGpuCoreDump)
        .Default(false);

    RegisterPostprocessor([&] {
        if (StderrTablePath) {
            *StderrTablePath = StderrTablePath->Normalize();
        }

        if (CoreTablePath) {
            *CoreTablePath = CoreTablePath->Normalize();
        }

        if (EnableCudaGpuCoreDump && !CoreTablePath) {
            THROW_ERROR_EXCEPTION("\"enable_cuda_gpu_core_dump\" option requires \"core_table_path\" options to be set");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

TMapOperationSpec::TMapOperationSpec()
{
    RegisterParameter("mapper", Mapper)
        .DefaultNew();
    RegisterParameter("output_table_paths", OutputTablePaths)
        .Default();
    RegisterParameter("ordered", Ordered)
        .Default(false);

    RegisterPostprocessor([&] {
        OutputTablePaths = NYT::NYPath::Normalize(OutputTablePaths);

        Mapper->InitEnableInputTableIndex(InputTablePaths.size(), JobIO);
        Mapper->TaskTitle = "Mapper";
    });
}

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

TReduceOperationSpecBase::TReduceOperationSpecBase()
{
    RegisterParameter("reducer", Reducer)
        .DefaultNew();
    RegisterParameter("input_table_paths", InputTablePaths)
        .NonEmpty();
    RegisterParameter("output_table_paths", OutputTablePaths)
        .Default();
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

////////////////////////////////////////////////////////////////////////////////

TNewReduceOperationSpec::TNewReduceOperationSpec()
{
    RegisterParameter("join_by", JoinBy)
        .Default();
    RegisterParameter("reduce_by", ReduceBy)
        .Default();
    RegisterParameter("sort_by", SortBy)
        .Default();
    RegisterParameter("pivot_keys", PivotKeys)
        .Default();
    RegisterParameter("enable_key_guarantee", EnableKeyGuarantee)
        .Default();
    RegisterParameter("validate_key_column_types", ValidateKeyColumnTypes)
        .Default(true);

    RegisterPostprocessor([&] () {
        NTableClient::ValidateKeyColumns(ReduceBy);
        NTableClient::ValidateKeyColumns(SortBy);

        bool hasPrimary = false;
        for (const auto& path : InputTablePaths) {
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

////////////////////////////////////////////////////////////////////////////////

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
    RegisterParameter("max_chunk_slice_per_shuffle_job", MaxChunkSlicePerShuffleJob)
        .Default(8000)
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
    RegisterParameter("max_shuffle_data_slice_count", MaxShuffleDataSliceCount)
        .GreaterThan(0)
        .Default(10'000'000);
    RegisterParameter("max_shuffle_job_count", MaxShuffleJobCount)
        .GreaterThan(0)
        .Default(200'000);
    RegisterParameter("max_merge_data_slice_count", MaxMergeDataSliceCount)
        .GreaterThan(0)
        .Default(10'000'000);
    RegisterParameter("sort_by", SortBy)
        .NonEmpty();
    RegisterParameter("enable_partitioned_data_balancing", EnablePartitionedDataBalancing)
        .Default(true);
    RegisterParameter("enable_intermediate_output_recalculation", EnableIntermediateOutputRecalculation)
        .Default(true);
    RegisterParameter("pivot_keys", PivotKeys)
        .Default();

    RegisterPostprocessor([&] () {
        NTableClient::ValidateKeyColumns(SortBy);

        InputTablePaths = NYT::NYPath::Normalize(InputTablePaths);

        // Validate pivot_keys.
        for (int index = 1; index < PivotKeys.size(); ++index) {
            if (PivotKeys[index] < PivotKeys[index - 1]) {
                THROW_ERROR_EXCEPTION("Pivot keys should be sorted")
                    << TErrorAttribute("previous_key", PivotKeys[index - 1])
                    << TErrorAttribute("current_key", PivotKeys[index]);
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

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

    RegisterParameter("max_input_data_weight", MaxInputDataWeight)
        .GreaterThan(0)
        .Default(500_TB);

    RegisterParameter("schema_inference_mode", SchemaInferenceMode)
        .Default(ESchemaInferenceMode::Auto);

    RegisterParameter("data_weight_per_sorted_merge_job", DataWeightPerSortedJob)
        .Alias("data_size_per_sorted_merge_job")
        .Default();

    RegisterPreprocessor([&] () {
        PartitionJobIO->TableReader->MaxBufferSize = 1_GB;
        PartitionJobIO->TableWriter->MaxBufferSize = 2_GB;

        SortJobIO->TableReader->MaxBufferSize = 1_GB;
        SortJobIO->TableReader->RetryCount = 3;
        SortJobIO->TableReader->PassCount = 50;

        // Output slices must be small enough to make reasonable jobs in sorted chunk pool.
        SortJobIO->TableWriter->DesiredChunkWeight = 256_MB;
        MergeJobIO->TableReader->RetryCount = 3;
        MergeJobIO->TableReader->PassCount = 50;

        MapSelectivityFactor = 1.0;
    });

    RegisterPostprocessor([&] {
        OutputTablePath = OutputTablePath.Normalize();

        if (Sampling && Sampling->SamplingRate) {
            THROW_ERROR_EXCEPTION("Sampling in sort operation is not supported");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TMapReduceOperationSpec::TMapReduceOperationSpec()
{
    RegisterParameter("output_table_paths", OutputTablePaths)
        .NonEmpty();
    RegisterParameter("reduce_by", ReduceBy)
        .Default();
    // Mapper can be absent -- leave it null by default.
    RegisterParameter("mapper", Mapper)
        .Default();
    // ReduceCombiner can be absent -- leave it null by default.
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
        .Default();

    RegisterParameter("force_reduce_combiners", ForceReduceCombiners)
        .Default(false);

    RegisterParameter("ordered", Ordered)
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
        SortJobIO->TableReader->PassCount = 50;

        MergeJobIO->TableReader->RetryCount = 3;
        MergeJobIO->TableReader->PassCount = 50 ;
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
            if (attributes->EnableTabletIndex) {
                throwError(NTableClient::EControlAttribute::TabletIndex, jobType);
            }
        };
        if (ForceReduceCombiners && !HasNontrivialReduceCombiner()) {
            THROW_ERROR_EXCEPTION("Found \"force_reduce_combiners\" without nontrivial \"reduce_combiner\" in operation spec");
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

        if (HasNontrivialMapper()) {
            Mapper->InitEnableInputTableIndex(InputTablePaths.size(), PartitionJobIO);
            Mapper->TaskTitle = "Mapper";
        }
        if (HasNontrivialReduceCombiner()) {
            ReduceCombiner->TaskTitle = "Reduce combiner";
        }
        Reducer->TaskTitle = "Reducer";
        // NB(psushin): don't init input table index for reduce jobs,
        // they cannot have table index.

        if (Sampling && Sampling->SamplingRate) {
            MapSelectivityFactor *= *Sampling->SamplingRate;
        }
    });
}

bool TMapReduceOperationSpec::HasNontrivialMapper() const
{
    return Mapper && Mapper->IsNontrivial();
}

bool TMapReduceOperationSpec::HasNontrivialReduceCombiner() const
{
    return ReduceCombiner && ReduceCombiner->IsNontrivial();
}

////////////////////////////////////////////////////////////////////////////////

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

    RegisterPreprocessor([&] {
        // NB: in remote copy operation chunks are never decompressed,
        // so the data weight does not affect anything.
        MaxDataWeightPerJob = std::numeric_limits<i64>::max();
    });
    RegisterPostprocessor([&] {
        InputTablePaths = NYPath::Normalize(InputTablePaths);
        OutputTablePath = OutputTablePath.Normalize();

        if (!ClusterName && !ClusterConnection) {
            THROW_ERROR_EXCEPTION("Neither cluster name nor cluster connection specified.");
        }

        if (Sampling && Sampling->SamplingRate) {
            THROW_ERROR_EXCEPTION("You do not want sampling in remote copy operation :)");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TVanillaOperationSpec::TVanillaOperationSpec()
{
    RegisterParameter("tasks", Tasks)
        .NonEmpty();

    RegisterPostprocessor([&] {
        for (const auto& [taskName, taskSpec] : Tasks) {
            if (taskName.empty()) {
                THROW_ERROR_EXCEPTION("Empty task names are not allowed");
            }

            taskSpec->TaskTitle = taskName;
        }

        if (Sampling && Sampling->SamplingRate) {
            THROW_ERROR_EXCEPTION("You do not want sampling in vanilla operation :)");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

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
    RegisterParameter("gpu", Gpu)
        .Default()
        .GreaterThanOrEqual(0);
}

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

TExtendedSchedulableConfig::TExtendedSchedulableConfig()
{
    RegisterParameter("pool", Pool)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TEphemeralSubpoolConfig::TEphemeralSubpoolConfig()
{
    RegisterParameter("mode", Mode)
        .Default(ESchedulingMode::FairShare);

    RegisterParameter("max_running_operation_count", MaxRunningOperationCount)
        .Default()
        .GreaterThanOrEqual(0);

    RegisterParameter("max_operation_count", MaxOperationCount)
        .Default()
        .GreaterThanOrEqual(0);
}

////////////////////////////////////////////////////////////////////////////////

TAbcConfig::TAbcConfig()
{
    RegisterParameter("id", Id)
        .GreaterThan(0);
    RegisterParameter("name", Name)
        .NonEmpty();
    RegisterParameter("slug", Slug)
        .NonEmpty();
}

////////////////////////////////////////////////////////////////////////////////

TPoolConfig::TPoolConfig()
{
    RegisterParameter("mode", Mode)
        .Default(ESchedulingMode::FairShare);

    RegisterParameter("max_running_operation_count", MaxRunningOperationCount)
        .Default()
        .GreaterThanOrEqual(0);

    RegisterParameter("max_operation_count", MaxOperationCount)
        .Default()
        .GreaterThanOrEqual(0);

    RegisterParameter("fifo_sort_parameters", FifoSortParameters)
        .Default({EFifoSortParameter::Weight, EFifoSortParameter::StartTime})
        .NonEmpty();

    RegisterParameter("enable_aggressive_starvation", EnableAggressiveStarvation)
        .Alias("aggressive_starvation_enabled")
        .Default(false);

    RegisterParameter("forbid_immediate_operations", ForbidImmediateOperations)
        .Default(false);

    RegisterParameter("create_ephemeral_subpools", CreateEphemeralSubpools)
        .Default(false);

    RegisterParameter("ephemeral_subpool_config", EphemeralSubpoolConfig)
        .DefaultNew();

    RegisterParameter("infer_children_weights_from_historic_usage", InferChildrenWeightsFromHistoricUsage)
        .Default(false);
    RegisterParameter("historic_usage_config", HistoricUsageConfig)
        .DefaultNew();

    RegisterParameter("allowed_profiling_tags", AllowedProfilingTags)
        .Default();

    RegisterParameter("enable_by_user_profiling", EnableByUserProfiling)
        .Default();

    RegisterParameter("abc", Abc)
        .Default();
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
    if (AllowedProfilingTags.size() > MaxAllowedProfilingTagCount) {
        THROW_ERROR_EXCEPTION("Limit for the number of allowed profiling tags exceeded")
            << TErrorAttribute("allowed_profiling_tag_count", AllowedProfilingTags.size())
            << TErrorAttribute("max_allowed_profiling_tag_count", MaxAllowedProfilingTagCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

TFairShareStrategyPackingConfig::TFairShareStrategyPackingConfig()
{
    RegisterParameter("enable", Enable)
        .Default(false);

    RegisterParameter("metric", Metric)
        .Default(EPackingMetricType::AngleLength);

    RegisterParameter("max_better_past_snapshots", MaxBetterPastSnapshots)
        .Default(2);
    RegisterParameter("absolute_metric_value_tolerance", AbsoluteMetricValueTolerance)
        .Default(0.05)
        .GreaterThanOrEqual(0.0);
    RegisterParameter("relative_metric_value_tolerance", RelativeMetricValueTolerance)
        .Default(1.5)
        .GreaterThanOrEqual(1.0);
    RegisterParameter("min_window_size_for_schedule", MinWindowSizeForSchedule)
        .Default(0)
        .GreaterThanOrEqual(0);
    RegisterParameter("max_heartbeat_window_size", MaxHearbeatWindowSize)
        .Default(10);
    RegisterParameter("max_heartbeat_age", MaxHeartbeatAge)
        .Default(TDuration::Hours(1));
}

////////////////////////////////////////////////////////////////////////////////

TStrategyOperationSpec::TStrategyOperationSpec()
{
    RegisterParameter("pool", Pool)
        .Default();
    RegisterParameter("scheduling_options_per_pool_tree", SchedulingOptionsPerPoolTree)
        .Alias("fair_share_options_per_pool_tree")
        .Default();
    RegisterParameter("pool_trees", PoolTrees)
        .Default();
    RegisterParameter("max_concurrent_schedule_job_calls", MaxConcurrentControllerScheduleJobCalls)
        .Alias("max_concurrent_controller_schedule_job_calls")
        .Default();
    RegisterParameter("schedule_in_single_tree", ScheduleInSingleTree)
        .Default(false);
    RegisterParameter("tentative_pool_trees", TentativePoolTrees)
        .Default();
    RegisterParameter("use_default_tentative_pool_trees", UseDefaultTentativePoolTrees)
        .Default(false);
    RegisterParameter("tentative_tree_eligibility", TentativeTreeEligibility)
        .DefaultNew();
    RegisterParameter("update_preemptable_jobs_list_logging_period", UpdatePreemptableJobsListLoggingPeriod)
        .Default(1000);
    RegisterParameter("custom_profiling_tag", CustomProfilingTag)
        .Default();
    RegisterParameter("max_unpreemptable_job_count", MaxUnpreemptableRunningJobCount)
        .Default();
    RegisterParameter("max_speculative_job_count_per_task", MaxSpeculativeJobCountPerTask)
        .Default(10);
    RegisterParameter("preemption_mode", PreemptionMode)
        .Default(EPreemptionMode::Normal);

    RegisterPostprocessor([&] {
        if (ScheduleInSingleTree && (TentativePoolTrees || UseDefaultTentativePoolTrees)) {
            THROW_ERROR_EXCEPTION("%Qv option cannot be used simultaneously with tentative pool trees (check %Qv and %Qv)",
                "schedule_in_single_tree",
                "tentative_pool_trees",
                "use_default_tentative_pool_trees");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TOperationFairShareTreeRuntimeParameters::TOperationFairShareTreeRuntimeParameters()
{
    RegisterParameter("weight", Weight)
        .Optional()
        .InRange(MinSchedulableWeight, MaxSchedulableWeight);
    RegisterParameter("pool", Pool);
    RegisterParameter("resource_limits", ResourceLimits)
        .DefaultNew();
    RegisterParameter("enable_detailed_logs", EnableDetailedLogs)
        .Default(false);
    RegisterParameter("tentative", Tentative)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

TOperationRuntimeParameters::TOperationRuntimeParameters()
{
    RegisterParameter("owners", Owners)
        .Optional();
    RegisterParameter("acl", Acl)
        .Optional();

    RegisterParameter("scheduling_options_per_pool_tree", SchedulingOptionsPerPoolTree);

    RegisterParameter("annotations", Annotations)
        .Optional();

    RegisterPostprocessor([&] {
        ValidateOperationAcl(Acl);
        ProcessAclAndOwnersParameters(&Acl, &Owners);
    });
}

////////////////////////////////////////////////////////////////////////////////

TOperationFairShareTreeRuntimeParametersUpdate::TOperationFairShareTreeRuntimeParametersUpdate()
{
    RegisterParameter("weight", Weight)
        .Optional()
        .InRange(MinSchedulableWeight, MaxSchedulableWeight);
    RegisterParameter("pool", Pool)
        .Optional();
    RegisterParameter("resource_limits", ResourceLimits)
        .Default();
    RegisterParameter("enable_detailed_logs", EnableDetailedLogs)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TOperationRuntimeParametersUpdate::TOperationRuntimeParametersUpdate()
{
    RegisterParameter("pool", Pool)
        .Optional();
    RegisterParameter("weight", Weight)
        .Optional()
        .InRange(MinSchedulableWeight, MaxSchedulableWeight);
    RegisterParameter("acl", Acl)
        .Optional();
    RegisterParameter("scheduling_options_per_pool_tree", SchedulingOptionsPerPoolTree)
        .Default();
    RegisterParameter("annotations", Annotations)
        .Optional();

    RegisterPostprocessor([&] {
        if (Acl.has_value()) {
            ValidateOperationAcl(*Acl);
        }
    });
}

bool TOperationRuntimeParametersUpdate::ContainsPool() const
{
    auto result = Pool.has_value();
    for (const auto& [_, treeOptions] : SchedulingOptionsPerPoolTree) {
        result |= treeOptions->Pool.has_value();
    }
    return result;
}

EPermissionSet TOperationRuntimeParametersUpdate::GetRequiredPermissions() const
{
    auto requiredPermissions = EPermissionSet(EPermission::Manage);

    for (const auto& [poolTree, treeParams] : SchedulingOptionsPerPoolTree) {
        if (treeParams->EnableDetailedLogs.has_value()) {
            requiredPermissions |= EPermission::Administer;
            break;
        }
    }

    return requiredPermissions;
}

////////////////////////////////////////////////////////////////////////////////

TOperationFairShareTreeRuntimeParametersPtr UpdateFairShareTreeRuntimeParameters(
    const TOperationFairShareTreeRuntimeParametersPtr& origin,
    const TOperationFairShareTreeRuntimeParametersUpdatePtr& update)
{
    try {
        if (!origin) {
            return NYTree::ConvertTo<TOperationFairShareTreeRuntimeParametersPtr>(ConvertToNode(update));
        }
        return UpdateYsonSerializable(origin, ConvertToNode(update));
    } catch (const std::exception& exception) {
        THROW_ERROR_EXCEPTION("Error updating operation fair share tree runtime parameters")
            << exception;
    }
}

TOperationRuntimeParametersPtr UpdateRuntimeParameters(
    const TOperationRuntimeParametersPtr& origin,
    const TOperationRuntimeParametersUpdatePtr& update)
{
    YT_VERIFY(origin);
    auto result = CloneYsonSerializable(origin);
    if (update->Acl) {
        result->Acl = *update->Acl;
    }
    for (auto& [poolTree, treeParams] : result->SchedulingOptionsPerPoolTree) {
        auto treeUpdateIt = update->SchedulingOptionsPerPoolTree.find(poolTree);
        if (treeUpdateIt != update->SchedulingOptionsPerPoolTree.end()) {
            treeParams = UpdateFairShareTreeRuntimeParameters(treeParams, treeUpdateIt->second);
        }

        // NB: root level attributes has higher priority.
        if (update->Weight) {
            treeParams->Weight = *update->Weight;
        }
        if (update->Pool) {
            treeParams->Pool = TPoolName(*update->Pool, std::nullopt);
        }
    }

    if (update->Annotations) {
        auto annotationsPatch = *update->Annotations;
        if (!result->Annotations) {
            result->Annotations = annotationsPatch;
        } else if (!annotationsPatch) {
            result->Annotations = nullptr;
        } else {
            result->Annotations = NYTree::PatchNode(result->Annotations, annotationsPatch)->AsMap();
        }
    }

    return result;
}

TSchedulerConnectionConfig::TSchedulerConnectionConfig()
{
    RegisterParameter("rpc_timeout", RpcTimeout)
        .Default(TDuration::Seconds(60));
    RegisterParameter("rpc_acknowledgement_timeout", RpcAcknowledgementTimeout)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

TJobCpuMonitorConfig::TJobCpuMonitorConfig()
{
    RegisterParameter("enable_cpu_reclaim", EnableCpuReclaim)
        .Default(false);

    RegisterParameter("check_period", CheckPeriod)
        .Default(TDuration::Seconds(1));

    RegisterParameter("start_delay", StartDelay)
        .Default(TDuration::Zero());

    RegisterParameter("smoothing_factor", SmoothingFactor)
        .InRange(0, 1)
        .Default(0.1);

    RegisterParameter("relative_upper_bound", RelativeUpperBound)
        .InRange(0, 1)
        .Default(0.9);

    RegisterParameter("relative_lower_bound", RelativeLowerBound)
        .InRange(0, 1)
        .Default(0.6);

    RegisterParameter("increase_coefficient", IncreaseCoefficient)
        .InRange(1, 2)
        .Default(1.45);

    RegisterParameter("decrease_coefficient", DecreaseCoefficient)
        .InRange(0, 1)
        .Default(0.97);

    RegisterParameter("vote_window_size", VoteWindowSize)
        .GreaterThan(0)
        .Default(5);

    RegisterParameter("vote_decision_threshold", VoteDecisionThreshold)
        .GreaterThan(0)
        .Default(3);

    RegisterParameter("min_cpu_limit", MinCpuLimit)
        .InRange(0, 1)
        .Default(1);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_DYNAMIC_PHOENIX_TYPE(TEraseOperationSpec);
DEFINE_DYNAMIC_PHOENIX_TYPE(TMapOperationSpec);
DEFINE_DYNAMIC_PHOENIX_TYPE(TMapReduceOperationSpec);
DEFINE_DYNAMIC_PHOENIX_TYPE(TMergeOperationSpec);
DEFINE_DYNAMIC_PHOENIX_TYPE(TNewReduceOperationSpec);
DEFINE_DYNAMIC_PHOENIX_TYPE(TOperationSpecBase);
DEFINE_DYNAMIC_PHOENIX_TYPE(TOrderedMergeOperationSpec);
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

} // namespace NYT::NScheduler
