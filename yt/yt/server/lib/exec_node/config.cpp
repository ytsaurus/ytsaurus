#include "config.h"

#include <yt/yt/library/tracing/jaeger/sampler.h>

#include <yt/yt/core/misc/config.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TSlotLocationConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("disk_quota", &TThis::DiskQuota)
        .Default()
        .GreaterThan(0);

    registrar.Parameter("disk_usage_watermark", &TThis::DiskUsageWatermark)
        .Default(10_GB)
        .GreaterThanOrEqual(0);

    registrar.Parameter("medium_name", &TThis::MediumName)
        .Default(NChunkClient::DefaultSlotsMediumName);

    registrar.Parameter("enable_disk_quota", &TThis::EnableDiskQuota)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TNumaNodeConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("numa_node_id", &TThis::NumaNodeId)
        .Default(0);
    registrar.Parameter("cpu_count", &TThis::CpuCount)
        .Default(0);
    registrar.Parameter("cpu_set", &TThis::CpuSet)
        .Default(EmptyCpuSet);
}

////////////////////////////////////////////////////////////////////////////////

void TSlotManagerTestingConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("skip_job_proxy_unavailable_alert", &TThis::SkipJobProxyUnavailableAlert)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TSlotManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("locations", &TThis::Locations);

    registrar.Parameter("enable_tmpfs", &TThis::EnableTmpfs)
        .Default(true);

    registrar.Parameter("detached_tmpfs_umount", &TThis::DetachedTmpfsUmount)
        .Default(true);

    registrar.Parameter("job_environment", &TThis::JobEnvironment)
        .DefaultCtor([] {
            return NJobProxy::TJobEnvironmentConfig(NJobProxy::EJobEnvironmentType::Simple);
        });

    registrar.Parameter("file_copy_chunk_size", &TThis::FileCopyChunkSize)
        .GreaterThanOrEqual(1_KB)
        .Default(10_MB);

    registrar.Parameter("enable_read_write_copy", &TThis::EnableReadWriteCopy)
        .Default(false);

    registrar.Parameter("enable_artifact_copy_tracking", &TThis::EnableArtifactCopyTracking)
        .Default(false);

    registrar.Parameter("do_not_set_user_id", &TThis::DoNotSetUserId)
        .Default(false);

    registrar.Parameter("disk_resources_update_period", &TThis::DiskResourcesUpdatePeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("slot_location_statistics_update_period", &TThis::SlotLocationStatisticsUpdatePeriod)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("default_medium_name", &TThis::DefaultMediumName)
        .Default(NChunkClient::DefaultSlotsMediumName);

    registrar.Parameter("testing", &TThis::Testing)
        .DefaultNew();

    registrar.Parameter("numa_nodes", &TThis::NumaNodes)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        std::unordered_set<i64> numaNodeIds;
        for (const auto& numaNode : config->NumaNodes) {
            if (numaNodeIds.contains(numaNode->NumaNodeId)) {
                THROW_ERROR_EXCEPTION("Numa nodes ids must be unique in \"numa_nodes\" list, but duplicate found")
                    << TErrorAttribute("numa_node_id", numaNode->NumaNodeId);
            }
            numaNodeIds.insert(numaNode->NumaNodeId);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TSlotManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("disable_jobs_on_gpu_check_failure", &TThis::DisableJobsOnGpuCheckFailure)
        .Default(true);

    registrar.Parameter("check_disk_space_limit", &TThis::CheckDiskSpaceLimit)
        .Default(true);

    registrar.Parameter("idle_cpu_fraction", &TThis::IdleCpuFraction)
        .Default(0);

    registrar.Parameter("enable_numa_node_scheduling", &TThis::EnableNumaNodeScheduling)
        .Default(false);

    registrar.Parameter("enable_job_environment_resurrection", &TThis::EnableJobEnvironmentResurrection)
        .Default(true);

    registrar.Parameter("max_consecutive_gpu_job_failures", &TThis::MaxConsecutiveGpuJobFailures)
        .Default(50);
    registrar.Parameter("max_consecutive_job_aborts", &TThis::MaxConsecutiveJobAborts)
        .Default(500);
    registrar.Parameter("disable_jobs_backoff_strategy", &TThis::DisableJobsBackoffStrategy)
        .Default({
            .Backoff = TDuration::Minutes(10),
            .BackoffJitter = 1.0,
        });

    registrar.Parameter("should_close_descriptors", &TThis::ShouldCloseDescriptors)
        .Default(false);

    registrar.Parameter("slot_init_timeout", &TThis::SlotInitTimeout)
        .Default(TDuration::Minutes(10));

    registrar.Parameter("slot_release_timeout", &TThis::SlotReleaseTimeout)
        .Default(TDuration::Minutes(20));

    registrar.Parameter("volume_release_timeout", &TThis::VolumeReleaseTimeout)
        .Default(TDuration::Minutes(20));

    registrar.Parameter("abort_on_free_volume_synchronization_failed", &TThis::AbortOnFreeVolumeSynchronizationFailed)
        .Default(false);

    registrar.Parameter("abort_on_jobs_disabled", &TThis::AbortOnJobsDisabled)
        .Default(false);

    registrar.Parameter("enable_container_device_checker", &TThis::EnableContainerDeviceChecker)
        .Default(true);

    registrar.Parameter("restart_container_after_failed_device_check", &TThis::RestartContainerAfterFailedDeviceCheck)
        .Default(true);

    registrar.Parameter("job_environment", &TThis::JobEnvironment)
        .DefaultCtor([] {
            return NJobProxy::TJobEnvironmentConfig(NJobProxy::EJobEnvironmentType::Simple);
        });}

////////////////////////////////////////////////////////////////////////////////

void TVolumeManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("delay_after_layer_imported", &TThis::DelayAfterLayerImported)
        .Default();

    registrar.Parameter("enable_async_layer_removal", &TThis::EnableAsyncLayerRemoval)
        .Default(true);

    registrar.Parameter("abort_on_operation_with_volume_failed", &TThis::AbortOnOperationWithVolumeFailed)
        .Default(false);

    registrar.Parameter("abort_on_operation_with_layer_failed", &TThis::AbortOnOperationWithLayerFailed)
        .Default(false);

    registrar.Parameter("throw_on_prepare_volume", &TThis::ThrowOnPrepareVolume)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TChunkCacheDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("test_cache_location_disabling", &TThis::TestCacheLocationDisabling)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TUserJobSensor::Register(TRegistrar registrar)
{
    registrar.Parameter("type", &TThis::Type);

    registrar.Parameter("profiling_name", &TThis::ProfilingName);
}

////////////////////////////////////////////////////////////////////////////////

void TUserJobStatisticSensor::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
}

////////////////////////////////////////////////////////////////////////////////

const THashMap<TString, TUserJobStatisticSensorPtr>& TUserJobMonitoringDynamicConfig::GetDefaultStatisticSensors()
{
    static const auto DefaultStatisticSensors = ConvertTo<THashMap<TString, TUserJobStatisticSensorPtr>>(BuildYsonStringFluently()
        .BeginMap()
            .Item("cpu/burst").BeginMap()
                .Item("path").Value("/user_job/cpu/burst")
                .Item("type").Value("counter")
                .Item("profiling_name").Value("/user_job/cpu/burst")
            .EndMap()
            .Item("cpu/user").BeginMap()
                .Item("path").Value("/user_job/cpu/user")
                .Item("type").Value("counter")
                .Item("profiling_name").Value("/user_job/cpu/user")
            .EndMap()
            .Item("cpu/system").BeginMap()
                .Item("path").Value("/user_job/cpu/system")
                .Item("type").Value("counter")
                .Item("profiling_name").Value("/user_job/cpu/system")
            .EndMap()
            .Item("cpu/wait").BeginMap()
                .Item("path").Value("/user_job/cpu/wait")
                .Item("type").Value("counter")
                .Item("profiling_name").Value("/user_job/cpu/wait")
            .EndMap()
            .Item("cpu/throttled").BeginMap()
                .Item("path").Value("/user_job/cpu/throttled")
                .Item("type").Value("counter")
                .Item("profiling_name").Value("/user_job/cpu/throttled")
            .EndMap()
            .Item("cpu/cfs_throttled").BeginMap()
                .Item("path").Value("/user_job/cpu/cfs_throttled")
                .Item("type").Value("counter")
                .Item("profiling_name").Value("/user_job/cpu/cfs_throttled")
            .EndMap()
            .Item("cpu/context_switches").BeginMap()
                .Item("path").Value("/user_job/cpu/context_switches")
                .Item("type").Value("counter")
                .Item("profiling_name").Value("/user_job/cpu/context_switches")
            .EndMap()

            .Item("current_memory/rss").BeginMap()
                .Item("path").Value("/user_job/current_memory/rss")
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/current_memory/rss")
            .EndMap()
            .Item("current_memory/mapped_file").BeginMap()
                .Item("path").Value("/user_job/current_memory/mapped_file")
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/current_memory/mapped_file")
            .EndMap()
            .Item("current_memory/major_page_faults").BeginMap()
                .Item("path").Value("/user_job/current_memory/major_page_faults")
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/current_memory/major_page_faults")
            .EndMap()
            .Item("tmpfs_size").BeginMap()
                .Item("path").Value("/user_job/tmpfs_size")
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/tmpfs_size")
            .EndMap()
            .Item("disk/usage").BeginMap()
                .Item("path").Value("/user_job/disk/usage")
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/disk/usage")
            .EndMap()
            .Item("disk/limit").BeginMap()
                .Item("path").Value("/user_job/disk/limit")
                .Item("type").Value("gauge")
                .Item("profiling_name").Value("/user_job/disk/limit")
            .EndMap()

            .Item("block_io/io_total").BeginMap()
                .Item("path").Value("/user_job/block_io/io_total")
                .Item("type").Value("counter")
                .Item("profiling_name").Value("/user_job/block_io/io_total")
            .EndMap()

            .Item("network/rx_bytes").BeginMap()
                .Item("path").Value("/user_job/network/rx_bytes")
                .Item("type").Value("counter")
                .Item("profiling_name").Value("/user_job/network/rx_bytes")
            .EndMap()
            .Item("network/tx_bytes").BeginMap()
                .Item("path").Value("/user_job/network/tx_bytes")
                .Item("type").Value("counter")
                .Item("profiling_name").Value("/user_job/network/tx_bytes")
            .EndMap()
        .EndMap());

    return DefaultStatisticSensors;
}

void TUserJobMonitoringDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("statistic_sensors", &TThis::StatisticSensors)
        .Alias("sensors")
        .DefaultCtor(&TUserJobMonitoringDynamicConfig::GetDefaultStatisticSensors);
}

////////////////////////////////////////////////////////////////////////////////

void THeartbeatReporterDynamicConfigBase::Register(TRegistrar registrar)
{
    registrar.Parameter("heartbeat_executor", &TThis::HeartbeatExecutor)
        .Default({
            {
                .Period = TDuration::Seconds(5),
                .Splay = TDuration::Seconds(1),
                .Jitter = 0.0,
            },
            {
                .MinBackoff = TDuration::Seconds(5),
                .MaxBackoff = TDuration::Seconds(60),
                .BackoffMultiplier = 2.0,
            },
        });

    registrar.Parameter("enable_tracing", &TThis::EnableTracing)
        .Default(false);

    registrar.Parameter("tracing_sampler", &TThis::TracingSampler)
        .DefaultNew();
}

void FormatValue(TStringBuilderBase* builder, const THeartbeatReporterDynamicConfigBase& config, TStringBuf /*spec*/)
{
    builder->AppendFormat(
        "{Period: %v, Splay: %v, MinBackoff: %v, MaxBackoff: %v, BackoffMultiplier: %v}",
        config.HeartbeatExecutor.Period,
        config.HeartbeatExecutor.Splay,
        config.HeartbeatExecutor.MinBackoff,
        config.HeartbeatExecutor.MaxBackoff,
        config.HeartbeatExecutor.BackoffMultiplier);
}

////////////////////////////////////////////////////////////////////////////////

void TControllerAgentConnectorDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("settle_jobs_timeout", &TThis::SettleJobsTimeout)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("test_heartbeat_delay", &TThis::TestHeartbeatDelay)
        .Default();

    registrar.Parameter("statistics_throttler", &TThis::StatisticsThrottler)
        .DefaultCtor([] { return NConcurrency::TThroughputThrottlerConfig::Create(1_MB); });
    registrar.Parameter("running_job_statistics_sending_backoff", &TThis::RunningJobStatisticsSendingBackoff)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("job_staleness_delay", &TThis::JobStalenessDelay)
        .Default(TDuration::Minutes(10));
}

void FormatValue(TStringBuilderBase* builder, const TControllerAgentConnectorDynamicConfig& config, TStringBuf spec)
{
    FormatValue(builder, static_cast<const THeartbeatReporterDynamicConfigBase&>(config), spec);
}

////////////////////////////////////////////////////////////////////////////////

void TMasterConnectorDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("heartbeat_timeout", &TThis::HeartbeatTimeout)
        .Default(TDuration::Seconds(60));

    registrar.Preprocessor([] (TThis* config) {
        config->HeartbeatExecutor.Jitter = 0.3;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TSchedulerConnectorDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter(
        "send_heartbeat_on_resources_released",
        &TThis::SendHeartbeatOnResourcesReleased)
        .Default(true);

    registrar.Parameter(
        "include_releasing_resources_in_scheduler_heartbeat",
        &TThis::IncludeReleasingResourcesInSchedulerHeartbeat)
        .Default(false);

    registrar.Parameter(
        "use_profiling_tags_from_scheduler",
        &TThis::UseProfilingTagsFromScheduler)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TJobInputCacheDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(false);
    registrar.Parameter("job_count_threshold", &TThis::JobCountThreshold)
        .Default();
    registrar.Parameter("block_cache", &TThis::BlockCache)
        .DefaultNew();
    registrar.Parameter("meta_cache", &TThis::MetaCache)
        .DefaultNew();
    registrar.Parameter("total_in_flight_block_size", &TThis::TotalInFlightBlockSize)
        .Alias("summary_block_size_in_flight")
        .Default(0);
    registrar.Parameter("fallback_timeout_fraction", &TThis::FallbackTimeoutFraction)
        .InRange(0.0, 1.0)
        .Default(0.8);
}

////////////////////////////////////////////////////////////////////////////////

void TGpuManagerTestingConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("test_resource", &TThis::TestResource)
        .Default(false);
    registrar.Parameter("test_layers", &TThis::TestLayers)
        .Default(false);
    registrar.Parameter("test_setup_commands", &TThis::TestSetupCommands)
        .Default(false);
    registrar.Parameter("test_extra_gpu_check_command_failure", &TThis::TestExtraGpuCheckCommandFailure)
        .Default(false);
    registrar.Parameter("test_gpu_count", &TThis::TestGpuCount)
        .Default(0);
    registrar.Parameter("test_utilization_gpu_rate", &TThis::TestUtilizationGpuRate)
        .InRange(0.0, 1.0)
        .Default(0.0);
    registrar.Parameter("test_gpu_info_update_period", &TThis::TestGpuInfoUpdatePeriod)
        .Default(TDuration::MilliSeconds(100));

    registrar.Postprocessor([] (TThis* config) {
        if (config->TestLayers && !config->TestResource) {
            THROW_ERROR_EXCEPTION("You need to specify 'test_resource' option if 'test_layers' is specified");
        }
        if (config->TestGpuCount > 0 && !config->TestResource) {
            THROW_ERROR_EXCEPTION("You need to specify 'test_resource' option if 'test_gpu_count' is greater than zero");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TGpuManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);

    registrar.Parameter("driver_layer_directory_path", &TThis::DriverLayerDirectoryPath)
        .Default();
    registrar.Parameter("driver_version", &TThis::DriverVersion)
        .Default();

    registrar.Parameter("gpu_info_source", &TThis::GpuInfoSource)
        .DefaultNew();

    registrar.Parameter("testing", &TThis::Testing)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TGpuManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("health_check_timeout", &TThis::HealthCheckTimeout)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("health_check_period", &TThis::HealthCheckPeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("health_check_failure_backoff", &TThis::HealthCheckFailureBackoff)
        .Default(TDuration::Minutes(10));

    registrar.Parameter("rdma_device_info_update_timeout", &TThis::RdmaDeviceInfoUpdateTimeout)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("rdma_device_info_update_period", &TThis::RdmaDeviceInfoUpdatePeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("job_setup_command", &TThis::JobSetupCommand)
        .Default();

    registrar.Parameter("driver_layer_fetching", &TThis::DriverLayerFetching)
        .Default({
            .Period = TDuration::Minutes(5),
            .Splay = TDuration::Minutes(5),
        });

    registrar.Parameter("cuda_toolkit_min_driver_version", &TThis::CudaToolkitMinDriverVersion)
        .Default();

    registrar.Parameter("gpu_info_source", &TThis::GpuInfoSource)
        .Default();

    registrar.Parameter("default_nvidia_driver_capabilities", &TThis::DefaultNvidiaDriverCapabilities)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TShellCommandConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .NonEmpty();
    registrar.Parameter("args", &TThis::Args)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TTestingConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("fail_address_resolve", &TThis::FailAddressResolve)
        .Default(false);
}

void TJobCommonConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("node_directory_prepare_retry_count", &TThis::NodeDirectoryPrepareRetryCount)
        .Default(10);

    registrar.Parameter("node_directory_prepare_backoff_time", &TThis::NodeDirectoryPrepareBackoffTime)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("job_proxy_preparation_timeout", &TThis::JobProxyPreparationTimeout)
        .Default(TDuration::Minutes(3));

    registrar.Parameter("waiting_for_job_cleanup_timeout", &TThis::WaitingForJobCleanupTimeout)
        .Default(TDuration::Minutes(15));

    registrar.Parameter("job_prepare_time_limit", &TThis::JobPrepareTimeLimit)
        .Default();

    registrar.Parameter("test_job_error_truncation", &TThis::TestJobErrorTruncation)
        .Default(false);

    registrar.Parameter("memory_tracker_cache_period", &TThis::MemoryTrackerCachePeriod)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("smaps_memory_tracker_cache_period", &TThis::SMapsMemoryTrackerCachePeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("user_job_monitoring", &TThis::UserJobMonitoring)
        .DefaultNew();

    registrar.Parameter("sensor_dump_timeout", &TThis::SensorDumpTimeout)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("treat_job_proxy_failure_as_abort", &TThis::TreatJobProxyFailureAsAbort)
        .Default(false);

    registrar.Parameter("job_setup_command", &TThis::JobSetupCommand)
        .Default();

    registrar.Parameter("setup_command_user", &TThis::SetupCommandUser)
        .Default("root");

    registrar.Parameter("statistics_output_table_count_limit", &TThis::StatisticsOutputTableCountLimit)
        .Default();

    registrar.Parameter("job_throttler", &TThis::JobThrottler)
        .DefaultNew();

    registrar.Parameter("virtual_sandbox_squash_fs_block_size", &TThis::VirtualSandboxSquashFSBlockSize)
        .Default(128_KB);

    registrar.Parameter("testing", &TThis::Testing)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TAllocationConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_multiple_jobs", &TThis::EnableMultipleJobs)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TJobControllerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("operation_info_request_backoff_strategy", &TThis::OperationInfoRequestBackoffStrategy)
        .Default({
            .Backoff = TDuration::Seconds(5),
            .BackoffJitter = 0.1,
        });

    // Make it greater than interrupt preemption timeout.
    registrar.Parameter("waiting_for_resources_timeout", &TThis::WaitingForResourcesTimeout)
        .Alias("waiting_jobs_timeout")
        .Default(TDuration::Seconds(30));

    // COMPAT(arkady-e1ppa): This option can be set to false when
    // sched and CA are updated to the fitting version of 24.1
    // which has protocol version 28 in controller_agent_tracker_serive_proxy.h.
    // Remove when everyone is 24.2.
    registrar.Parameter("disable_legacy_allocation_preparation", &TThis::DisableLegacyAllocationPreparation)
        .Default(false);

    registrar.Parameter("cpu_overdraft_timeout", &TThis::CpuOverdraftTimeout)
        .Default(TDuration::Minutes(10));

    registrar.Parameter("min_required_disk_space", &TThis::MinRequiredDiskSpace)
        .Default(100_MB);

    registrar.Parameter("memory_overdraft_timeout", &TThis::MemoryOverdraftTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("resource_adjustment_period", &TThis::ResourceAdjustmentPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("recently_removed_jobs_clean_period", &TThis::RecentlyRemovedJobsCleanPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("recently_removed_jobs_store_timeout", &TThis::RecentlyRemovedJobsStoreTimeout)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("job_proxy_build_info_update_period", &TThis::JobProxyBuildInfoUpdatePeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("disable_job_proxy_profiling", &TThis::DisableJobProxyProfiling)
        .Default(false);

    registrar.Parameter("job_proxy", &TThis::JobProxy)
        .Default();

    registrar.Parameter("unknown_operation_jobs_removal_delay", &TThis::UnknownOperationJobsRemovalDelay)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("disabled_jobs_interruption_timeout", &TThis::DisabledJobsInterruptionTimeout)
        .Default(TDuration::Minutes(1))
        .GreaterThan(TDuration::Zero());

    registrar.Parameter("job_common", &TThis::JobCommon)
        .DefaultNew();

    registrar.Parameter("profiling_period", &TThis::ProfilingPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("profile_job_proxy_process_exit", &TThis::ProfileJobProxyProcessExit)
        .Default(false);

    registrar.Parameter("test_resource_acquisition_delay", &TThis::TestResourceAcquisitionDelay)
        .Default();

    registrar.Parameter("allocation", &TThis::Allocation)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TNbdClientConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("io_timeout", &TThis::IOTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("reconnect_timeout", &TThis::ReconnectTimeout)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("connection_count", &TThis::ConnectionCount)
        .Default(2);
}

////////////////////////////////////////////////////////////////////////////////

void TNbdConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default();
    registrar.Parameter("client", &TThis::Client)
        .DefaultNew();
    registrar.Parameter("server", &TThis::Server)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TJobProxyLoggingConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("mode", &TThis::Mode)
        .Default(EJobProxyLoggingMode::Simple);

    registrar.Parameter("log_manager_template", &TThis::LogManagerTemplate)
        .DefaultNew();

    registrar.Parameter("job_proxy_stderr_path", &TThis::JobProxyStderrPath)
        .Default();

    registrar.Parameter("executor_stderr_path", &TThis::ExecutorStderrPath)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TJobProxyConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("job_proxy_logging", &TThis::JobProxyLogging)
        .DefaultNew();

    registrar.Parameter("job_proxy_jaeger", &TThis::JobProxyJaeger)
        .DefaultNew();

    registrar.Parameter("job_proxy_dns_over_rpc_resolver", &TThis::JobProxyDnsOverRpcResolver)
        .DefaultNew();

    registrar.Parameter("job_proxy_authentication_manager", &TThis::JobProxyAuthenticationManager)
        .DefaultNew();

    registrar.Parameter("core_watcher", &TThis::CoreWatcher)
        .DefaultNew();

    registrar.Parameter("supervisor_rpc_timeout", &TThis::SupervisorRpcTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("job_proxy_heartbeat_period", &TThis::JobProxyHeartbeatPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("job_proxy_send_heartbeat_before_abort", &TThis::JobProxySendHeartbeatBeforeAbort)
        .Default(false);

    registrar.Parameter("test_root_fs", &TThis::TestRootFS)
        .Default(false);

    registrar.Parameter("test_poll_job_shell", &TThis::TestPollJobShell)
        .Default(false);

    registrar.Parameter("check_user_job_memory_limit", &TThis::CheckUserJobMemoryLimit)
        .Default(true);

    registrar.Parameter("always_abort_on_memory_reserve_overdraft", &TThis::AlwaysAbortOnMemoryReserveOverdraft)
        .Default(false);

    registrar.Parameter("forward_all_environment_variables", &TThis::ForwardAllEnvironmentVariables)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TLogDumpConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("buffer_size", &TThis::BufferSize)
        .Default(1_MB)
        .GreaterThan(0);

    registrar.Parameter("log_writer_name", &TThis::LogWriterName);
}

void TJobProxyLogManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("directory", &TThis::Directory);

    registrar.Parameter("sharding_key_length", &TThis::ShardingKeyLength)
        .GreaterThan(0);

    registrar.Parameter("logs_storage_period", &TThis::LogsStoragePeriod);

    registrar.Parameter("directory_traversal_concurrency", &TThis::DirectoryTraversalConcurrency)
        .Default(0)
        .GreaterThanOrEqual(0);

    registrar.Parameter("log_dump", &TThis::LogDump);
}

////////////////////////////////////////////////////////////////////////////////

void TLogDumpDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("buffer_size", &TThis::BufferSize)
        .Default()
        .GreaterThan(0);

    registrar.Parameter("log_writer_name", &TThis::LogWriterName)
        .Default();
}

void TJobProxyLogManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("logs_storage_period", &TThis::LogsStoragePeriod)
        .Default();

    registrar.Parameter("directory_traversal_concurrency", &TThis::DirectoryTraversalConcurrency)
        .Default()
        .GreaterThanOrEqual(0);

    registrar.Parameter("log_dump", &TThis::LogDump)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TExecNodeConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("root_fs_binds", &TThis::RootFSBinds)
        .Default();

    registrar.Parameter("slot_manager", &TThis::SlotManager)
        .DefaultNew();

    registrar.Parameter("gpu_manager", &TThis::GpuManager)
        .DefaultNew();

    registrar.Parameter("job_proxy_solomon_exporter", &TThis::JobProxySolomonExporter)
        .DefaultNew();

    registrar.Parameter("job_proxy", &TThis::JobProxy)
        .DefaultNew();

    registrar.Parameter("job_proxy_log_manager", &TThis::JobProxyLogManager);
}

////////////////////////////////////////////////////////////////////////////////

void TExecNodeDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("master_connector", &TThis::MasterConnector)
        .DefaultNew();

    registrar.Parameter("slot_manager", &TThis::SlotManager)
        .DefaultNew();

    registrar.Parameter("volume_manager", &TThis::VolumeManager)
        .DefaultNew();

    registrar.Parameter("job_proxy_log_manager", &TThis::JobProxyLogManager)
        .DefaultNew();

    registrar.Parameter("job_controller", &TThis::JobController)
        .DefaultNew();

    registrar.Parameter("gpu_manager", &TThis::GpuManager)
        .DefaultNew();

    registrar.Parameter("job_reporter", &TThis::JobReporter)
        .DefaultNew();

    registrar.Parameter("scheduler_connector", &TThis::SchedulerConnector)
        .DefaultNew();

    registrar.Parameter("controller_agent_connector", &TThis::ControllerAgentConnector)
        .DefaultNew();

    registrar.Parameter("user_job_container_creation_throttler", &TThis::UserJobContainerCreationThrottler)
        .DefaultNew();

    registrar.Parameter("chunk_cache", &TThis::ChunkCache)
        .DefaultNew();

    registrar.Parameter("job_input_cache", &TThis::JobInputCache)
        .DefaultNew();

    registrar.Parameter("nbd", &TThis::Nbd)
        .Default();

    registrar.Preprocessor([] (TThis* config) {
        // 10 user jobs containers per second by default.
        config->UserJobContainerCreationThrottler->Limit = 10;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
