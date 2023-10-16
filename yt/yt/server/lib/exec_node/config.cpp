#include "config.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NExecNode {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TJobThrashingDetectorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(false);
    registrar.Parameter("check_period", &TThis::CheckPeriod)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("major_page_fault_count_threshold", &TThis::MajorPageFaultCountLimit)
        .Default(500);
    registrar.Parameter("limit_overflow_count_threshold_to_abort_job", &TThis::LimitOverflowCountThresholdToAbortJob)
        .Default(5);
}

////////////////////////////////////////////////////////////////////////////////

void TJobEnvironmentConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("type", &TThis::Type)
        .Default(EJobEnvironmentType::Simple);

    registrar.Parameter("start_uid", &TThis::StartUid)
        .Default(10000);

    registrar.Parameter("memory_watchdog_period", &TThis::MemoryWatchdogPeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("job_thrashing_detector", &TThis::JobThrashingDetector)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TTestingJobEnvironmentConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("testing_job_environment_scenario", &TThis::TestingJobEnvironmentScenario)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TPortoJobEnvironmentConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("porto_executor", &TThis::PortoExecutor)
        .DefaultNew();

    registrar.Parameter("block_io_watchdog_period", &TThis::BlockIOWatchdogPeriod)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("external_binds", &TThis::ExternalBinds)
        .Default();

    registrar.Parameter("jobs_io_weight", &TThis::JobsIOWeight)
        .Default(0.05);
    registrar.Parameter("node_dedicated_cpu", &TThis::NodeDedicatedCpu)
        .GreaterThanOrEqual(0)
        .Default(2);

    registrar.Parameter("use_short_container_names", &TThis::UseShortContainerNames)
        .Default(false);

    registrar.Parameter("use_daemon_subcontainer", &TThis::UseDaemonSubcontainer)
        .Default(false);

    registrar.Parameter("use_exec_from_layer", &TThis::UseExecFromLayer)
        .Default(false);

    registrar.Parameter("allow_mount_fuse_device", &TThis::AllowMountFuseDevice)
        .Default(true);

    registrar.Parameter("container_destruction_backoff", &TThis::ContainerDestructionBackoff)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

void TCriJobEnvironmentConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cri_executor", &TThis::CriExecutor)
        .DefaultNew();

    registrar.Parameter("job_proxy_image", &TThis::JobProxyImage)
        .NonEmpty();

    registrar.Parameter("job_proxy_bind_mounts", &TThis::JobProxyBindMounts)
        .Default();

    registrar.Parameter("use_job_proxy_from_image", &TThis::UseJobProxyFromImage);
}

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
        .DefaultCtor([] () { return ConvertToNode(New<TSimpleJobEnvironmentConfig>()); });
    registrar.Parameter("file_copy_chunk_size", &TThis::FileCopyChunkSize)
        .GreaterThanOrEqual(1_KB)
        .Default(10_MB);
    registrar.Parameter("enable_read_write_copy", &TThis::EnableReadWriteCopy)
        .Default(false);

    registrar.Parameter("disk_resources_update_period", &TThis::DiskResourcesUpdatePeriod)
        .Alias("disk_info_update_period")
        .Default(TDuration::Seconds(5));
    registrar.Parameter("slot_location_statistics_update_period", &TThis::SlotLocationStatisticsUpdatePeriod)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("max_consecutive_job_aborts", &TThis::MaxConsecutiveJobAborts)
        .Alias("max_consecutive_aborts")
        .Default(500);
    registrar.Parameter("max_consecutive_gpu_job_failures", &TThis::MaxConsecutiveGpuJobFailures)
        .Default(50);
    registrar.Parameter("disable_jobs_timeout", &TThis::DisableJobsTimeout)
        .Default(TDuration::Minutes(10));

    registrar.Parameter("default_medium_name", &TThis::DefaultMediumName)
        .Default(NChunkClient::DefaultSlotsMediumName);

    registrar.Parameter("disable_jobs_on_gpu_check_failure", &TThis::DisableJobsOnGpuCheckFailure)
        .Default(true);

    registrar.Parameter("idle_cpu_fraction", &TThis::IdleCpuFraction)
        .Default(0);

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

void TOldHeartbeatReporterDynamicConfigBase::Register(TRegistrar registrar)
{
    registrar.Parameter("heartbeat_period", &TThis::HeartbeatPeriod)
        .Default();
    registrar.Parameter("heartbeat_splay", &TThis::HeartbeatSplay)
        .Default();
    registrar.Parameter("failed_heartbeat_backoff_start_time", &TThis::FailedHeartbeatBackoffStartTime)
        .Default();
    registrar.Parameter("failed_heartbeat_backoff_max_time", &TThis::FailedHeartbeatBackoffMaxTime)
        .Default();
    registrar.Parameter("failed_heartbeat_backoff_multiplier", &TThis::FailedHeartbeatBackoffMultiplier)
        .GreaterThanOrEqual(1.0)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void THeartbeatReporterDynamicConfigBase::Register(TRegistrar registrar)
{
    registrar.Parameter("heartbeat_period", &TThis::HeartbeatPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("heartbeat_splay", &TThis::HeartbeatSplay)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("failed_heartbeat_backoff_start_time", &TThis::FailedHeartbeatBackoffStartTime)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Seconds(5));
    registrar.Parameter("failed_heartbeat_backoff_max_time", &TThis::FailedHeartbeatBackoffMaxTime)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Seconds(60));
    registrar.Parameter("failed_heartbeat_backoff_multiplier", &TThis::FailedHeartbeatBackoffMultiplier)
        .GreaterThanOrEqual(1.0)
        .Default(2.0);
}

////////////////////////////////////////////////////////////////////////////////

void TSchedulerConnectorDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter(
        "send_heartbeat_on_job_finished",
        &TSchedulerConnectorDynamicConfig::SendHeartbeatOnJobFinished)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TControllerAgentConnectorDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("test_heartbeat_delay", &TThis::TestHeartbeatDelay)
        .Default();
    registrar.Parameter("statistics_throttler", &TThis::StatisticsThrottler)
        .Default();
    registrar.Parameter("running_job_statistics_sending_backoff", &TThis::RunningJobStatisticsSendingBackoff)
        .Default();
    registrar.Parameter("use_job_tracker_service_to_settle_jobs", &TThis::UseJobTrackerServiceToSettleJobs)
        .Default(false);
    registrar.Parameter("total_confirmation_period", &TThis::TotalConfirmationPeriod)
        .Default(TDuration::Minutes(10));
}

////////////////////////////////////////////////////////////////////////////////

void THeartbeatReporterConfigBase::Register(TRegistrar registrar)
{
    registrar.Parameter("heartbeat_period", &TThis::HeartbeatPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("heartbeat_splay", &TThis::HeartbeatSplay)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("failed_heartbeat_backoff_start_time", &TThis::FailedHeartbeatBackoffStartTime)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Seconds(5));
    registrar.Parameter("failed_heartbeat_backoff_max_time", &TThis::FailedHeartbeatBackoffMaxTime)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Seconds(60));
    registrar.Parameter("failed_heartbeat_backoff_multiplier", &TThis::FailedHeartbeatBackoffMultiplier)
        .GreaterThanOrEqual(1.0)
        .Default(2.0);
}

void THeartbeatReporterConfigBase::ApplyDynamicInplace(const TOldHeartbeatReporterDynamicConfigBase& dynamicConfig)
{
    HeartbeatPeriod = dynamicConfig.HeartbeatPeriod.value_or(HeartbeatPeriod);
    HeartbeatSplay = dynamicConfig.HeartbeatSplay.value_or(HeartbeatSplay);
    FailedHeartbeatBackoffStartTime = dynamicConfig.FailedHeartbeatBackoffStartTime.value_or(
        FailedHeartbeatBackoffStartTime);
    FailedHeartbeatBackoffMaxTime = dynamicConfig.FailedHeartbeatBackoffMaxTime.value_or(
        FailedHeartbeatBackoffMaxTime);
    FailedHeartbeatBackoffMultiplier = dynamicConfig.FailedHeartbeatBackoffMultiplier.value_or(
        FailedHeartbeatBackoffMultiplier);
}

////////////////////////////////////////////////////////////////////////////////

void TControllerAgentConnectorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("statistics_throttler", &TThis::StatisticsThrottler)
        .DefaultCtor([] () { return NConcurrency::TThroughputThrottlerConfig::Create(1_MB); });
    registrar.Parameter("running_job_statistics_sending_backoff", &TThis::RunningJobStatisticsSendingBackoff)
        .Default(TDuration::Seconds(30));
}

TControllerAgentConnectorConfigPtr TControllerAgentConnectorConfig::ApplyDynamic(const TControllerAgentConnectorDynamicConfigPtr& dynamicConfig) const
{
    YT_VERIFY(dynamicConfig);

    auto newConfig = CloneYsonStruct(MakeStrong(this));
    newConfig->ApplyDynamicInplace(*dynamicConfig);

    return newConfig;
}

void TControllerAgentConnectorConfig::ApplyDynamicInplace(const TControllerAgentConnectorDynamicConfig& dynamicConfig)
{
    THeartbeatReporterConfigBase::ApplyDynamicInplace(dynamicConfig);
    if (dynamicConfig.StatisticsThrottler) {
        StatisticsThrottler->Limit = dynamicConfig.StatisticsThrottler->Limit;
        StatisticsThrottler->Period = dynamicConfig.StatisticsThrottler->Period;
    }
    RunningJobStatisticsSendingBackoff = dynamicConfig.RunningJobStatisticsSendingBackoff.value_or(
        RunningJobStatisticsSendingBackoff);

    Postprocess();
}

////////////////////////////////////////////////////////////////////////////////

void TUserJobSensor::Register(TRegistrar registrar)
{
    registrar.Parameter("type", &TThis::Type);
    registrar.Parameter("source", &TThis::Source)
        .Default(EUserJobSensorSource::Statistics);
    registrar.Parameter("path", &TThis::Path)
        .Default();
    registrar.Parameter("profiling_name", &TThis::ProfilingName);

    registrar.Postprocessor([] (TThis* config) {
        if (config->Source == EUserJobSensorSource::Statistics && !config->Path) {
            THROW_ERROR_EXCEPTION("Parameter \"path\" is required for sensor with %lv source",
                config->Source);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TUserJobMonitoringConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("sensors", &TThis::Sensors)
        .Default();
}

const THashMap<TString, TUserJobSensorPtr>& TUserJobMonitoringConfig::GetDefaultSensors()
{
    static const auto DefaultSensors = ConvertTo<THashMap<TString, TUserJobSensorPtr>>(BuildYsonStringFluently()
        .BeginMap()
            .Item("cpu/user").BeginMap()
                .Item("path").Value("/user_job/cpu/user")
                .Item("type").Value("counter")
                .Item("source").Value("statistics")
                .Item("profiling_name").Value("/user_job/cpu/user")
            .EndMap()
            .Item("cpu/system").BeginMap()
                .Item("path").Value("/user_job/cpu/system")
                .Item("type").Value("counter")
                .Item("source").Value("statistics")
                .Item("profiling_name").Value("/user_job/cpu/system")
            .EndMap()
            .Item("cpu/wait").BeginMap()
                .Item("path").Value("/user_job/cpu/wait")
                .Item("type").Value("counter")
                .Item("source").Value("statistics")
                .Item("profiling_name").Value("/user_job/cpu/wait")
            .EndMap()
            .Item("cpu/throttled").BeginMap()
                .Item("path").Value("/user_job/cpu/throttled")
                .Item("type").Value("counter")
                .Item("source").Value("statistics")
                .Item("profiling_name").Value("/user_job/cpu/throttled")
            .EndMap()
            .Item("cpu/context_switches").BeginMap()
                .Item("path").Value("/user_job/cpu/context_switches")
                .Item("type").Value("counter")
                .Item("source").Value("statistics")
                .Item("profiling_name").Value("/user_job/cpu/context_switches")
            .EndMap()

            .Item("current_memory/rss").BeginMap()
                .Item("path").Value("/user_job/current_memory/rss")
                .Item("type").Value("gauge")
                .Item("source").Value("statistics")
                .Item("profiling_name").Value("/user_job/current_memory/rss")
            .EndMap()
            .Item("current_memory/mapped_file").BeginMap()
                .Item("path").Value("/user_job/current_memory/mapped_file")
                .Item("type").Value("gauge")
                .Item("source").Value("statistics")
                .Item("profiling_name").Value("/user_job/current_memory/mapped_file")
            .EndMap()
            .Item("current_memory/major_page_faults").BeginMap()
                .Item("path").Value("/user_job/current_memory/major_page_faults")
                .Item("type").Value("gauge")
                .Item("source").Value("statistics")
                .Item("profiling_name").Value("/user_job/current_memory/major_page_faults")
            .EndMap()
            .Item("tmpfs_size").BeginMap()
                .Item("path").Value("/user_job/tmpfs_size")
                .Item("type").Value("gauge")
                .Item("source").Value("statistics")
                .Item("profiling_name").Value("/user_job/tmpfs_size")
            .EndMap()
            .Item("disk/usage").BeginMap()
                .Item("path").Value("/user_job/disk/usage")
                .Item("type").Value("gauge")
                .Item("source").Value("statistics")
                .Item("profiling_name").Value("/user_job/disk/usage")
            .EndMap()
            .Item("disk/limit").BeginMap()
                .Item("path").Value("/user_job/disk/limit")
                .Item("type").Value("gauge")
                .Item("source").Value("statistics")
                .Item("profiling_name").Value("/user_job/disk/limit")
            .EndMap()

            .Item("network/rx_bytes").BeginMap()
                .Item("path").Value("/user_job/network/rx_bytes")
                .Item("type").Value("counter")
                .Item("source").Value("statistics")
                .Item("profiling_name").Value("/user_job/network/rx_bytes")
            .EndMap()
            .Item("network/tx_bytes").BeginMap()
                .Item("path").Value("/user_job/network/tx_bytes")
                .Item("type").Value("counter")
                .Item("source").Value("statistics")
                .Item("profiling_name").Value("/user_job/network/tx_bytes")
            .EndMap()

            .Item("gpu/utilization_gpu").BeginMap()
                .Item("type").Value("gauge")
                .Item("source").Value("gpu")
                .Item("profiling_name").Value("/user_job/gpu/utilization_gpu")
            .EndMap()
            .Item("gpu/utilization_memory").BeginMap()
                .Item("type").Value("gauge")
                .Item("source").Value("gpu")
                .Item("profiling_name").Value("/user_job/gpu/utilization_memory")
            .EndMap()
            .Item("gpu/utilization_power").BeginMap()
                .Item("type").Value("gauge")
                .Item("source").Value("gpu")
                .Item("profiling_name").Value("/user_job/gpu/utilization_power")
            .EndMap()
            .Item("gpu/utilization_clock_sm").BeginMap()
                .Item("type").Value("gauge")
                .Item("source").Value("gpu")
                .Item("profiling_name").Value("/user_job/gpu/utilization_clock_sm")
            .EndMap()
            .Item("gpu/sm_utilization").BeginMap()
                .Item("type").Value("gauge")
                .Item("source").Value("gpu")
                .Item("profiling_name").Value("/user_job/gpu/sm_utilization")
            .EndMap()
            .Item("gpu/sm_occupancy").BeginMap()
                .Item("type").Value("gauge")
                .Item("source").Value("gpu")
                .Item("profiling_name").Value("/user_job/gpu/sm_occupancy")
            .EndMap()
            .Item("gpu/memory").BeginMap()
                .Item("type").Value("gauge")
                .Item("source").Value("gpu")
                .Item("profiling_name").Value("/user_job/gpu/memory")
            .EndMap()
            .Item("gpu/power").BeginMap()
                .Item("type").Value("gauge")
                .Item("source").Value("gpu")
                .Item("profiling_name").Value("/user_job/gpu/power")
            .EndMap()
            .Item("gpu/clock_sm").BeginMap()
                .Item("type").Value("gauge")
                .Item("source").Value("gpu")
                .Item("profiling_name").Value("/user_job/gpu/clock_sm")
            .EndMap()
            .Item("gpu/stuck").BeginMap()
                .Item("type").Value("gauge")
                .Item("source").Value("gpu")
                .Item("profiling_name").Value("/user_job/gpu/stuck")
            .EndMap()
        .EndMap());

    return DefaultSensors;
}

////////////////////////////////////////////////////////////////////////////////

void TUserJobMonitoringDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("sensors", &TThis::Sensors)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TExecNodeConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("slot_manager", &TThis::SlotManager)
        .DefaultNew();
    registrar.Parameter("job_controller", &TThis::JobController)
        .DefaultNew();
    registrar.Parameter("job_reporter", &TThis::JobReporter)
        .Alias("statistics_reporter")
        .DefaultNew();

    registrar.Parameter("controller_agent_connector", &TThis::ControllerAgentConnector)
        .DefaultNew();

    registrar.Parameter("job_proxy_logging", &TThis::JobProxyLogging)
        .DefaultNew();
    registrar.Parameter("job_proxy_jaeger", &TThis::JobProxyJaeger)
        .DefaultNew();
    registrar.Parameter("job_proxy_stderr_path", &TThis::JobProxyStderrPath)
        .Default();

    registrar.Parameter("executor_stderr_path", &TThis::ExecutorStderrPath)
        .Default();

    registrar.Parameter("supervisor_rpc_timeout", &TThis::SupervisorRpcTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("job_prober_rpc_timeout", &TThis::JobProberRpcTimeout)
        .Default(TDuration::Seconds(300));

    registrar.Parameter("job_proxy_heartbeat_period", &TThis::JobProxyHeartbeatPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("job_proxy_send_heartbeat_before_abort", &TThis::JobProxySendHeartbeatBeforeAbort)
        .Default(false);

    registrar.Parameter("job_proxy_dns_over_rpc_resolver", &TThis::JobProxyDnsOverRpcResolver)
        .DefaultNew();

    registrar.Parameter("test_root_fs", &TThis::TestRootFS)
        .Default(false);

    registrar.Parameter("enable_artifact_copy_tracking", &TThis::EnableArtifactCopyTracking)
        .Default(false);

    registrar.Parameter("use_common_root_fs_quota", &TThis::UseCommonRootFSQuota)
        .Default(false);

    registrar.Parameter("use_artifact_binds", &TThis::UseArtifactBinds)
        .Default(false);

    registrar.Parameter("root_fs_binds", &TThis::RootFSBinds)
        .Default();

    registrar.Parameter("node_directory_prepare_retry_count", &TThis::NodeDirectoryPrepareRetryCount)
        .Default(10);
    registrar.Parameter("node_directory_prepare_backoff_time", &TThis::NodeDirectoryPrepareBackoffTime)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("job_proxy_preparation_timeout", &TThis::JobProxyPreparationTimeout)
        .Default(TDuration::Minutes(3));

    registrar.Parameter("min_required_disk_space", &TThis::MinRequiredDiskSpace)
        .Default(100_MB);
    registrar.Parameter("waiting_for_job_cleanup_timeout", &TThis::WaitingForJobCleanupTimeout)
        .Default(TDuration::Minutes(15));

    registrar.Parameter("job_prepare_time_limit", &TThis::JobPrepareTimeLimit)
        .Default();

    registrar.Parameter("test_job_error_truncation", &TThis::TestJobErrorTruncation)
        .Default(false);

    registrar.Parameter("core_watcher", &TThis::CoreWatcher)
        .DefaultNew();

    registrar.Parameter("user_job_container_creation_throttler", &TThis::UserJobContainerCreationThrottler)
        .DefaultNew();

    registrar.Parameter("test_poll_job_shell", &TThis::TestPollJobShell)
        .Default(false);

    registrar.Parameter("do_not_set_user_id", &TThis::DoNotSetUserId)
        .Default(false);

    registrar.Parameter("memory_tracker_cache_period", &TThis::MemoryTrackerCachePeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("smaps_memory_tracker_cache_period", &TThis::SMapsMemoryTrackerCachePeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("check_user_job_memory_limit", &TThis::CheckUserJobMemoryLimit)
        .Default(true);

    registrar.Parameter("always_abort_on_memory_reserve_overdraft", &TThis::AlwaysAbortOnMemoryReserveOverdraft)
        .Default(false);

    registrar.Parameter("user_job_monitoring", &TThis::UserJobMonitoring)
        .DefaultNew();

    registrar.Parameter("job_proxy_authentication_manager", &TThis::JobProxyAuthenticationManager)
        .DefaultNew();

    registrar.Parameter("job_proxy_solomon_exporter", &TThis::JobProxySolomonExporter)
        .DefaultNew();
    registrar.Parameter("sensor_dump_timeout", &TThis::SensorDumpTimeout)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("nbd_server", &TThis::NbdServerConfig)
        .Default();

    registrar.Preprocessor([] (TThis* config) {
        // 10 user jobs containers per second by default.
        config->UserJobContainerCreationThrottler->Limit = 10;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TMasterConnectorDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("heartbeat_timeout", &TThis::HeartbeatTimeout)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

void TSlotManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("disable_jobs_on_gpu_check_failure", &TThis::DisableJobsOnGpuCheckFailure)
        .Default();

    registrar.Parameter("check_disk_space_limit", &TThis::CheckDiskSpaceLimit)
        .Default(true);

    registrar.Parameter("idle_cpu_fraction", &TThis::IdleCpuFraction)
        .Default();

    registrar.Parameter("enable_numa_node_scheduling", &TThis::EnableNumaNodeScheduling)
        .Default(false);

    registrar.Parameter("enable_job_environment_resurrection", &TThis::EnableJobEnvironmentResurrection)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_async_layer_removal", &TThis::EnableAsyncLayerRemoval)
        .Default(true);

    registrar.Parameter("delay_after_layer_imported", &TThis::DelayAfterLayerImported)
        .Default();
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

    registrar.Parameter("job_controller", &TThis::JobController)
        .DefaultNew();

    registrar.Parameter("job_reporter", &TThis::JobReporter)
        .DefaultNew();

    registrar.Parameter("scheduler_connector", &TThis::SchedulerConnector)
        .DefaultNew();

    registrar.Parameter("controller_agent_connector", &TThis::ControllerAgentConnector)
        .Default();

    registrar.Parameter("job_proxy_preparation_timeout", &TThis::JobProxyPreparationTimeout)
        .Default(TDuration::Minutes(3));

    registrar.Parameter("waiting_for_job_cleanup_timeout", &TThis::WaitingForJobCleanupTimeout)
        .Default();
    registrar.Parameter("slot_release_timeout", &TThis::SlotReleaseTimeout)
        .Default(TDuration::Minutes(20));

    registrar.Parameter("abort_on_free_volume_synchronization_failed", &TThis::AbortOnFreeVolumeSynchronizationFailed)
        .Default(true);

    registrar.Parameter("abort_on_free_slot_synchronization_failed", &TThis::AbortOnFreeSlotSynchronizationFailed)
        .Default(true);

    registrar.Parameter("abort_on_operation_with_volume_failed", &TThis::AbortOnOperationWithVolumeFailed)
        .Default(true);
    registrar.Parameter("abort_on_operation_with_layer_failed", &TThis::AbortOnOperationWithLayerFailed)
        .Default(true);

    registrar.Parameter("abort_on_jobs_disabled", &TThis::AbortOnJobsDisabled)
        .Default(false);

    registrar.Parameter("treat_job_proxy_failure_as_abort", &TThis::TreatJobProxyFailureAsAbort)
        .Default(false);

    registrar.Parameter("user_job_monitoring", &TThis::UserJobMonitoring)
        .DefaultNew();

    registrar.Parameter("job_throttler", &TThis::JobThrottler)
        .DefaultNew();

    registrar.Parameter("user_job_container_creation_throttler", &TThis::UserJobContainerCreationThrottler)
        .DefaultNew();

    registrar.Parameter("statistics_output_table_count_limit", &TThis::StatisticsOutputTableCountLimit)
        .Default();

    registrar.Preprocessor([] (TThis* config) {
        // 10 user jobs containers per second by default.
        config->UserJobContainerCreationThrottler->Limit = 10;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
