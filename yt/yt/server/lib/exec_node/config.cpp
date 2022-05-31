#include "config.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NExecNode {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TJobEnvironmentConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("type", &TThis::Type)
        .Default(EJobEnvironmentType::Simple);

    registrar.Parameter("start_uid", &TThis::StartUid)
        .Default(10000);

    registrar.Parameter("memory_watchdog_period", &TThis::MemoryWatchdogPeriod)
        .Default(TDuration::Seconds(1));
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

    registrar.Parameter("container_destruction_backoff", &TThis::ContainerDestructionBackoff)
        .Default(TDuration::Seconds(60));
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
}

////////////////////////////////////////////////////////////////////////////////

void THeartbeatReporterDynamicConfigBase::Register(TRegistrar registrar)
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

void TControllerAgentConnectorDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("test_heartbeat_delay", &TThis::TestHeartbeatDelay)
        .Default();
    registrar.Parameter("statistics_throttler", &TThis::StatisticsThrottler)
        .Default();
    registrar.Parameter("running_job_sending_backoff", &TThis::RunningJobInfoSendingBackoff)
        .Default();
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

void THeartbeatReporterConfigBase::ApplyDynamicInplace(const THeartbeatReporterDynamicConfigBase& dynamicConfig)
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

TSchedulerConnectorConfigPtr TSchedulerConnectorConfig::ApplyDynamic(const TSchedulerConnectorDynamicConfigPtr& dynamicConfig)
{
    YT_VERIFY(dynamicConfig);

    auto newConfig = CloneYsonSerializable(MakeStrong(this));
    newConfig->ApplyDynamicInplace(*dynamicConfig);

    return newConfig;
}

void TSchedulerConnectorConfig::ApplyDynamicInplace(const TSchedulerConnectorDynamicConfig& dynamicConfig)
{
    THeartbeatReporterConfigBase::ApplyDynamicInplace(dynamicConfig);
    Postprocess();
}

////////////////////////////////////////////////////////////////////////////////

void TControllerAgentConnectorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("statistics_throttler", &TThis::StatisticsThrottler)
        .DefaultNew(1_MB);
    registrar.Parameter("running_job_sending_backoff", &TThis::RunningJobInfoSendingBackoff)
        .Default(TDuration::Seconds(30));
}

TControllerAgentConnectorConfigPtr TControllerAgentConnectorConfig::ApplyDynamic(const TControllerAgentConnectorDynamicConfigPtr& dynamicConfig)
{
    YT_VERIFY(dynamicConfig);

    auto newConfig = CloneYsonSerializable(MakeStrong(this));
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
    RunningJobInfoSendingBackoff = dynamicConfig.RunningJobInfoSendingBackoff.value_or(RunningJobInfoSendingBackoff);
    Postprocess();
}

////////////////////////////////////////////////////////////////////////////////

void TMasterConnectorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("heartbeat_period", &TThis::HeartbeatPeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("heartbeat_period_splay", &TThis::HeartbeatPeriodSplay)
        .Default(TDuration::Seconds(1));
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
    registrar.Parameter("scheduler_connector", &TThis::SchedulerConnector)
        .DefaultNew();

    registrar.Parameter("job_proxy_logging", &TThis::JobProxyLogging)
        .DefaultNew();
    registrar.Parameter("job_proxy_jaeger", &TThis::JobProxyJaeger)
        .DefaultNew();
    registrar.Parameter("job_proxy_stderr_path", &TThis::JobProxyStderrPath)
        .Default();

    registrar.Parameter("supervisor_rpc_timeout", &TThis::SupervisorRpcTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("job_prober_rpc_timeout", &TThis::JobProberRpcTimeout)
        .Default(TDuration::Seconds(300));

    registrar.Parameter("job_proxy_heartbeat_period", &TThis::JobProxyHeartbeatPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("job_proxy_upload_debug_artifact_chunks", &TThis::JobProxyUploadDebugArtifactChunks)
        .Default(false);

    registrar.Parameter("test_root_fs", &TThis::TestRootFS)
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
    registrar.Parameter("job_abortion_timeout", &TThis::JobAbortionTimeout)
        .Default(TDuration::Minutes(15));

    registrar.Parameter("test_job_error_truncation", &TThis::TestJobErrorTruncation)
        .Default(false);

    registrar.Parameter("core_watcher", &TThis::CoreWatcher)
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

    registrar.Parameter("master_connector", &TThis::MasterConnector)
        .DefaultNew();

    registrar.Parameter("job_proxy_solomon_exporter", &TThis::JobProxySolomonExporter)
        .DefaultNew();
    registrar.Parameter("sensor_dump_timeout", &TThis::SensorDumpTimeout)
        .Default(TDuration::Seconds(5));
}

////////////////////////////////////////////////////////////////////////////////

void TMasterConnectorDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("heartbeat_period", &TThis::HeartbeatPeriod)
        .Default();
    registrar.Parameter("heartbeat_period_splay", &TThis::HeartbeatPeriodSplay)
        .Default();
    registrar.Parameter("heartbeat_timeout", &TThis::HeartbeatTimeout)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

void TSlotManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("disable_jobs_on_gpu_check_failure", &TThis::DisableJobsOnGpuCheckFailure)
        .Default();

    registrar.Parameter("check_disk_space_limit", &TThis::CheckDiskSpaceLimit)
        .Default(false);

    registrar.Parameter("idle_cpu_fraction", &TThis::IdleCpuFraction)
        .Default();
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
        .Default();

    registrar.Parameter("controller_agent_connector", &TThis::ControllerAgentConnector)
        .Default();

    registrar.Parameter("abort_on_jobs_disabled", &TThis::AbortOnJobsDisabled)
        .Default(false);

    registrar.Parameter("treat_job_proxy_failure_as_abort", &TThis::TreatJobProxyFailureAsAbort)
        .Default(false);

    registrar.Parameter("user_job_monitoring", &TThis::UserJobMonitoring)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
