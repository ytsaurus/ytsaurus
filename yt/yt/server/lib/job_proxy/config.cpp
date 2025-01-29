#include "config.h"

#include <yt/yt/library/profiling/solomon/config.h>

#include <yt/yt/library/stockpile/config.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

void TJobProxyTestingConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("fail_on_job_proxy_spawned_call", &TThis::FailOnJobProxySpawnedCall)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TJobThrottlerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("min_backoff_time", &TThis::MinBackoffTime)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("max_backoff_time", &TThis::MaxBackoffTime)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("backoff_multiplier", &TThis::BackoffMultiplier)
        .Default(1.5);

    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("bandwidth_prefetch", &TThis::BandwidthPrefetch)
        .DefaultNew();

    registrar.Parameter("rps_prefetch", &TThis::RpsPrefetch)
        .DefaultNew();

    registrar.Preprocessor([] (TThis* config) {
        config->BandwidthPrefetch->MaxPrefetchAmount = 16_MB;
        config->RpsPrefetch->MaxPrefetchAmount = 20;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TCoreWatcherConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("period", &TThis::Period)
        .Default(TDuration::Seconds(5))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("io_timeout", &TThis::IOTimeout)
        .Default(TDuration::Seconds(60))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("finalization_timeout", &TThis::FinalizationTimeout)
        .Default(TDuration::Seconds(60))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("cores_processing_timeout", &TThis::CoresProcessingTimeout)
        .Default(TDuration::Minutes(15))
        .GreaterThan(TDuration::Zero());
}

////////////////////////////////////////////////////////////////////////////////

void TUserJobNetworkAddress::Register(TRegistrar registrar)
{
    registrar.Parameter("address", &TThis::Address)
        .Default();

    registrar.Parameter("name", &TThis::Name)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TTmpfsManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("tmpfs_paths", &TThis::TmpfsPaths)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TMemoryTrackerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("include_memory_mapped_files", &TThis::IncludeMemoryMappedFiles)
        .Default(true);

    registrar.Parameter("use_smaps_memory_tracker", &TThis::UseSMapsMemoryTracker)
        .Default(false);

    registrar.Parameter("memory_statistics_cache_period", &TThis::MemoryStatisticsCachePeriod)
        .Default(TDuration::Zero());
}

////////////////////////////////////////////////////////////////////////////////

void TBindConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("external_path", &TThis::ExternalPath);

    registrar.Parameter("internal_path", &TThis::InternalPath);

    registrar.Parameter("read_only", &TThis::ReadOnly)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TJobTraceEventProcessorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("reporter", &TThis::Reporter)
        .DefaultNew();

    registrar.Parameter("logging_period", &TThis::LoggingInterval)
        .Default(100);
}

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

void TJobEnvironmentConfigBase::Register(TRegistrar registrar)
{
    registrar.Parameter("start_uid", &TThis::StartUid)
        .Default(10000);

    registrar.Parameter("memory_watchdog_period", &TThis::MemoryWatchdogPeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("job_thrashing_detector", &TThis::JobThrashingDetector)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TSimpleJobEnvironmentConfig::Register(TRegistrar)
{ };

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

    registrar.Parameter("container_destruction_backoff", &TThis::ContainerDestructionBackoff)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

void TCriJobEnvironmentConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cri_executor", &TThis::CriExecutor)
        .DefaultNew();

    registrar.Parameter("cri_image_cache", &TThis::CriImageCache)
        .DefaultNew();

    registrar.Parameter("job_proxy_image", &TThis::JobProxyImage)
        .NonEmpty();

    registrar.Parameter("job_proxy_bind_mounts", &TThis::JobProxyBindMounts)
        .Default();

    registrar.Parameter("use_job_proxy_from_image", &TThis::UseJobProxyFromImage);
}

////////////////////////////////////////////////////////////////////////////////

void TJobProxyInternalConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("slot_index", &TThis::SlotIndex);

    registrar.Parameter("slot_path", &TThis::SlotPath);

    registrar.Parameter("tmpfs_manager", &TThis::TmpfsManager)
        .DefaultNew();

    registrar.Parameter("memory_tracker", &TThis::MemoryTracker)
        .DefaultNew();

    registrar.Parameter("root_path", &TThis::RootPath)
        .Default();

    registrar.Parameter("stderr_path", &TThis::StderrPath)
        .Default();

    registrar.Parameter("executor_stderr_path", &TThis::ExecutorStderrPath)
        .Default();

    // COMPAT(artemagafonov): RootFS is always writable, so the flag should be removed after the update of all nodes.
    registrar.Parameter("make_rootfs_writable", &TThis::MakeRootFSWritable)
        .Default(true);

    registrar.Parameter("enable_fuse", &TThis::EnableFuse)
        .Default(false);

    registrar.Parameter("docker_image", &TThis::DockerImage)
        .Default();

    registrar.Parameter("binds", &TThis::Binds)
        .Default();

    registrar.Parameter("gpu_indexes", &TThis::GpuIndexes)
        .Default();

    registrar.Parameter("supervisor_connection", &TThis::SupervisorConnection);

    registrar.Parameter("supervisor_rpc_timeout", &TThis::SupervisorRpcTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("heartbeat_period", &TThis::HeartbeatPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("input_pipe_blinker_period", &TThis::InputPipeBlinkerPeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("job_environment", &TThis::JobEnvironment);

    registrar.Parameter("addresses", &TThis::Addresses)
        .Default();

    registrar.Parameter("local_host_name", &TThis::LocalHostName)
        .Default();

    registrar.Parameter("rack", &TThis::Rack)
        .Default();

    registrar.Parameter("data_center", &TThis::DataCenter)
        .Default();

    registrar.Parameter("ahead_memory_reserve", &TThis::AheadMemoryReserve)
        .Default(100_MB);

    registrar.Parameter("test_root_fs", &TThis::TestRootFS)
        .Default(false);

    registrar.Parameter("always_abort_on_memory_reserve_overdraft", &TThis::AlwaysAbortOnMemoryReserveOverdraft)
        .Default(false);

    registrar.Parameter("job_throttler", &TThis::JobThrottler)
        .DefaultNew();

    registrar.Parameter("host_name", &TThis::HostName)
        .Default();

    registrar.Parameter("enable_nat64", &TThis::EnableNat64)
        .Default(false);

    registrar.Parameter("disable_network", &TThis::DisableNetwork)
        .Default(false);

    registrar.Parameter("network_addresses", &TThis::NetworkAddresses)
        .Default();

    registrar.Parameter("abort_on_uncaught_exception", &TThis::AbortOnUncaughtException)
        .Default(false);

    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);

    registrar.Parameter("core_watcher", &TThis::CoreWatcher)
        .DefaultNew();

    registrar.Parameter("test_poll_job_shell", &TThis::TestPollJobShell)
        .Default(false);

    registrar.Parameter("do_not_set_user_id", &TThis::DoNotSetUserId)
        .Default(false);

    registrar.Parameter("check_user_job_memory_limit", &TThis::CheckUserJobMemoryLimit)
        .Default(true);

    registrar.Parameter("check_user_job_oom_kill", &TThis::CheckUserJobOomKill)
        .Default(true);

    registrar.Parameter("enable_job_shell_seccomp", &TThis::EnableJobShellSeccopm)
        .Default(true);

    registrar.Parameter("use_porto_kill_for_signalling", &TThis::UsePortoKillForSignalling)
        .Default(false);

    registrar.Parameter("force_idle_cpu_policy", &TThis::ForceIdleCpuPolicy)
        .Default(false);

    registrar.Parameter("send_heartbeat_before_abort", &TThis::SendHeartbeatBeforeAbort)
        .Default(false);

    registrar.Parameter("enable_stderr_and_core_live_preview", &TThis::EnableStderrAndCoreLivePreview)
        .Default(true);

    registrar.Parameter("forward_all_environment_variables", &TThis::ForwardAllEnvironmentVariables)
        .Default(false);

    registrar.Parameter("tvm_bridge_connection", &TThis::TvmBridgeConnection)
        .Default();

    registrar.Parameter("tvm_bridge", &TThis::TvmBridge)
        .Default();

    registrar.Parameter("api_service", &TThis::ApiService)
        .DefaultNew();

    registrar.Parameter("statistics_output_table_count_limit", &TThis::StatisticsOutputTableCountLimit)
        .Default();

    registrar.Parameter("dns_over_rpc_resolver", &TThis::DnsOverRpcResolver)
        .Default();

    registrar.Parameter("job_testing_options", &TThis::JobTestingOptions)
        .Default();

    registrar.Parameter("authentication_manager", &TThis::AuthenticationManager)
        .DefaultNew();

    registrar.Parameter("original_cluster_connection", &TThis::OriginalClusterConnection)
        .Default();

    registrar.Parameter("container_cpu_limit", &TThis::ContainerCpuLimit)
        .Default();

    registrar.Parameter("slot_container_memory_limit", &TThis::SlotContainerMemoryLimit)
        .Default();

    registrar.Parameter("testing_config", &TThis::TestingConfig)
        .DefaultNew();

    registrar.Parameter("use_retrying_channels", &TThis::UseRetryingChannels)
        .Default(false);

    registrar.Parameter("retrying_channel", &TThis::RetryingChannel)
        .DefaultNew();

    registrar.Parameter("enable_cuda_profile_event_streaming", &TThis::EnableCudaProfileEventStreaming)
        .Default(false);

    registrar.Parameter("job_trace_event_processor", &TThis::JobTraceEventProcessor)
        .DefaultNew();

    registrar.Parameter("operations_archive_version", &TThis::OperationsArchiveVersion)
        .Default();
    registrar.Parameter("pipe_reader_timeout_threshold", &TThis::PipeReaderTimeoutThreshold)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("enable_root_volume_disk_quota", &TThis::EnableRootVolumeDiskQuota)
        .Default(false);

    registrar.Parameter("adaptive_row_count_upper_bound", &TThis::AdaptiveRowCountUpperBound)
        .Default(std::numeric_limits<i64>::max());

    registrar.Preprocessor([] (TThis* config) {
        config->SolomonExporter->EnableSelfProfiling = false;
        config->SolomonExporter->WindowSize = 1;

        auto stockpileConfig = config->GetSingletonConfig<TStockpileConfig>();
        stockpileConfig->ThreadCount = 0;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TJobProxyDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("jaeger", &TThis::Jaeger)
        .DefaultNew();

    registrar.Parameter("enable_job_shell_seccomp", &TThis::EnableJobShellSeccopm)
        .Default(true);

    registrar.Parameter("use_porto_kill_for_signalling", &TThis::UsePortoKillForSignalling)
        .Default(false);

    registrar.Parameter("force_idle_cpu_policy", &TThis::ForceIdleCpuPolicy)
        .Default(false);

    registrar.Parameter("abort_on_uncaught_exception", &TThis::AbortOnUncaughtException)
        .Default(false);

    registrar.Parameter("enable_stderr_and_core_live_preview", &TThis::EnableStderrAndCoreLivePreview)
        .Default(true);

    registrar.Parameter("check_user_job_oom_kill", &TThis::CheckUserJobOomKill)
        .Default(true);

    registrar.Parameter("job_environment", &TThis::JobEnvironment)
        .Default();

    registrar.Parameter("testing_config", &TThis::TestingConfig)
        .DefaultNew();

    registrar.Parameter("use_retrying_channels", &TThis::UseRetryingChannels)
        .Default(false);

    registrar.Parameter("enable_cuda_profile_event_streaming", &TThis::EnableCudaProfileEventStreaming)
        .Default(false);

    registrar.Parameter("job_trace_event_processor", &TThis::JobTraceEventProcessor)
        .DefaultNew();

    registrar.Parameter("retrying_channel", &TThis::RetryingChannel)
        .DefaultCtor([] {
            auto config = New<NRpc::TRetryingChannelConfig>();
            config->RetryBackoffTime = TDuration::Seconds(1);
            config->RetryAttempts = 10;
            return config;
        });

    registrar.Parameter("pipe_reader_timeout_threshold", &TThis::PipeReaderTimeoutThreshold)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("adaptive_row_count_upper_bound", &TThis::AdaptiveRowCountUpperBound)
        .Default(std::numeric_limits<i64>::max());

    registrar.Parameter("memory_profile_dump_path", &TThis::MemoryProfileDumpPath)
        .Alias("heap_dump_directory")
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
