#include "config.h"

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

void TResourceLimitsConfig::Register(TRegistrar registrar)
{
    // These are some very low default limits.
    // Override for production use.
    registrar.Parameter("user_slots", &TThis::UserSlots)
        .GreaterThanOrEqual(0)
        .Default(1);
    registrar.Parameter("cpu", &TThis::Cpu)
        .GreaterThanOrEqual(0)
        .Default(1);
    registrar.Parameter("gpu", &TThis::Gpu)
        .GreaterThanOrEqual(0)
        .Default(0);
    registrar.Parameter("network", &TThis::Network)
        .GreaterThanOrEqual(0)
        .Default(100);
    registrar.Parameter("user_memory", &TThis::UserMemory)
        .Alias("memory")
        .GreaterThanOrEqual(0)
        .Default(std::numeric_limits<i64>::max());
    registrar.Parameter("system_memory", &TThis::SystemMemory)
        .GreaterThanOrEqual(0)
        .Default(std::numeric_limits<i64>::max());
    registrar.Parameter("replication_slots", &TThis::ReplicationSlots)
        .GreaterThanOrEqual(0)
        .Default(16);
    registrar.Parameter("replication_data_size", &TThis::ReplicationDataSize)
        .Default(10_GB)
        .GreaterThanOrEqual(0);
    registrar.Parameter("merge_data_size", &TThis::MergeDataSize)
        .Default(10_GB)
        .GreaterThanOrEqual(0);
    registrar.Parameter("removal_slots", &TThis::RemovalSlots)
        .GreaterThanOrEqual(0)
        .Default(16);
    registrar.Parameter("repair_slots", &TThis::RepairSlots)
        .GreaterThanOrEqual(0)
        .Default(4);
    registrar.Parameter("repair_data_size", &TThis::RepairDataSize)
        .Default(4_GB)
        .GreaterThanOrEqual(0);
    registrar.Parameter("seal_slots", &TThis::SealSlots)
        .GreaterThanOrEqual(0)
        .Default(16);
    registrar.Parameter("merge_slots", &TThis::MergeSlots)
        .GreaterThanOrEqual(0)
        .Default(4);
    registrar.Parameter("autotomy_slots", &TThis::AutotomySlots)
        .GreaterThanOrEqual(0)
        .Default(4);
}

////////////////////////////////////////////////////////////////////////////////

void TGpuManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);

    registrar.Parameter("health_check_timeout", &TThis::HealthCheckTimeout)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("health_check_period", &TThis::HealthCheckPeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("health_check_failure_backoff", &TThis::HealthCheckFailureBackoff)
        .Default(TDuration::Minutes(10));

    registrar.Parameter("job_setup_command", &TThis::JobSetupCommand)
        .Default();

    registrar.Parameter("driver_layer_directory_path", &TThis::DriverLayerDirectoryPath)
        .Default();
    registrar.Parameter("driver_version", &TThis::DriverVersion)
        .Default();
    registrar.Parameter("driver_layer_fetch_period", &TThis::DriverLayerFetchPeriod)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("cuda_toolkit_min_driver_version", &TThis::CudaToolkitMinDriverVersion)
        .Alias("toolkit_min_driver_version")
        .Default();

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

void TGpuManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("health_check_timeout", &TThis::HealthCheckTimeout)
        .Default();
    registrar.Parameter("health_check_period", &TThis::HealthCheckPeriod)
        .Default();
    registrar.Parameter("health_check_failure_backoff", &TThis::HealthCheckFailureBackoff)
        .Default();

    registrar.Parameter("driver_layer_fetch_period", &TThis::DriverLayerFetchPeriod)
        .Default();
    registrar.Parameter("cuda_toolkit_min_driver_version", &TThis::CudaToolkitMinDriverVersion)
        .Alias("toolkit_min_driver_version")
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

void TMappedMemoryControllerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("check_period", &TThis::CheckPeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("reserved_memory", &TThis::ReservedMemory)
        .Default(10_GB);
}

////////////////////////////////////////////////////////////////////////////////

void TJobControllerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("get_job_specs_timeout", &TThis::GetJobSpecsTimeout)
        .Default();

    registrar.Parameter("total_confirmation_period", &TThis::TotalConfirmationPeriod)
        .Default();

    registrar.Parameter("cpu_overdraft_timeout", &TThis::CpuOverdraftTimeout)
        .Default();

    registrar.Parameter("memory_overdraft_timeout", &TThis::MemoryOverdraftTimeout)
        .Default();

    registrar.Parameter("profiling_period", &TThis::ProfilingPeriod)
        .Default();

    registrar.Parameter("resource_adjustment_period", &TThis::ResourceAdjustmentPeriod)
        .Default();

    registrar.Parameter("recently_removed_jobs_clean_period", &TThis::RecentlyRemovedJobsCleanPeriod)
        .Default();

    registrar.Parameter("recently_removed_jobs_store_timeout", &TThis::RecentlyRemovedJobsStoreTimeout)
        .Default();

    registrar.Parameter("gpu_manager", &TThis::GpuManager)
        .DefaultNew();

    registrar.Parameter("job_proxy_build_info_update_period", &TThis::JobProxyBuildInfoUpdatePeriod)
        .Default();

    registrar.Parameter("disable_job_proxy_profiling", &TThis::DisableJobProxyProfiling)
        .Default();

    registrar.Parameter("job_proxy", &TThis::JobProxy)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TJobControllerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("resource_limits", &TThis::ResourceLimits)
        .DefaultNew();
    registrar.Parameter("statistics_throttler", &TThis::StatisticsThrottler)
        .DefaultNew();

    // Make it greater than interrupt preemption timeout.
    registrar.Parameter("waiting_jobs_timeout", &TThis::WaitingJobsTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("get_job_specs_timeout", &TThis::GetJobSpecsTimeout)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("total_confirmation_period", &TThis::TotalConfirmationPeriod)
        .Default(TDuration::Minutes(10));

    registrar.Parameter("memory_overdraft_timeout", &TThis::MemoryOverdraftTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("cpu_overdraft_timeout", &TThis::CpuOverdraftTimeout)
        .Default(TDuration::Minutes(10));

    registrar.Parameter("profiling_period", &TThis::ProfilingPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("resource_adjustment_period", &TThis::ResourceAdjustmentPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("recently_removed_jobs_clean_period", &TThis::RecentlyRemovedJobsCleanPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("recently_removed_jobs_store_timeout", &TThis::RecentlyRemovedJobsStoreTimeout)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("cpu_per_tablet_slot", &TThis::CpuPerTabletSlot)
        .Default(1.0);

    registrar.Parameter("start_port", &TThis::StartPort)
        .Default(20000);

    registrar.Parameter("port_count", &TThis::PortCount)
        .Default(10000);

    registrar.Parameter("port_set", &TThis::PortSet)
        .Default();

    registrar.Parameter("gpu_manager", &TThis::GpuManager)
        .DefaultNew();

    registrar.Parameter("mapped_memory_controller", &TThis::MappedMemoryController)
        .Default();

    registrar.Parameter("free_memory_watermark", &TThis::FreeMemoryWatermark)
        .Default(0)
        .GreaterThanOrEqual(0);

    registrar.Parameter("job_setup_command", &TThis::JobSetupCommand)
        .Default();

    registrar.Parameter("setup_command_user", &TThis::SetupCommandUser)
        .Default("root");

    registrar.Parameter("job_proxy_build_info_update_period", &TThis::JobProxyBuildInfoUpdatePeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("disable_job_proxy_profiling", &TThis::DisableJobProxyProfiling)
        .Default(false);

    registrar.Preprocessor([] (TThis* config) {
        // 100 kB/sec * 1000 [nodes] = 100 MB/sec that corresponds to
        // approximate incoming bandwidth of 1Gbit/sec of the scheduler.
        config->StatisticsThrottler->Limit = 100_KB;
    });
}

////////////////////////////////////////////////////////////////////////////////
    
void TJobReporterDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_job_reporter", &TThis::EnableJobReporter)
        .Default();
    registrar.Parameter("enable_job_spec_reporter", &TThis::EnableJobSpecReporter)
        .Default();
    registrar.Parameter("enable_job_stderr_reporter", &TThis::EnableJobStderrReporter)
        .Default();
    registrar.Parameter("enable_job_profile_reporter", &TThis::EnableJobProfileReporter)
        .Default();
    registrar.Parameter("enable_job_fail_context_reporter", &TThis::EnableJobFailContextReporter)
        .Default();
}

void TJobReporterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("job_handler", &TThis::JobHandler)
        .DefaultNew();
    registrar.Parameter("operation_id_handler", &TThis::OperationIdHandler)
        .DefaultNew();
    registrar.Parameter("job_spec_handler", &TThis::JobSpecHandler)
        .DefaultNew();
    registrar.Parameter("job_stderr_handler", &TThis::JobStderrHandler)
        .DefaultNew();
    registrar.Parameter("job_fail_context_handler", &TThis::JobFailContextHandler)
        .DefaultNew();
    registrar.Parameter("job_profile_handler", &TThis::JobProfileHandler)
        .DefaultNew();

    registrar.Parameter("user", &TThis::User)
        .Default(NRpc::RootUserName);
    registrar.Parameter("report_statistics_lz4", &TThis::ReportStatisticsLz4)
        .Default(false);

    registrar.Parameter("max_in_progress_job_data_size", &TThis::MaxInProgressJobDataSize)
        .Default();
    registrar.Parameter("max_in_progress_operation_id_data_size", &TThis::MaxInProgressOperationIdDataSize)
        .Default();
    registrar.Parameter("max_in_progress_job_spec_data_size", &TThis::MaxInProgressJobSpecDataSize)
        .Default();
    registrar.Parameter("max_in_progress_job_stderr_data_size", &TThis::MaxInProgressJobStderrDataSize)
        .Default();
    registrar.Parameter("max_in_progress_job_fail_context_data_size", &TThis::MaxInProgressJobFailContextDataSize)
        .Default();
        
    registrar.Parameter("enable_job_reporter", &TThis::EnableJobReporter)
        .Default(true);
    registrar.Parameter("enable_job_spec_reporter", &TThis::EnableJobSpecReporter)
        .Default(true);
    registrar.Parameter("enable_job_stderr_reporter", &TThis::EnableJobStderrReporter)
        .Default(true);
    registrar.Parameter("enable_job_profile_reporter", &TThis::EnableJobProfileReporter)
        .Default(true);
    registrar.Parameter("enable_job_fail_context_reporter", &TThis::EnableJobFailContextReporter)
        .Default(true);

    registrar.Preprocessor([] (TThis* config) {
        config->OperationIdHandler->MaxInProgressDataSize = 10_MB;

        config->JobHandler->Path = NScheduler::GetOperationsArchiveJobsPath();
        config->OperationIdHandler->Path = NScheduler::GetOperationsArchiveOperationIdsPath();
        config->JobSpecHandler->Path = NScheduler::GetOperationsArchiveJobSpecsPath();
        config->JobStderrHandler->Path = NScheduler::GetOperationsArchiveJobStderrsPath();
        config->JobFailContextHandler->Path = NScheduler::GetOperationsArchiveJobFailContextsPath();
        config->JobProfileHandler->Path = NScheduler::GetOperationsArchiveJobProfilesPath();
    });

    registrar.Postprocessor([] (TThis* config) {
        if (config->MaxInProgressJobDataSize) {
            config->JobHandler->MaxInProgressDataSize = *config->MaxInProgressJobDataSize;
        }
        if (config->MaxInProgressOperationIdDataSize) {
            config->OperationIdHandler->MaxInProgressDataSize = *config->MaxInProgressOperationIdDataSize;
        }
        if (config->MaxInProgressJobSpecDataSize) {
            config->JobSpecHandler->MaxInProgressDataSize = *config->MaxInProgressJobSpecDataSize;
        }
        if (config->MaxInProgressJobStderrDataSize) {
            config->JobStderrHandler->MaxInProgressDataSize = *config->MaxInProgressJobStderrDataSize;
        }
        if (config->MaxInProgressJobFailContextDataSize) {
            config->JobFailContextHandler->MaxInProgressDataSize = *config->MaxInProgressJobFailContextDataSize;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NJobAgent
