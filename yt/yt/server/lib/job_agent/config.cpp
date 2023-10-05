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
    registrar.Parameter("reincarnation_slots", &TThis::ReincarnationSlots)
        .GreaterThanOrEqual(0)
        .Default(2);
}

////////////////////////////////////////////////////////////////////////////////

void TGpuInfoSourceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("type", &TThis::Type)
        .Default(EGpuInfoSourceType::NvidiaSmi);
    registrar.Parameter("nv_gpu_manager_service_address", &TThis::NvGpuManagerServiceAddress)
        .Default("unix:/var/run/nvgpu-manager.sock");
    registrar.Parameter("nv_gpu_manager_service_name", &TThis::NvGpuManagerServiceName)
        .Default("nvgpu.NvGpuManager");
    registrar.Parameter("nv_gpu_manager_devices_cgroup_path", &TThis::NvGpuManagerDevicesCgroupPath)
        .Default();
    registrar.Parameter("gpu_indexes_from_nvidia_smi", &TThis::GpuIndexesFromNvidiaSmi)
        .Default(true);
}

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
    registrar.Parameter("driver_layer_fetch_splay", &TThis::DriverLayerFetchPeriodSplay)
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

    registrar.Parameter("gpu_info_source", &TThis::GpuInfoSource)
        .DefaultNew();

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

    registrar.Parameter("job_setup_command", &TThis::JobSetupCommand)
        .Default();

    registrar.Parameter("driver_layer_fetch_period", &TThis::DriverLayerFetchPeriod)
        .Default();
    registrar.Parameter("cuda_toolkit_min_driver_version", &TThis::CudaToolkitMinDriverVersion)
        .Alias("toolkit_min_driver_version")
        .Default();

    registrar.Parameter("gpu_info_source", &TThis::GpuInfoSource)
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

void TMemoryPressureDetectorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(false);
    registrar.Parameter("check_period", &TThis::CheckPeriod)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("major_page_fault_count_threshold", &TThis::MajorPageFaultCountThreshold)
        .Default(500);
    registrar.Parameter("memory_watermark_multiplier_increase_step", &TThis::MemoryWatermarkMultiplierIncreaseStep)
        .Default(0.1);
    registrar.Parameter("max_memory_watermark_multiplier", &TThis::MaxMemoryWatermarkMultiplier)
        .GreaterThan(1)
        .Default(2);
}

////////////////////////////////////////////////////////////////////////////////

void TJobControllerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("get_job_specs_timeout", &TThis::GetJobSpecsTimeout)
        .Default();

    registrar.Parameter("cpu_overdraft_timeout", &TThis::CpuOverdraftTimeout)
        .Default();

    registrar.Parameter("cpu_to_vcpu_factor", &TThis::CpuToVCpuFactor)
        .Default();

    registrar.Parameter("enable_cpu_to_vcpu_factor", &TThis::EnableCpuToVCpuFactor)
        .Default(false);

    registrar.Parameter("account_master_memory_request", &TThis::AccountMasterMemoryRequest)
        .Default(true);

    registrar.Parameter("cpu_model_to_cpu_to_vcpu_factor", &TThis::CpuModelToCpuToVCpuFactor)
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

    registrar.Parameter("memory_pressure_detector", &TThis::MemoryPressureDetector)
        .DefaultNew();

    registrar.Parameter("operation_infos_request_period", &TThis::OperationInfosRequestPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("unknown_operation_jobs_removal_delay", &TThis::UnknownOperationJobsRemovalDelay)
        .Default();

    registrar.Parameter("disabled_jobs_interruption_timeout", &TThis::DisabledJobsInterruptionTimeout)
        .Default(TDuration::Minutes(1))
        .GreaterThan(TDuration::Zero());

    registrar.Postprocessor([] (TThis* config) {
        if (config->CpuToVCpuFactor && *config->CpuToVCpuFactor <= 0) {
            THROW_ERROR_EXCEPTION("`cpu_to_vcpu_factor` must be greater than 0")
                << TErrorAttribute("cpu_to_vcpu_factor", *config->CpuToVCpuFactor);
        }
        if (config->CpuModelToCpuToVCpuFactor) {
            for (const auto& [cpu_model, factor] : config->CpuModelToCpuToVCpuFactor.value()) {
                if (factor <= 0) {
                    THROW_ERROR_EXCEPTION("Factor in \"cpu_model_to_cpu_to_vcpu_factor\" must be greater than 0")
                        << TErrorAttribute("cpu_model", cpu_model)
                        << TErrorAttribute("cpu_to_vcpu_factor", factor);
                }
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TJobControllerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("resource_limits", &TThis::ResourceLimits)
        .DefaultNew();

    // Make it greater than interrupt preemption timeout.
    registrar.Parameter("waiting_jobs_timeout", &TThis::WaitingJobsTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("get_job_specs_timeout", &TThis::GetJobSpecsTimeout)
        .Default(TDuration::Seconds(5));

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

    registrar.Parameter("cpu_to_vcpu_factor", &TThis::CpuToVCpuFactor)
        .Default();

    registrar.Parameter("cpu_model", &TThis::CpuModel)
        .Default();

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

    registrar.Parameter("unknown_operation_jobs_removal_delay", &TThis::UnknownOperationJobsRemovalDelay)
        .Default(TDuration::Minutes(1));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NJobAgent
