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

void TJobResourceManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("resource_limits", &TThis::ResourceLimits)
        .DefaultNew();

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
}

////////////////////////////////////////////////////////////////////////////////

void TJobResourceManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cpu_to_vcpu_factor", &TThis::CpuToVCpuFactor)
        .Default();

    registrar.Parameter("enable_cpu_to_vcpu_factor", &TThis::EnableCpuToVCpuFactor)
        .Default(false);

    registrar.Parameter("cpu_model_to_cpu_to_vcpu_factor", &TThis::CpuModelToCpuToVCpuFactor)
        .Default();

    registrar.Parameter("profiling_period", &TThis::ProfilingPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("memory_pressure_detector", &TThis::MemoryPressureDetector)
        .DefaultNew();

    registrar.Parameter("mapped_memory_controller", &TThis::MappedMemoryController)
        .Default();

    registrar.Parameter("free_memory_watermark", &TThis::FreeMemoryWatermark)
        .Default(0)
        .GreaterThanOrEqual(0);

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

} // namespace NY::NJobAgent
