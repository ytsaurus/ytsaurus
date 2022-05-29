#include "config.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NExecNode {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

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

} // namespace NYT::NExecNode
