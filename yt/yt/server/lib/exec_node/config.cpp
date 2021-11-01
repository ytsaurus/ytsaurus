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
                .Item("type").Value("counter")
            .EndMap()
            .Item("cpu/system").BeginMap()
                .Item("type").Value("counter")
            .EndMap()
            .Item("cpu/wait").BeginMap()
                .Item("type").Value("counter")
            .EndMap()
            .Item("cpu/throttled").BeginMap()
                .Item("type").Value("counter")
            .EndMap()
            .Item("cpu/context_switches").BeginMap()
                .Item("type").Value("counter")
            .EndMap()

            .Item("current_memory/rss").BeginMap()
                .Item("type").Value("gauge")
            .EndMap()
            .Item("tmpfs_size").BeginMap()
                .Item("type").Value("gauge")
            .EndMap()

            .Item("gpu/utilization_gpu").BeginMap()
                .Item("type").Value("gauge")
            .EndMap()
            .Item("gpu/utilization_memory").BeginMap()
                .Item("type").Value("gauge")
            .EndMap()
            .Item("gpu/utilization_power").BeginMap()
                .Item("type").Value("gauge")
            .EndMap()
            .Item("gpu/utilization_clock_sm").BeginMap()
                .Item("type").Value("gauge")
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

} // namespace NYT::NExecNode
