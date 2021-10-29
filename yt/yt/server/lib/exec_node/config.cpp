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

void MergeHeartbeatReporterConfigs(
    THeartbeatReporterConfigBase& configToSet,
    const THeartbeatReporterConfigBase& staticConfig,
    const THeartbeatReporterDynamicConfigBase& dynamicConfig)
{
    configToSet.HeartbeatPeriod = dynamicConfig.HeartbeatPeriod.value_or(staticConfig.HeartbeatPeriod);
    configToSet.HeartbeatSplay = dynamicConfig.HeartbeatSplay.value_or(staticConfig.HeartbeatSplay);
    configToSet.FailedHeartbeatBackoffStartTime = dynamicConfig.FailedHeartbeatBackoffStartTime.value_or(
        staticConfig.FailedHeartbeatBackoffStartTime);
    configToSet.FailedHeartbeatBackoffMaxTime = dynamicConfig.FailedHeartbeatBackoffMaxTime.value_or(
        staticConfig.FailedHeartbeatBackoffMaxTime);
    configToSet.FailedHeartbeatBackoffMultiplier = dynamicConfig.FailedHeartbeatBackoffMultiplier.value_or(
        staticConfig.FailedHeartbeatBackoffMultiplier);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
