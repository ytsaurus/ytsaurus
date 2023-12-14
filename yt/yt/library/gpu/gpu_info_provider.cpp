#include "gpu_info_provider.h"

#include "config.h"

#include "nvidia_smi_gpu_info_provider.h"
#include "nv_manager_gpu_info_provider.h"

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/string/string_builder.h>

namespace NYT::NGpu {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TGpuInfo& gpuInfo, TStringBuf /*format*/)
{
    builder->AppendFormat(
        "{UpdateTime: %v, Index: %v, Name: %v, UtilizationGpuRate: %v, UtilizationMemoryRate: %v, "
        "MemoryUsed: %v, MemoryTotal: %v, PowerDraw: %v, PowerLimit: %v, ClocksSM: %v, ClocksMaxSM: %v, "
        "SMUtilizationRate: %v, SMOccupancyRate: %v, Stuck: {Status: %v, LastTransitionTime: %v}}",
        gpuInfo.UpdateTime,
        gpuInfo.Index,
        gpuInfo.Name,
        gpuInfo.UtilizationGpuRate,
        gpuInfo.UtilizationMemoryRate,
        gpuInfo.MemoryUsed,
        gpuInfo.MemoryTotal,
        gpuInfo.PowerDraw,
        gpuInfo.PowerLimit,
        gpuInfo.ClocksSM,
        gpuInfo.ClocksMaxSM,
        gpuInfo.SMUtilizationRate,
        gpuInfo.SMOccupancyRate,
        gpuInfo.Stuck.Status,
        gpuInfo.Stuck.LastTransitionTime);
}

void Serialize(const TGpuInfo& gpuInfo, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("update_time").Value(gpuInfo.UpdateTime)
            .Item("index").Value(gpuInfo.Index)
            .Item("name").Value(gpuInfo.Name)
            .Item("utilization_gpu_rate").Value(gpuInfo.UtilizationGpuRate)
            .Item("utilization_memory_rate").Value(gpuInfo.UtilizationMemoryRate)
            .Item("memory_used").Value(gpuInfo.MemoryUsed)
            .Item("memory_limit").Value(gpuInfo.MemoryTotal)
            .Item("power_draw").Value(gpuInfo.PowerDraw)
            .Item("power_limit").Value(gpuInfo.PowerLimit)
            .Item("clocks_sm").Value(gpuInfo.ClocksSM)
            .Item("clocks_max_sm").Value(gpuInfo.ClocksMaxSM)
            .Item("sm_utilization_rate").Value(gpuInfo.SMUtilizationRate)
            .Item("sm_occupancy_rate").Value(gpuInfo.SMOccupancyRate)
            .Item("stuck").BeginMap()
                .Item("status").Value(gpuInfo.Stuck.Status)
                .Item("last_transition_time").Value(gpuInfo.Stuck.LastTransitionTime)
            .EndMap()
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

IGpuInfoProviderPtr CreateGpuInfoProvider(TGpuInfoSourceConfigPtr config)
{
    switch (config->Type) {
    case EGpuInfoSourceType::NvGpuManager:
        return CreateNvManagerGpuInfoProvider(std::move(config));
    case EGpuInfoSourceType::NvidiaSmi:
        return CreateNvidiaSmiGpuInfoProvider();
    default:
        YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NGpu
