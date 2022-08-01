#include "nvgpu_manager.h"

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

void FromProto(TGpuInfo* gpuInfo, int index, const nvgpu::GpuDevice& device)
{
    const auto& spec = device.spec().nvidia();
    const auto& status = device.status().nvidia();

    gpuInfo->Index = index;
    gpuInfo->UtilizationGpuRate = status.gpu_utilization() / 100.0;
    gpuInfo->UtilizationMemoryRate = status.memory_utilization() / 100.0;
    gpuInfo->MemoryUsed = status.memory_used_mb() * 1_MB;
    gpuInfo->MemoryTotal = spec.memory_size_mb() * 1_MB;
    gpuInfo->PowerDraw = status.power();
    gpuInfo->PowerLimit = spec.power();
    gpuInfo->Name = spec.uuid();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
