#include "config.h"

#include <yt/yt/core/rpc/config.h>

namespace NYT::NGpu {

////////////////////////////////////////////////////////////////////////////////

void TGpuInfoSourceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("type", &TThis::Type)
        .Default(EGpuInfoSourceType::NvidiaSmi);

    registrar.Parameter("nv_gpu_manager_service_address", &TThis::NvGpuManagerServiceAddress)
        .Default("unix:/var/run/nvgpu-manager.sock");
    registrar.Parameter("nv_gpu_manager_service_name", &TThis::NvGpuManagerServiceName)
        .Default("nvgpu.NvGpuManager");
    registrar.Parameter("nv_gpu_manager_channel", &TThis::NvGpuManagerChannel)
        .DefaultCtor([] {
            auto config = New<NRpc::TRetryingChannelConfig>();
            config->RetryBackoffTime = TDuration::Seconds(20);
            config->RetryAttempts = 5;
            return config;
        });
    registrar.Parameter("nv_gpu_manager_devices_cgroup_path", &TThis::NvGpuManagerDevicesCgroupPath)
        .Default();
    // COMPAT(ignat)
    registrar.Parameter("gpu_indexes_from_nvidia_smi", &TThis::GpuIndexesFromNvidiaSmi)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NGpu
