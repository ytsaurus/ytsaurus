#include "config.h"

#include <yt/yt/core/rpc/config.h>

namespace NYT::NGpu {

////////////////////////////////////////////////////////////////////////////////

void TGpuInfoProviderConfigBase::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TNvidiaSmiGpuInfoProviderConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TGrpcGpuInfoProviderConfigBase::Register(TRegistrar registrar)
{
    // COMPAT(renadeen): Remove aliases after this code is in production and the new names are in use.
    registrar.Parameter("address", &TThis::Address)
        .Alias("nv_gpu_manager_service_address")
        .Default();
    registrar.Parameter("service_name", &TThis::ServiceName)
        .Alias("nv_gpu_manager_service_name")
        .Default();
    registrar.Parameter("channel", &TThis::Channel)
        .Alias("nv_gpu_manager_channel")
        .DefaultCtor([] {
            auto config = New<NRpc::TRetryingChannelConfig>();
            config->RetryBackoffTime = TDuration::Seconds(20);
            config->RetryAttempts = 5;
            return config;
        });
}

////////////////////////////////////////////////////////////////////////////////

void TNvManagerGpuInfoProviderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("devices_cgroup_path", &TThis::DevicesCgroupPath)
        .Alias("nv_gpu_manager_devices_cgroup_path")
        .Default();
    // COMPAT(ignat)
    registrar.Parameter("gpu_indexes_from_nvidia_smi", &TThis::GpuIndexesFromNvidiaSmi)
        .Default(false);
    registrar.Preprocessor([] (TThis* config){
        config->Address = "unix:/var/run/nvgpu-manager.sock";
        config->ServiceName = "nvgpu.NvGpuManager";
    });
}

////////////////////////////////////////////////////////////////////////////////

void TGpuAgentGpuInfoProviderConfig::Register(TRegistrar registrar)
{
    registrar.Preprocessor([] (TThis* config){
        config->Address = "unix:/var/run/gpu-agent.sock";
        config->ServiceName = "yt.GpuAgent";
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NGpu
