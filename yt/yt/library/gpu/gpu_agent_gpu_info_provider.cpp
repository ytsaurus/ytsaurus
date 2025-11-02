#include "gpu_agent_gpu_info_provider.h"

#include "config.h"
#include "gpu_info_provider_detail.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/config.h>
#include <yt/yt/core/rpc/retrying_channel.h>

#include <yt/yt/core/rpc/grpc/channel.h>
#include <yt/yt/core/rpc/grpc/config.h>

#include <library/cpp/protobuf/interop/cast.h>

#include <yt/yt/gpuagent/api/api.pb.h>

namespace NYT::NGpu {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = GpuLogger;

////////////////////////////////////////////////////////////////////////////////

using TReqListGpuDevices = NYT::NGpuAgent::NProto::ListGpuDevicesRequest;
using TRspListGpuDevices = NYT::NGpuAgent::NProto::ListGpuDevicesResponse;

class TGpuAgentServiceProxy
    : public NYT::NRpc::TProxyBase
{
public:
    TGpuAgentServiceProxy(NYT::NRpc::IChannelPtr channel, std::string serviceName)
        : TProxyBase(std::move(channel), NYT::NRpc::TServiceDescriptor(std::move(serviceName)))
    { }

    DEFINE_RPC_PROXY_METHOD(NGpu, ListGpuDevices);
};

std::optional<ESlowdownType> ConvertSlowdownType(NYT::NGpuAgent::NProto::SlowdownType source)
{
    switch (source) {
        case NYT::NGpuAgent::NProto::HW:
            return ESlowdownType::HW;
        case NYT::NGpuAgent::NProto::HWPowerBrake:
            return ESlowdownType::HWPowerBrake;
        case NYT::NGpuAgent::NProto::HWThermal:
            return ESlowdownType::HWThermal;
        case NYT::NGpuAgent::NProto::SWThermal:
            return ESlowdownType::SWThermal;
        default:
            return std::nullopt;
    }
}

void FromProto(TGpuInfo* gpuInfo, const NYT::NGpuAgent::NProto::GpuDevice& device)
{
    gpuInfo->Index = device.minor();
    gpuInfo->Name = device.uuid();
    gpuInfo->UtilizationGpuRate = device.gpu_utilization_rate() / 100.0;
    gpuInfo->UtilizationMemoryRate = device.memory_utilization_rate() / 100.0;
    gpuInfo->MemoryUsed = device.memory_used();
    gpuInfo->MemoryTotal = device.memory_total();
    gpuInfo->PowerDraw = device.power_usage();
    gpuInfo->PowerLimit = device.power_limit();
    if (device.clock_sm()) {
        gpuInfo->ClocksSM = device.clock_sm();
    }

    for (int i = 0; i < device.slowdowns_size(); ++i) {
        auto converted = ConvertSlowdownType(device.slowdowns(i));
        if (converted) {
            gpuInfo->Slowdowns[*converted] = true;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TGpuAgentGpuInfoProvider
    : public TGrpcGpuInfoProviderBase
{
public:
    explicit TGpuAgentGpuInfoProvider(TGpuAgentGpuInfoProviderConfigPtr config)
        : TGrpcGpuInfoProviderBase(std::move(config))
    { }

    std::vector<TGpuInfo> GetGpuInfos(TDuration timeout) const override
    {
        YT_LOG_INFO("List GPU devices with GpuAgent");

        TGpuAgentServiceProxy proxy(Channel_, BaseGrpcConfig_->ServiceName);
        auto req = proxy.ListGpuDevices();
        req->SetTimeout(timeout);

        auto rspFuture = req->Invoke();
        auto rsp = WaitFor(rspFuture)
            .ValueOrThrow();

        YT_LOG_INFO("GPU devices listed with GpuAgent (Count: %v)", rsp->devices_size());

        std::vector<TGpuInfo> gpuInfos;
        gpuInfos.reserve(rsp->devices_size());

        for (const auto& device : rsp->devices()) {
            auto gpuInfo = NYT::FromProto<TGpuInfo>(device);
            gpuInfos.push_back(gpuInfo);
        }

        return gpuInfos;
    }
};

////////////////////////////////////////////////////////////////////////////////

IGpuInfoProviderPtr CreateGpuAgentGpuInfoProvider(TGpuAgentGpuInfoProviderConfigPtr config)
{
    return New<TGpuAgentGpuInfoProvider>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NGpu
