#pragma once

#include "gpu_info_provider.h"
#include "private.h"
#include "public.h"

#include <library/cpp/yt/yson/consumer.h>

namespace NYT::NGpu {

////////////////////////////////////////////////////////////////////////////////

class TGpuInfoProviderBase
    : public IGpuInfoProvider
{
    std::vector<TGpuInfo> GetGpuInfos(TDuration timeout) const override;
    std::vector<TRdmaDeviceInfo> GetRdmaDeviceInfos(TDuration timeout) const override;
};

////////////////////////////////////////////////////////////////////////////////

class TGrpcGpuInfoProviderBase
    : public TGpuInfoProviderBase
{
public:
    explicit TGrpcGpuInfoProviderBase(TGrpcGpuInfoProviderConfigBasePtr config);

protected:
    const TGrpcGpuInfoProviderConfigBasePtr BaseGrpcConfig_;

    const NRpc::IChannelPtr Channel_;

private:
    NRpc::IChannelPtr CreateGrpcChannel();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NGpu
