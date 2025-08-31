#include "gpu_info_provider_detail.h"

#include <yt/yt/core/rpc/retrying_channel.h>

#include <yt/yt/core/rpc/grpc/channel.h>
#include <yt/yt/core/rpc/grpc/config.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/string/string_builder.h>

namespace NYT::NGpu {

using namespace NYTree;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

std::vector<TGpuInfo> TGpuInfoProviderBase::GetGpuInfos(TDuration /*timeout*/) const
{
    return {};
}

std::vector<TRdmaDeviceInfo> TGpuInfoProviderBase::GetRdmaDeviceInfos(TDuration /*timeout*/) const
{
    return {};
}

////////////////////////////////////////////////////////////////////////////////

TGrpcGpuInfoProviderBase::TGrpcGpuInfoProviderBase(TGrpcGpuInfoProviderConfigBasePtr config)
    : BaseGrpcConfig_(std::move(config))
    , Channel_(CreateGrpcChannel())
{ }

IChannelPtr TGrpcGpuInfoProviderBase::CreateGrpcChannel()
{
    auto innerChannelConfig = New<NGrpc::TChannelConfig>();
    innerChannelConfig->Address = BaseGrpcConfig_->Address;
    auto innerChannel = NGrpc::CreateGrpcChannel(innerChannelConfig);

    return CreateRetryingChannel(BaseGrpcConfig_->Channel, innerChannel);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NGpu
