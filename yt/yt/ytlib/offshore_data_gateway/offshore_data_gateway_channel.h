#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NOffshoreDataGateway {

////////////////////////////////////////////////////////////////////////////////

struct IOffshoreDataGatewayChannelManager
    : public TRefCounted
{
    virtual NRpc::IChannelPtr GetStickyChannel() = 0;
    virtual const NRpc::IChannelPtr& GetNonStickyChannel() = 0;
};

DEFINE_REFCOUNTED_TYPE(IOffshoreDataGatewayChannelManager)

////////////////////////////////////////////////////////////////////////////////

IOffshoreDataGatewayChannelManagerPtr CreateOffshoreDataGatewayChannelManager(
    const TOffshoreDataGatewayChannelConfigPtr& config,
    NRpc::IChannelFactoryPtr channelFactory,
    NApi::NNative::IConnectionPtr connection);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
