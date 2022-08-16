#pragma once

#include "public.h"

#include <yt/yt/library/auth/tvm.h>

#include <yt/yt/core/rpc/channel.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelFactoryPtr CreateServiceTicketInjectingChannelFactory(
    NRpc::IChannelFactoryPtr underlyingFactory,
    NAuth::IServiceTicketAuthPtr serviceTicketAuth);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
