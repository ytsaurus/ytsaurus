#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateProxyingChunkService(
    NElection::TCellId cellId,
    TProxyingChunkServiceConfigPtr serviceConfig,
    NApi::NNative::TMasterConnectionConfigPtr connectionConfig,
    NRpc::IChannelFactoryPtr channelFactory,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
