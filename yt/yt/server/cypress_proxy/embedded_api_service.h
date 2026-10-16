#pragma once

#include "public.h"

#include <yt/yt/server/lib/rpc_proxy/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

NRpcProxy::IApiServicePtr CreateEmbeddedApiService(
    IBootstrap* bootstrap,
    NRpcProxy::IProxyCoordinatorPtr proxyCoordinator,
    NRpc::IChannelPtr localChannel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
