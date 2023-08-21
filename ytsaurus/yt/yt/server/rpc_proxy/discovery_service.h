#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/api/rpc_proxy/public.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateDiscoveryService(
    TProxyConfigPtr config,
    IProxyCoordinatorPtr proxyCoordinator,
    NApi::NNative::IConnectionPtr connection,
    IInvokerPtr controlInvoker,
    IInvokerPtr workerInvoker,
    NNodeTrackerClient::TAddressMap localAddresses);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
