#pragma once

#include "public.h"

#include <yt/client/api/rpc_proxy/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateDiscoveryService(
    NRpcProxy::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
