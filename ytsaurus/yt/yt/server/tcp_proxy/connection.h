#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/net/public.h>

namespace NYT::NTcpProxy {

////////////////////////////////////////////////////////////////////////////////

void HandleConnection(
    NNet::IConnectionPtr sourceConnection,
    NNet::TNetworkAddress destinationAddress,
    const NNet::IDialerPtr& dialer,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTcpProxy
