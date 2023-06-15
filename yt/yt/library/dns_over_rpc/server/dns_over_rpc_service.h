#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/dns/public.h>

namespace NYT::NDns {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateDnsOverRpcService(
    IDnsResolverPtr resolver,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns
