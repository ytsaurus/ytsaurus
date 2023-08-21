#pragma once

#include "public.h"

#include <yt/yt/core/dns/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NDns {

////////////////////////////////////////////////////////////////////////////////

IDnsResolverPtr CreateDnsOverRpcResolver(
    TDnsOverRpcResolverConfigPtr config,
    NRpc::IChannelPtr channel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns
