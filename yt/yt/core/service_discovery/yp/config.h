#pragma once

#include "public.h"

#include <yt/core/misc/cache_config.h>

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NServiceDiscovery::NYP {

////////////////////////////////////////////////////////////////////////////////

class TServiceDiscoveryConfig
    : public NRpc::TRetryingChannelConfig
    , public TAsyncExpiringCacheConfig
{
public:
    //! Provider endpoint.
    TString Fqdn;
    int GrpcPort;

    //! Provider throttles requests based on this string.
    TString Client;

    TServiceDiscoveryConfig();
};

DEFINE_REFCOUNTED_TYPE(TServiceDiscoveryConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServiceDiscovery::NYP
