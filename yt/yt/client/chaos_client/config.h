#pragma once

#include "public.h"

#include <yt/yt/core/rpc/config.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardCacheConfig
    : public TAsyncExpiringCacheConfig
    , public NRpc::TBalancingChannelConfig
    , public NRpc::TRetryingChannelConfig
{ };

DEFINE_REFCOUNTED_TYPE(TReplicationCardCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

