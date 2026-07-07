#pragma once

#include "public.h"

#include <yt/yt/core/rpc/config.h>

namespace NYT::NTabletBalancerClient {

////////////////////////////////////////////////////////////////////////////////

struct TTabletBalancerChannelConfig
    : public NRpc::TRetryingChannelConfig
{
    TDuration RpcTimeout;
    TDuration RpcAcknowledgementTimeout;

    REGISTER_YSON_STRUCT(TTabletBalancerChannelConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancerChannelConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancerClient
