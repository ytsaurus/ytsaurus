#pragma once

#include "public.h"

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NOffshoreDataGateway {

////////////////////////////////////////////////////////////////////////////////

class TOffshoreDataGatewayChannelConfig
    : public NRpc::TRetryingChannelConfig
{
public:
    TDuration RpcTimeout;

    REGISTER_YSON_STRUCT(TOffshoreDataGatewayChannelConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOffshoreDataGatewayChannelConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
