#pragma once

#include "public.h"

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NOffshoreNodeProxy {

////////////////////////////////////////////////////////////////////////////////

class TOffshoreNodeProxyChannelConfig
    : public NRpc::TRetryingChannelConfig
{
public:
    TDuration RpcTimeout;

    REGISTER_YSON_STRUCT(TOffshoreNodeProxyChannelConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOffshoreNodeProxyChannelConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreNodeProxy
