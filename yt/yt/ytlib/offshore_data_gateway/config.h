#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NOffshoreDataGateway {

////////////////////////////////////////////////////////////////////////////////

class TOffshoreDataGatewayChannelTestingConfig
    : public NYTree::TYsonStruct
{
public:
    bool BypassCache;

    REGISTER_YSON_STRUCT(TOffshoreDataGatewayChannelTestingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOffshoreDataGatewayChannelTestingConfig)

////////////////////////////////////////////////////////////////////////////////

class TOffshoreDataGatewayChannelConfig
    : public NRpc::TRetryingChannelConfig
{
public:
    TDuration RpcTimeout;

    // Nullopt means no periodic update and therefore no available channels.
    // Some non-null update period should be set to use offshore data gateway channels.
    std::optional<TDuration> DataGatewayUpdatePeriod;

    TOffshoreDataGatewayChannelTestingConfigPtr Testing;

    REGISTER_YSON_STRUCT(TOffshoreDataGatewayChannelConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOffshoreDataGatewayChannelConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
