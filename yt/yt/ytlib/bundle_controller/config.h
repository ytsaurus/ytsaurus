#pragma once

#include "public.h"

#include <yt/yt/core/misc/cache_config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NBundleController {

////////////////////////////////////////////////////////////////////////////////

class TBundleControllerChannelConfig
    : public NRpc::TRetryingChannelConfig
{
public:
    TDuration RpcTimeout;

    TDuration RpcAcknowledgementTimeout;

    REGISTER_YSON_STRUCT(TBundleControllerChannelConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleControllerChannelConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBundleController
