#pragma once

#include "public.h"

#include <yt/core/rpc/config.h>

#include <yt/core/misc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TMasterCacheServiceConfig
    : public NRpc::TThrottlingChannelConfig
    , public TSlruCacheConfig
{
public:
    TMasterCacheServiceConfig()
    {
        RegisterPreprocessor([&] () {
            Capacity = 256_MB;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TMasterCacheServiceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
