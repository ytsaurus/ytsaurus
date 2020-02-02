#pragma once

#include "public.h"

#include <yt/core/rpc/config.h>

#include <yt/core/misc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TObjectServiceCacheConfig
    : public NRpc::TThrottlingChannelConfig
    , public TSlruCacheConfig
{
public:
    TObjectServiceCacheConfig()
    {
        RegisterPreprocessor([&] () {
            Capacity = 1_GB;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TObjectServiceCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterCacheServiceConfig
    : public TObjectServiceCacheConfig
{
public:
    double NodeCacheTtlRatio;

    TMasterCacheServiceConfig()
    {
        RegisterParameter("node_cache_ttl_ratio", NodeCacheTtlRatio)
            .InRange(0, 1)
            .Default(0.5);
    }
};

DEFINE_REFCOUNTED_TYPE(TMasterCacheServiceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
