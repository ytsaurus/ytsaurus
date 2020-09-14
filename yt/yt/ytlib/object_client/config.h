#pragma once

#include "public.h"

#include <yt/client/api/client.h>

#include <yt/core/misc/config.h>

#include <yt/core/rpc/config.h>

#include <yt/client/api/public.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

class TObjectAttributeCacheConfig
    : public TAsyncExpiringCacheConfig
{
public:
    NApi::EMasterChannelKind ReadFrom;
    // All following parameters make sense only if ReadFrom is Cache.
    TDuration MasterCacheExpireAfterSuccessfulUpdateTime;
    TDuration MasterCacheExpireAfterFailedUpdateTime;
    int MasterCacheStickyGroupSize;

    TObjectAttributeCacheConfig()
    {
        RegisterParameter("read_from", ReadFrom)
            .Default(NApi::EMasterChannelKind::Follower);
        RegisterParameter("master_cache_expire_after_successful_update_time", MasterCacheExpireAfterSuccessfulUpdateTime)
            .Default(TDuration::Seconds(15));
        RegisterParameter("master_cache_expire_after_failed_update_time", MasterCacheExpireAfterFailedUpdateTime)
            .Default(TDuration::Seconds(15));
        RegisterParameter("master_cache_cache_sticky_group_size", MasterCacheStickyGroupSize)
            .Default(1);
    }

    // TODO(max42): eliminate this by proper inheritance.
    NApi::TMasterReadOptions GetMasterReadOptions()
    {
        return NApi::TMasterReadOptions {
            ReadFrom,
            MasterCacheExpireAfterSuccessfulUpdateTime,
            MasterCacheExpireAfterFailedUpdateTime,
            MasterCacheStickyGroupSize,
        };
    }
};

DEFINE_REFCOUNTED_TYPE(TObjectAttributeCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TObjectServiceCacheConfig
    : public NRpc::TThrottlingChannelConfig
    , public TSlruCacheConfig
{
public:
    TObjectServiceCacheConfig()
    {
        RegisterPreprocessor([&] {
            Capacity = 1_GB;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TObjectServiceCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingObjectServiceConfig
    : public TObjectServiceCacheConfig
{
public:
    double CacheTtlRatio;

    TCachingObjectServiceConfig()
    {
        RegisterParameter("cache_ttl_ratio", CacheTtlRatio)
            .InRange(0, 1)
            .Default(0.5);
    }
};

DEFINE_REFCOUNTED_TYPE(TCachingObjectServiceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
