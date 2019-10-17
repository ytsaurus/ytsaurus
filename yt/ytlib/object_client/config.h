#pragma once

#include "public.h"

#include <yt/client/api/public.h>

#include <yt/core/misc/config.h>

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
};

DEFINE_REFCOUNTED_TYPE(TObjectAttributeCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
