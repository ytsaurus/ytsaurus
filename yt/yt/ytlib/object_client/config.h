#pragma once

#include "public.h"

#include <yt/client/api/client.h>

#include <yt/core/misc/cache_config.h>

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
            .Default(1.0);
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
    : public TSlruCacheConfig
{
public:
    double TopEntryByteRateThreshold;

    TObjectServiceCacheConfig()
    {
        RegisterPreprocessor([&] {
            Capacity = 1_GB;
        });

        RegisterParameter("top_entry_byte_rate_threshold", TopEntryByteRateThreshold)
            .Default(10_KB);
    }
};

DEFINE_REFCOUNTED_TYPE(TObjectServiceCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TObjectServiceCacheDynamicConfig
    : public TSlruCacheDynamicConfig
{
public:
    std::optional<double> TopEntryByteRateThreshold;

    TObjectServiceCacheDynamicConfig()
    {
        RegisterParameter("top_entry_byte_rate_threshold", TopEntryByteRateThreshold)
            .Optional();
    }
};

DEFINE_REFCOUNTED_TYPE(TObjectServiceCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingObjectServiceConfig
    : public NRpc::TThrottlingChannelConfig
    , public TObjectServiceCacheConfig
{
public:
    double CacheTtlRatio;
    i64 EntryByteRateLimit;

    TCachingObjectServiceConfig()
    {
        RegisterParameter("cache_ttl_ratio", CacheTtlRatio)
            .InRange(0, 1)
            .Default(0.5);
        RegisterParameter("entry_byte_rate_limit", EntryByteRateLimit)
            .GreaterThan(0)
            .Default(10_MB);
    }
};

DEFINE_REFCOUNTED_TYPE(TCachingObjectServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TCachingObjectServiceDynamicConfig
    : public NRpc::TThrottlingChannelDynamicConfig
    , public TObjectServiceCacheDynamicConfig
{
public:
    std::optional<double> CacheTtlRatio;
    std::optional<i64> EntryByteRateLimit;

    TCachingObjectServiceDynamicConfig()
    {
        RegisterParameter("cache_ttl_ratio", CacheTtlRatio)
            .InRange(0, 1)
            .Optional();
        RegisterParameter("entry_byte_rate_limit", EntryByteRateLimit)
            .GreaterThan(0)
            .Optional();
    }
};

DEFINE_REFCOUNTED_TYPE(TCachingObjectServiceDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TReqExecuteBatchWithRetriesConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration StartBackoff;
    TDuration MaxBackoff;
    double BackoffMultiplier;
    int RetryCount;

    TReqExecuteBatchWithRetriesConfig()
    {
        RegisterParameter("base_backoff", StartBackoff)
            .Default(TDuration::Seconds(1));
        RegisterParameter("max_backoff", MaxBackoff)
            .Default(TDuration::Seconds(20));
        RegisterParameter("backoff_multiplier", BackoffMultiplier)
            .GreaterThanOrEqual(1)
            .Default(2);
        RegisterParameter("retry_count", RetryCount)
            .GreaterThanOrEqual(0)
            .Default(5);
    }
};

DEFINE_REFCOUNTED_TYPE(TReqExecuteBatchWithRetriesConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
