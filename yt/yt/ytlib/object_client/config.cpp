#include "config.h"

#include <yt/yt/client/api/client.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

void TObjectAttributeCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("read_from", &TThis::ReadFrom)
        .Default(NApi::EMasterChannelKind::Follower);
    registrar.Parameter("master_cache_expire_after_successful_update_time", &TThis::MasterCacheExpireAfterSuccessfulUpdateTime)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("master_cache_expire_after_failed_update_time", &TThis::MasterCacheExpireAfterFailedUpdateTime)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("master_cache_cache_sticky_group_size", &TThis::MasterCacheStickyGroupSize)
        .Default();
}

NApi::TMasterReadOptions TObjectAttributeCacheConfig::GetMasterReadOptions()
{
    return NApi::TMasterReadOptions{
        .ReadFrom = ReadFrom,
        .ExpireAfterSuccessfulUpdateTime = MasterCacheExpireAfterSuccessfulUpdateTime,
        .ExpireAfterFailedUpdateTime = MasterCacheExpireAfterFailedUpdateTime,
        .CacheStickyGroupSize = MasterCacheStickyGroupSize
    };
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceCacheConfig::TObjectServiceCacheConfig()
{
    RegisterPreprocessor([&] {
        Capacity = 1_GB;
    });

    RegisterParameter("top_entry_byte_rate_threshold", TopEntryByteRateThreshold)
        .Default(10_KB);
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceCacheDynamicConfig::TObjectServiceCacheDynamicConfig()
{
    RegisterParameter("top_entry_byte_rate_threshold", TopEntryByteRateThreshold)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TCachingObjectServiceConfig::TCachingObjectServiceConfig()
{
    RegisterParameter("cache_ttl_ratio", CacheTtlRatio)
        .InRange(0, 1)
        .Default(0.5);
    RegisterParameter("entry_byte_rate_limit", EntryByteRateLimit)
        .GreaterThan(0)
        .Default(10_MB);
}

////////////////////////////////////////////////////////////////////////////////

TCachingObjectServiceDynamicConfig::TCachingObjectServiceDynamicConfig()
{
    RegisterParameter("cache_ttl_ratio", CacheTtlRatio)
        .InRange(0, 1)
        .Optional();
    RegisterParameter("entry_byte_rate_limit", EntryByteRateLimit)
        .GreaterThan(0)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TReqExecuteBatchWithRetriesConfig::TReqExecuteBatchWithRetriesConfig()
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

////////////////////////////////////////////////////////////////////////////////

TAbcConfig::TAbcConfig() {
    RegisterParameter("id", Id)
        .GreaterThan(0);
    RegisterParameter("name", Name)
        .Default()
        .NonEmpty();
    RegisterParameter("slug", Slug)
        .NonEmpty();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
