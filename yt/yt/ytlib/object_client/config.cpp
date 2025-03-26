#include "config.h"

#include <yt/yt/client/api/client.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

void TObjectAttributeCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("master_read_options", &TThis::MasterReadOptions)
        .DefaultNew();
    registrar.Parameter("user_name", &TThis::UserName)
        .Default(NRpc::RootUserName);

    // COMPAT(dakovalkov)
    registrar.Parameter("read_from", &TThis::ReadFrom_)
        .Optional();
    registrar.Parameter("master_cache_expire_after_successful_update_time", &TThis::MasterCacheExpireAfterSuccessfulUpdateTime_)
        .Optional();
    registrar.Parameter("master_cache_expire_after_failed_update_time", &TThis::MasterCacheExpireAfterFailedUpdateTime_)
        .Optional();
    registrar.Parameter("master_cache_cache_sticky_group_size", &TThis::MasterCacheStickyGroupSize_)
        .Optional();

    registrar.Postprocessor([] (TThis* config) {
        if (config->ReadFrom_) {
            config->MasterReadOptions->ReadFrom = *config->ReadFrom_;
        }
        if (config->MasterCacheExpireAfterSuccessfulUpdateTime_) {
            config->MasterReadOptions->ExpireAfterSuccessfulUpdateTime = *config->MasterCacheExpireAfterSuccessfulUpdateTime_;
        }
        if (config->MasterCacheExpireAfterFailedUpdateTime_) {
            config->MasterReadOptions->ExpireAfterFailedUpdateTime = *config->MasterCacheExpireAfterFailedUpdateTime_;
        }
        if (config->MasterCacheStickyGroupSize_) {
            config->MasterReadOptions->CacheStickyGroupSize = *config->MasterCacheStickyGroupSize_;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TObjectServiceCacheConfig::Register(TRegistrar registrar)
{
    registrar.Preprocessor([] (TThis* config) {
        config->Capacity = 1_GB;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TObjectServiceCacheDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("entry_byte_rate_limit", &TThis::EntryByteRateLimit)
        .GreaterThan(0)
        .Default(10_MB);
    registrar.Parameter("top_entry_byte_rate_threshold", &TThis::TopEntryByteRateThreshold)
        .Default(10_KB);
    registrar.Parameter("aggregation_period", &TThis::AggregationPeriod)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("min_advised_sticky_group_size", &TThis::MinAdvisedStickyGroupSize)
        .GreaterThanOrEqual(1)
        .Default(1);
    registrar.Parameter("max_advised_sticky_group_size", &TThis::MaxAdvisedStickyGroupSize)
        .LessThanOrEqual(1'000)
        .Default(20);

    registrar.Postprocessor([] (TObjectServiceCacheDynamicConfig* config) {
        if (config->MinAdvisedStickyGroupSize > config->MaxAdvisedStickyGroupSize) {
            THROW_ERROR_EXCEPTION("\"min_advised_sticky_group_size\" must be less than or equal to \"max_advised_sticky_group_size\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TCachingObjectServiceConfig::Register(TRegistrar registrar)
{
    registrar.Preprocessor([] (TThis* config) {
        // The default value of 10 is too low for most caching object service instances.
        config->RateLimit = 1000;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TCachingObjectServiceDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cache_ttl_ratio", &TThis::CacheTtlRatio)
        .InRange(0, 1)
        .Default(0.5);
}

////////////////////////////////////////////////////////////////////////////////

void TReqExecuteBatchWithRetriesConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("base_backoff", &TThis::StartBackoff)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("max_backoff", &TThis::MaxBackoff)
        .Default(TDuration::Seconds(20));
    registrar.Parameter("backoff_multiplier", &TThis::BackoffMultiplier)
        .GreaterThanOrEqual(1)
        .Default(2);
    registrar.Parameter("retry_count", &TThis::RetryCount)
        .GreaterThanOrEqual(0)
        .Default(5);
}

////////////////////////////////////////////////////////////////////////////////

void TAbcConfig::Register(TRegistrar registrar) {
    registrar.Parameter("id", &TThis::Id)
        .GreaterThan(0);
    registrar.Parameter("name", &TThis::Name)
        .Default()
        .NonEmpty();
    registrar.Parameter("slug", &TThis::Slug)
        .NonEmpty();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
