#include "config.h"

#include <yt/core/misc/config.h>

namespace NYT::NHttpProxy::NClickHouse {

////////////////////////////////////////////////////////////////////////////////

TCliqueCacheConfig::TCliqueCacheConfig()
{
    RegisterParameter("cache_base", CacheBase)
        .DefaultNew(/* capacity */ 1000);
    RegisterParameter("soft_age_threshold", SoftAgeThreshold)
        .Default(TDuration::Seconds(15));
    RegisterParameter("hard_age_threshold", HardAgeThreshold)
        .Default(TDuration::Minutes(15));
    RegisterParameter("master_cache_expire_time", MasterCacheExpireTime)
        .Default(TDuration::Seconds(5));
    RegisterParameter("unavailable_instance_ban_timeout", UnavailableInstanceBanTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

TClickHouseConfig::TClickHouseConfig()
{
    RegisterParameter("discovery_path", DiscoveryPath)
        .Default("//sys/clickhouse/cliques");
    RegisterParameter("http_client", HttpClient)
        .DefaultNew();
    RegisterParameter("profiling_period", ProfilingPeriod)
        .Default(TDuration::Seconds(1));
    RegisterParameter("clique_cache", CliqueCache)
        .DefaultNew();
    RegisterParameter("ignore_missing_credentials", IgnoreMissingCredentials)
        .Default(false);
    RegisterParameter("dead_instance_retry_count", DeadInstanceRetryCount)
        .Default(4);
    RegisterParameter("retry_without_update_limit", RetryWithoutUpdateLimit)
        .Default(2);
    RegisterParameter("force_discovery_update_age_threshold", ForceDiscoveryUpdateAgeThreshold)
        .Default(TDuration::Seconds(1));
    RegisterParameter("alias_resolution_timeout", AliasResolutionTimeout)
        .Default(TDuration::Seconds(30));
    RegisterParameter("force_enqueue_profiling", ForceEnqueueProfiling)
        .Default(false);

    RegisterPreprocessor([&] {
        HttpClient->HeaderReadTimeout = TDuration::Hours(1);
        HttpClient->BodyReadIdleTimeout = TDuration::Hours(1);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy::NClickHouse
