#include "config.h"

#include <yt/yt/ytlib/security_client/config.h>

#include <yt/yt/core/misc/config.h>

namespace NYT::NHttpProxy::NClickHouse {

////////////////////////////////////////////////////////////////////////////////

TDiscoveryCacheConfig::TDiscoveryCacheConfig()
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

TStaticClickHouseConfig::TStaticClickHouseConfig()
{
    RegisterParameter("profiling_period", ProfilingPeriod)
        .Default(TDuration::Seconds(1));
    RegisterParameter("operation_cache", OperationCache)
        .DefaultNew();
    RegisterParameter("permission_cache", PermissionCache)
        .DefaultNew();
    RegisterParameter("discovery_cache", DiscoveryCache)
        .DefaultNew();

    RegisterPreprocessor([&] {
        OperationCache->RefreshTime = TDuration::Minutes(1);
        OperationCache->ExpireAfterSuccessfulUpdateTime = TDuration::Minutes(2);
        OperationCache->ExpireAfterFailedUpdateTime = TDuration::Seconds(30);

        PermissionCache->RefreshUser = ClickHouseUserName;
        PermissionCache->AlwaysUseRefreshUser = false;
        PermissionCache->RefreshTime = TDuration::Minutes(1);
        PermissionCache->ExpireAfterFailedUpdateTime = TDuration::Minutes(2);
        PermissionCache->ExpireAfterSuccessfulUpdateTime = TDuration::Minutes(2);
    });
}

////////////////////////////////////////////////////////////////////////////////

TDynamicClickHouseConfig::TDynamicClickHouseConfig()
{
    RegisterParameter("discovery_path", DiscoveryPath)
        .Default("//sys/clickhouse/cliques");
    RegisterParameter("http_client", HttpClient)
        .DefaultNew();
    RegisterParameter("ignore_missing_credentials", IgnoreMissingCredentials)
        .Default(false);
    RegisterParameter("dead_instance_retry_count", DeadInstanceRetryCount)
        .Default(6);
    RegisterParameter("retry_without_update_limit", RetryWithoutUpdateLimit)
        .Default(3);
    RegisterParameter("force_discovery_update_age_threshold", ForceDiscoveryUpdateAgeThreshold)
        .Default(TDuration::Seconds(1));
    RegisterParameter("alias_resolution_timeout", AliasResolutionTimeout)
        .Default(TDuration::Seconds(30));
    RegisterParameter("datalens_tracing_override", DatalensTracingOverride)
        .Default();

    RegisterPreprocessor([&] {
        HttpClient->HeaderReadTimeout = TDuration::Hours(1);
        HttpClient->BodyReadIdleTimeout = TDuration::Hours(1);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy::NClickHouse
