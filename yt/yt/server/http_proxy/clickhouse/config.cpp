#include "config.h"

#include <yt/yt/ytlib/security_client/config.h>

#include <yt/yt/core/misc/config.h>

namespace NYT::NHttpProxy::NClickHouse {

////////////////////////////////////////////////////////////////////////////////

void TDiscoveryCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cache_base", &TThis::CacheBase)
        .DefaultNew();
    registrar.Parameter("soft_age_threshold", &TThis::SoftAgeThreshold)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("hard_age_threshold", &TThis::HardAgeThreshold)
        .Default(TDuration::Minutes(15));
    registrar.Parameter("master_cache_expire_time", &TThis::MasterCacheExpireTime)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("unavailable_instance_ban_timeout", &TThis::UnavailableInstanceBanTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Preprocessor([] (TThis* config) {
        config->CacheBase->Capacity = 1000;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TStaticClickHouseConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("profiling_period", &TThis::ProfilingPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("operation_cache", &TThis::OperationCache)
        .DefaultNew();
    registrar.Parameter("permission_cache", &TThis::PermissionCache)
        .DefaultNew();
    registrar.Parameter("discovery_cache", &TThis::DiscoveryCache)
        .DefaultNew();

    registrar.Parameter("operation_id_update_period", &TThis::OperationIdUpdatePeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("populate_user_with_token", &TThis::PopulateUserWithToken)
        .Default(false);

    registrar.Parameter("chyt_strawberry_path", &TThis::ChytStrawberryPath)
        .Default("//sys/strawberry/chyt");

    registrar.Preprocessor([] (TThis* config) {
        config->OperationCache->RefreshTime = TDuration::Minutes(1);
        config->OperationCache->ExpireAfterSuccessfulUpdateTime = TDuration::Minutes(2);
        config->OperationCache->ExpireAfterFailedUpdateTime = TDuration::Seconds(30);

        config->PermissionCache->RefreshUser = ClickHouseUserName;
        config->PermissionCache->AlwaysUseRefreshUser = false;
        config->PermissionCache->RefreshTime = TDuration::Minutes(1);
        config->PermissionCache->ExpireAfterFailedUpdateTime = TDuration::Minutes(2);
        config->PermissionCache->ExpireAfterSuccessfulUpdateTime = TDuration::Minutes(2);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicClickHouseConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("discovery_path", &TThis::DiscoveryPath)
        .Default("//sys/clickhouse/cliques");
    registrar.Parameter("http_client", &TThis::HttpClient)
        .DefaultNew();
    registrar.Parameter("ignore_missing_credentials", &TThis::IgnoreMissingCredentials)
        .Default(false);
    registrar.Parameter("dead_instance_retry_count", &TThis::DeadInstanceRetryCount)
        .Default(6);
    registrar.Parameter("retry_without_update_limit", &TThis::RetryWithoutUpdateLimit)
        .Default(3);
    registrar.Parameter("force_discovery_update_age_threshold", &TThis::ForceDiscoveryUpdateAgeThreshold)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("alias_resolution_timeout", &TThis::AliasResolutionTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("datalens_tracing_override", &TThis::DatalensTracingOverride)
        .Default();

    registrar.Preprocessor([] (TThis* config) {
        config->HttpClient->HeaderReadTimeout = TDuration::Hours(1);
        config->HttpClient->BodyReadIdleTimeout = TDuration::Hours(1);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy::NClickHouse
