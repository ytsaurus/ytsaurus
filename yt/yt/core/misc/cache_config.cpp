#include "cache_config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TSlruCacheConfig::TSlruCacheConfig(i64 capacity)
{
    RegisterParameter("capacity", Capacity)
        .Default(capacity)
        .GreaterThanOrEqual(0);
    RegisterParameter("younger_size_fraction", YoungerSizeFraction)
        .Default(0.25)
        .InRange(0.0, 1.0);
    RegisterParameter("shard_count", ShardCount)
        .Default(16)
        .GreaterThan(0);
    RegisterParameter("touch_buffer_capacity", TouchBufferCapacity)
        .Default(65536)
        .GreaterThan(0);
    RegisterParameter("small_ghost_cache_ratio", SmallGhostCacheRatio)
        .Default(0.5)
        .GreaterThanOrEqual(0.0);
    RegisterParameter("large_ghost_cache_ratio", LargeGhostCacheRatio)
        .Default(2.0)
        .GreaterThanOrEqual(0.0);
    RegisterParameter("enable_ghost_caches", EnableGhostCaches)
        .Default(true);

    RegisterPostprocessor([&] () {
        if (!IsPowerOf2(ShardCount)) {
            THROW_ERROR_EXCEPTION("\"shard_count\" must be power of two, actual: %v",
                ShardCount);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TSlruCacheDynamicConfig::TSlruCacheDynamicConfig()
{
    RegisterParameter("capacity", Capacity)
        .Optional()
        .GreaterThanOrEqual(0);
    RegisterParameter("younger_size_fraction", YoungerSizeFraction)
        .Optional()
        .InRange(0.0, 1.0);
    RegisterParameter("enable_ghost_caches", EnableGhostCaches)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TAsyncExpiringCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("expire_after_access_time", &TThis::ExpireAfterAccessTime)
        .Default(TDuration::Seconds(300));
    registrar.Parameter("expire_after_successful_update_time", &TThis::ExpireAfterSuccessfulUpdateTime)
        .Alias("success_expiration_time")
        .Default(TDuration::Seconds(15));
    registrar.Parameter("expire_after_failed_update_time", &TThis::ExpireAfterFailedUpdateTime)
        .Alias("failure_expiration_time")
        .Default(TDuration::Seconds(15));
    registrar.Parameter("refresh_time", &TThis::RefreshTime)
        .Alias("success_probation_time")
        .Default(TDuration::Seconds(10));
    registrar.Parameter("batch_update", &TThis::BatchUpdate)
        .Default(false);

    registrar.Postprocessor([] (TThis* config) {
        if (config->RefreshTime && *config->RefreshTime && *config->RefreshTime > config->ExpireAfterSuccessfulUpdateTime) {
            THROW_ERROR_EXCEPTION("\"refresh_time\" must be less than \"expire_after_successful_update_time\"")
                << TErrorAttribute("refresh_time", config->RefreshTime)
                << TErrorAttribute("expire_after_successful_update_time", config->ExpireAfterSuccessfulUpdateTime);
        }
    });
}

void TAsyncExpiringCacheConfig::ApplyDynamicInplace(
    const TAsyncExpiringCacheDynamicConfigPtr& dynamicConfig)
{
    ExpireAfterAccessTime = dynamicConfig->ExpireAfterAccessTime.value_or(ExpireAfterAccessTime);
    ExpireAfterSuccessfulUpdateTime = dynamicConfig->ExpireAfterSuccessfulUpdateTime.value_or(ExpireAfterSuccessfulUpdateTime);
    ExpireAfterFailedUpdateTime = dynamicConfig->ExpireAfterFailedUpdateTime.value_or(ExpireAfterFailedUpdateTime);
    RefreshTime = dynamicConfig->RefreshTime.has_value()
        ? dynamicConfig->RefreshTime
        : RefreshTime;
    BatchUpdate = dynamicConfig->BatchUpdate.value_or(BatchUpdate);
}

TAsyncExpiringCacheConfigPtr TAsyncExpiringCacheConfig::ApplyDynamic(
    const TAsyncExpiringCacheDynamicConfigPtr& dynamicConfig) const
{
    auto config = New<TAsyncExpiringCacheConfig>();

    config->ApplyDynamicInplace(dynamicConfig);

    config->Postprocess();
    return config;
}

////////////////////////////////////////////////////////////////////////////////

void TAsyncExpiringCacheDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("expire_after_access_time", &TThis::ExpireAfterAccessTime)
        .Optional();
    registrar.Parameter("expire_after_successful_update_time", &TThis::ExpireAfterSuccessfulUpdateTime)
        .Optional();
    registrar.Parameter("expire_after_failed_update_time", &TThis::ExpireAfterFailedUpdateTime)
        .Optional();
    registrar.Parameter("refresh_time", &TThis::RefreshTime)
        .Optional();
    registrar.Parameter("batch_update", &TThis::BatchUpdate)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
