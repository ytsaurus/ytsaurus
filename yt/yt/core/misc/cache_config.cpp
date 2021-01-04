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
}

////////////////////////////////////////////////////////////////////////////////

TAsyncExpiringCacheConfig::TAsyncExpiringCacheConfig()
{
    RegisterParameter("expire_after_access_time", ExpireAfterAccessTime)
        .Default(TDuration::Seconds(300));
    RegisterParameter("expire_after_successful_update_time", ExpireAfterSuccessfulUpdateTime)
        .Alias("success_expiration_time")
        .Default(TDuration::Seconds(15));
    RegisterParameter("expire_after_failed_update_time", ExpireAfterFailedUpdateTime)
        .Alias("failure_expiration_time")
        .Default(TDuration::Seconds(15));
    RegisterParameter("refresh_time", RefreshTime)
        .Alias("success_probation_time")
        .Default(TDuration::Seconds(10));
    RegisterParameter("batch_update", BatchUpdate)
        .Default(false);

    RegisterPostprocessor([&] () {
        if (RefreshTime && *RefreshTime > ExpireAfterSuccessfulUpdateTime) {
            THROW_ERROR_EXCEPTION("\"refresh_time\" must be less than \"expire_after_successful_update_time\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
