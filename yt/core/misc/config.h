#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSlruCacheConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! The maximum number of weight units cached items are allowed to occupy.
    //! Zero means that no items are cached.
    i64 Capacity;

    //! The fraction of total capacity given to the younger segment.
    double YoungerSizeFraction;

    //! Capacity of internal buffer used to amortize and de-contend touch operations.
    int TouchBufferCapacity;

    explicit TSlruCacheConfig(i64 capacity = 0)
    {
        RegisterParameter("capacity", Capacity)
            .Default(capacity)
            .GreaterThanOrEqual(0);
        RegisterParameter("younger_size_fraction", YoungerSizeFraction)
            .Default(0.25)
            .InRange(0.0, 1.0);
        RegisterParameter("touch_buffer_capacity", TouchBufferCapacity)
            .Default(65536)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TSlruCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TExpiringCacheConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Time since last Get() after which an entry is removed.
    TDuration ExpireAfterAccessTime;

    //! Time since last update, if succeeded, after which an entry is removed.
    TDuration ExpireAfterSuccessfulUpdateTime;

    //! Time since last update, if it failed, after which an entry is removed.
    TDuration ExpireAfterFailedUpdateTime;

    //! Time before next (background) update.
    TDuration RefreshTime;

    TExpiringCacheConfig()
    {
        RegisterParameter("expire_after_access_time", ExpireAfterAccessTime)
            .Default(TDuration::Seconds(300));
        RegisterParameter("expire_after_successful_update_time", ExpireAfterSuccessfulUpdateTime)
            //.Alias("success_expiration_time")
            .Default(TDuration::Seconds(15));
        RegisterParameter("expire_after_failed_update_time", ExpireAfterFailedUpdateTime)
            //.Alias("failure_expiration_time")
            .Default(TDuration::Seconds(15));
        RegisterParameter("refresh_time", RefreshTime)
            //.Alias("success_probation_time")
            .Default(TDuration::Seconds(10));

        // TODO(savrus): remove this in 18.0 and use Alias.
        RegisterParameter("success_expiration_time", ExpireAfterSuccessfulUpdateTime)
            .Default(TDuration::Seconds(15));
        RegisterParameter("failure_expiration_time", ExpireAfterFailedUpdateTime)
            .Default(TDuration::Seconds(15));
        RegisterParameter("success_probation_time", RefreshTime)
            .Default(TDuration::Seconds(10));

        RegisterValidator([&] () {
            if (RefreshTime > ExpireAfterSuccessfulUpdateTime) {
                THROW_ERROR_EXCEPTION("\"refresh_time\" must be less than \"expire_after_successful_update_time\"");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TExpiringCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
