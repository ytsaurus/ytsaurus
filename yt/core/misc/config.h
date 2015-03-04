#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSlruCacheConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! The maximum number of bytes that blocks are allowed to occupy.
    //! Zero means that no blocks are cached.
    i64 Capacity;

    //! The fraction of total capacity given to the younger segment.
    double YoungerSizeFraction;

    explicit TSlruCacheConfig(i64 capacity = 0)
    {
        RegisterParameter("capacity", Capacity)
            .Default(capacity)
            .GreaterThanOrEqual(0);
        RegisterParameter("younger_size_fraction", YoungerSizeFraction)
            .Default(0.25)
            .InRange(0.0, 1.0);
    }
};

DEFINE_REFCOUNTED_TYPE(TSlruCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TExpiringCacheConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TDuration SuccessExpirationTime;
    TDuration SuccessProbationTime;
    TDuration FailureExpirationTime;

    TExpiringCacheConfig()
    {
        RegisterParameter("success_expiration_time", SuccessExpirationTime)
            .Default(TDuration::Seconds(15));
        RegisterParameter("success_probation_time", SuccessProbationTime)
            .Default(TDuration::Seconds(10));
        RegisterParameter("failure_expiration_time", FailureExpirationTime)
            .Default(TDuration::Seconds(15));

        RegisterValidator([&] () {
            if (SuccessProbationTime > SuccessExpirationTime) {
                THROW_ERROR_EXCEPTION("\"success_probation_time\" must be less than \"success_expiration_time\"");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TExpiringCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
