#pragma once

#include "public.h"

#include <yt/core/misc/config.h>

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TObjectManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Maximum total weight of objects processed per a single GC mutation.
    int MaxWeightPerGCSweep;

    //! Period between subsequent GC queue checks.
    TDuration GCSweepPeriod;

    //! Amount of time to wait before yielding meta state thread to another request.
    TDuration YieldTimeout;

    TObjectManagerConfig()
    {
        RegisterParameter("max_weight_per_gc_sweep", MaxWeightPerGCSweep)
            .Default(100000);
        RegisterParameter("gc_sweep_period", GCSweepPeriod)
            .Default(TDuration::MilliSeconds(1000));
        RegisterParameter("yield_timeout", YieldTimeout)
            .Default(TDuration::MilliSeconds(10));
    }
};

DEFINE_REFCOUNTED_TYPE(TObjectManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterCacheServiceConfig
    : public NRpc::TThrottlingChannelConfig
    , public TSlruCacheConfig
{
public:
    TMasterCacheServiceConfig()
    {
        RegisterInitializer([&] () {
            Capacity = (i64) 16 * 1024 * 1024;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TMasterCacheServiceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
