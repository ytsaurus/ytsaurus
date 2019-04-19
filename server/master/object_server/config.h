#pragma once

#include "public.h"

#include <yt/core/misc/config.h>

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TObjectManagerConfig
    : public NYTree::TYsonSerializable
{ };

DEFINE_REFCOUNTED_TYPE(TObjectManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicObjectManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Maximum total weight of objects processed per a single GC mutation.
    int MaxWeightPerGCSweep;

    //! Period between subsequent GC queue checks.
    TDuration GCSweepPeriod;

    TDynamicObjectManagerConfig()
    {
        RegisterParameter("max_weight_per_gc_sweep", MaxWeightPerGCSweep)
            .Default(100000);
        RegisterParameter("gc_sweep_period", GCSweepPeriod)
            .Default(TDuration::MilliSeconds(1000));
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicObjectManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TObjectServiceConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Maximum amount of a single batch of Execute requests is allowed to occupy the automaton thread.
    TDuration YieldTimeout;

    //! The amount of time remaining to a batch request timeout when the object
    //! service shall try and send partial (subbatch) response.
    //! NB: this will have no effect if the request's timeout is shorter than this.
    TDuration TimeoutBackoffLeadTime;

    //! When user is banned or exceeds the queue size limit, Object Service replies
    //! with an error and caches this error for the given period of time.
    //! This helps to offload the Automaton Thread in case of DOS attack.
    TDuration StickyUserErrorExpireTime;

    TObjectServiceConfig()
    {
        RegisterParameter("yield_timeout", YieldTimeout)
            .Default(TDuration::MilliSeconds(10));

        RegisterParameter("timeout_backoff_lead_time", TimeoutBackoffLeadTime)
            .Default(TDuration::Seconds(5));

        RegisterParameter("sticky_user_error_expire_time", StickyUserErrorExpireTime)
            .Default(TDuration::Seconds(1));
    }
};

DEFINE_REFCOUNTED_TYPE(TObjectServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterCacheServiceConfig
    : public NRpc::TThrottlingChannelConfig
    , public TSlruCacheConfig
{
public:
    TMasterCacheServiceConfig()
    {
        RegisterPreprocessor([&] () {
            Capacity = 256_MB;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TMasterCacheServiceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
