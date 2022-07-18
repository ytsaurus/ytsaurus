#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TThroughputThrottlerConfig
    : public NYTree::TYsonSerializable
{
public:
    explicit TThroughputThrottlerConfig(
        std::optional<double> limit = std::nullopt,
        TDuration period = TDuration::MilliSeconds(1000));
    //! Limit on average throughput (per sec). Null means unlimited.
    std::optional<double> Limit;

    //! Period for leaky bucket algorithm.
    TDuration Period;
};

DEFINE_REFCOUNTED_TYPE(TThroughputThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

//! This wrapper may be helpful if we have some parametrization over the limit
//! (e.g. in network bandwith limit on nodes).
//! The exact logic of limit/relative_limit clash resolution
//! and the parameter access are external to the config itself.
class TRelativeThroughputThrottlerConfig
    : public TThroughputThrottlerConfig
{
public:
    explicit TRelativeThroughputThrottlerConfig(
        std::optional<double> limit = std::nullopt,
        TDuration period = TDuration::MilliSeconds(1000));

    std::optional<double> RelativeLimit;
};

DEFINE_REFCOUNTED_TYPE(TRelativeThroughputThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

class TPrefetchingThrottlerConfig
    : public NYTree::TYsonSerializable
{
public:
    TPrefetchingThrottlerConfig();

    //! RPS limit for requests to the underlying throttler.
    double TargetRps;

    //! Minimum amount to be prefetched from the underlying throttler.
    i64 MinPrefetchAmount;

    //! Maximum amount to be prefetched from the underlying throttler.
    //! Guards from a uncontrolled growth of the requested amount.
    i64 MaxPrefetchAmount;

    //! Time window for the RPS estimation.
    TDuration Window;

    //! Enable the prefetching throttler.
    //! If disabled #CreatePrefetchingThrottler() will not create #TPrefetchingThrottler
    //! and will return the underlying throttler instead.
    //! #TPrefetchingThrottler itself does not check this field.
    bool Enable;
};

DEFINE_REFCOUNTED_TYPE(TPrefetchingThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
