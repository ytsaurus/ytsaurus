#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

struct TDistributedThrottlerServiceConfig
    : public NYTree::TYsonStruct
{
    //! Named throttlers: name -> token bucket config.
    //! If config has Limit = null, the throttler is unlimited.
    THashMap<TThrottlerId, NConcurrency::TThroughputThrottlerConfigPtr> Throttlers;

    //! Timeout for requests waiting in the priority queue.
    TDuration QueueTimeout;

    //! Period for the drain fiber polling.
    TDuration DrainPeriod;

    //! Response keeper config for idempotent retries.
    NRpc::TResponseKeeperConfigPtr ResponseKeeper;

    REGISTER_YSON_STRUCT(TDistributedThrottlerServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDistributedThrottlerServiceConfig);

////////////////////////////////////////////////////////////////////////////////

struct TDistributedThrottlerClientConfig
    : public NYTree::TYsonStruct
{
    //! Server address (host:port or local channel).
    std::string ServerAddress;

    //! Throttler name on the server.
    TThrottlerId ThrottlerName;

    //! Client identifier (used for logging/monitoring).
    std::string ClientId;

    //! Prefetching throttler config.
    NConcurrency::TPrefetchingThrottlerConfigPtr PrefetchingConfig;

    //! Retrying channel config (retries on RPC errors, with exponential backoff).
    NRpc::TRetryingChannelConfigPtr RetryingChannel;

    //! Per-RPC timeout.
    TDuration RpcTimeout;

    REGISTER_YSON_STRUCT(TDistributedThrottlerClientConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDistributedThrottlerClientConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
