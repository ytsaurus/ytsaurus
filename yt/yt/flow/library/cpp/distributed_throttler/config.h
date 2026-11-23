#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/distributed_throttler/prefetching/config.h>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

struct TDistributedThrottlerServiceConfig
    : public NYTree::TYsonStruct
{
    //! Named throttlers: name -> token bucket and quota-class config.
    THashMap<TThrottlerId, TDistributedThrottlerBucketConfigPtr> Throttlers;

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

} // namespace NYT::NFlow::NDistributedThrottler
