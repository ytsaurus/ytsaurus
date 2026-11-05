#pragma once

#include "config.h"
#include "public.h"

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

//! RPC service that manages named distributed throttler buckets.
struct IDistributedThrottlerService
    : public virtual TRefCounted
{
    //! Reconfigure throttler limits (add/remove/change throttlers).
    virtual void Reconfigure(TDistributedThrottlerServiceConfigPtr config) = 0;

    //! Get the underlying RPC service for registration on an RPC server.
    virtual NRpc::IServicePtr GetRpcService() = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedThrottlerService);

IDistributedThrottlerServicePtr CreateDistributedThrottlerService(
    TDistributedThrottlerServiceConfigPtr config,
    IInvokerPtr invoker,
    NLogging::TLogger logger = {},
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
