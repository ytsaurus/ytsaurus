#pragma once

#include "config.h"
#include "public.h"

#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

//! Owns distributed throttler clients for a single computation:
//! looks them up by name, reconfigures them, and feeds them a priority key.
struct IDistributedThrottlerFactory
    : public TRefCounted
{
    //! Returns a stable handle for the given throttler, creating it on first
    //! call. The handle survives Reconfigure: when the underlying client is
    //! rebuilt, the same pointer keeps working with the new config. Safe to
    //! cache in user code.
    //! Throws if the name is not in the current configs.
    virtual NConcurrency::IThroughputThrottlerPtr GetClient(
        std::string_view throttlerName) = 0;

    //! Priority key attached to every subsequent RequestQuota RPC.
    //! Smaller value == higher priority on the server's queue.
    virtual void SetPriority(TPriority priority) = 0;

    //! Replaces the throttler configs. Handles whose spec is unchanged keep
    //! their prefetch state; changed ones get a freshly-built underlying
    //! client swapped in atomically. Handles for names dropped from the new
    //! config keep responding but throw on every call until the name reappears.
    virtual void Reconfigure(
        THashMap<TThrottlerId, TDynamicThrottlerSpecPtr> throttlers) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedThrottlerFactory);

////////////////////////////////////////////////////////////////////////////////

//! |channelProvider| is called per RPC; typically returns a channel to the
//! current controller leader, null when disconnected.
//! |clientId| is reported to the server on each RequestQuota.
//! |initialThrottlers| is the starting set of configs; can be replaced via
//! Reconfigure.
IDistributedThrottlerFactoryPtr CreateDistributedThrottlerFactory(
    std::function<NRpc::IChannelPtr()> channelProvider,
    std::string clientId,
    THashMap<TThrottlerId, TDynamicThrottlerSpecPtr> initialThrottlers,
    IStatusProfilerPtr statusProfiler,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
