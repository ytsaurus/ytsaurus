#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/job_throttler_factory.h>

#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

struct IWorkerThrottlerFactory
    : public TRefCounted
{
    virtual IJobThrottlerFactoryPtr CreateFactory(
        std::string clientId,
        IStatusProfilerPtr statusProfiler,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler) = 0;

    virtual void Reconfigure(
        const THashMap<NFlow::TThrottlerId, TDynamicThrottlerSpecPtr>& specs) = 0;
};

DEFINE_REFCOUNTED_TYPE(IWorkerThrottlerFactory);

IWorkerThrottlerFactoryPtr CreateWorkerThrottlerFactory(
    const THashMap<NFlow::TThrottlerId, TDynamicThrottlerSpecPtr>& specs,
    NRpc::IChannelPtr controllerChannel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
