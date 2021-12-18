
#pragma once

#include "private.h"

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TThrottlerManager
    : public TRefCounted
{
public:
    explicit TThrottlerManager(
        NConcurrency::TThroughputThrottlerConfigPtr config,
        NLogging::TLogger logger = {},
        NProfiling::TProfiler profiler = {});

    NConcurrency::IThroughputThrottlerPtr GetThrottler(NObjectClient::TCellTag cellTag);

    void Reconfigure(NConcurrency::TThroughputThrottlerConfigPtr config);

private:
    NConcurrency::TThroughputThrottlerConfigPtr Config_;
    const NLogging::TLogger Logger_;
    const NProfiling::TProfiler Profiler_;

    //! Protects the section immediately following it.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    THashMap<NObjectClient::TCellTag, NConcurrency::IReconfigurableThroughputThrottlerPtr> ThrottlerMap_;
};

DEFINE_REFCOUNTED_TYPE(TThrottlerManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
