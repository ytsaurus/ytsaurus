#pragma once

#include "private.h"

#include <ytlib/object_client/public.h>

#include <core/logging/log.h>
#include <core/concurrency/throughput_throttler.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TThrottlerManager
    : public TRefCounted
{
public:
    TThrottlerManager(
        NConcurrency::TThroughputThrottlerConfigPtr config,
        const NLogging::TLogger& logger = NLogging::TLogger(),
        const NProfiling::TProfiler& profiler = NProfiling::TProfiler());

    NConcurrency::IThroughputThrottlerPtr GetThrottler(NObjectClient::TCellTag cellTag);

private:
    const NConcurrency::TThroughputThrottlerConfigPtr Config_;
    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler_;

    //! Protects the section immediately following it.
    TSpinLock SpinLock_;
    yhash_map<NObjectClient::TCellTag, NConcurrency::IThroughputThrottlerPtr> ThrottlerMap_;
};

DEFINE_REFCOUNTED_TYPE(TThrottlerManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
