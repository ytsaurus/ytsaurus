#pragma once

#include "private.h"

#include <yt/ytlib/object_client/public.h>

#include <yt/core/concurrency/config.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

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
    const NLogging::TLogger Logger_;
    const NProfiling::TProfiler Profiler_;

    //! Protects the section immediately following it.
    TSpinLock SpinLock_;
    yhash<NObjectClient::TCellTag, NConcurrency::IThroughputThrottlerPtr> ThrottlerMap_;
};

DEFINE_REFCOUNTED_TYPE(TThrottlerManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
