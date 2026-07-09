#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <atomic>
#include <queue>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

class TDistributedThrottlerBucket
    : public TRefCounted
{
public:
    TDistributedThrottlerBucket(
        NConcurrency::TThroughputThrottlerConfigPtr config,
        TDuration drainPeriod,
        IInvokerPtr invoker,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler = {});

    ~TDistributedThrottlerBucket();

    //! Spawns the drain fiber.
    void Start();

    //! Enqueues a quota request; resolves when the bucket grants it.
    //! RPC-retry idempotency is the service's job, not this one (see
    //! IResponseKeeper in the surrounding service).
    TFuture<void> RequestQuota(
        const std::string& clientId,
        i64 amount,
        ui64 timestamp);

    //! Updates the token bucket limit/period.
    void Reconfigure(NConcurrency::TThroughputThrottlerConfigPtr config);

    //! Picked up by the drain fiber on the next idle cycle.
    void SetDrainPeriod(TDuration drainPeriod);

    //! Stops the drain fiber.
    void Stop();

    //! For tests: exposes the token bucket so they can fake time via SetLastUpdated().
    NConcurrency::IReconfigurableThroughputThrottlerPtr GetTokenBucket() const;

private:
    struct TPendingRequest
    {
        ui64 Timestamp;
        TInstant EnqueueTime;
        std::string ClientId;
        i64 Amount;
        TPromise<void> Promise;

        bool operator>(const TPendingRequest& other) const;
    };

    static void DrainLoop(TWeakPtr<TDistributedThrottlerBucket> weakThis);

    std::atomic<TDuration> DrainPeriod_;
    const IInvokerPtr Invoker_;
    const NLogging::TLogger Logger;

    NConcurrency::IReconfigurableThroughputThrottlerPtr TokenBucket_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::priority_queue<TPendingRequest, std::vector<TPendingRequest>, std::greater<TPendingRequest>> PendingRequests_;

    TFuture<void> DrainLoopResult_;
};

DEFINE_REFCOUNTED_TYPE(TDistributedThrottlerBucket);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
