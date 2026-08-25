#pragma once

#include <library/cpp/yt/error/error.h>

#include "config.h"
#include "weighted_fair_queue_scheduler.h"

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/threading/atomic_object.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <atomic>
#include <memory>
#include <queue>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

class TDistributedThrottlerBucket
    : public TRefCounted
{
public:
    TDistributedThrottlerBucket(
        TDistributedThrottlerBucketConfigPtr config,
        TDuration drainPeriod,
        IInvokerPtr invoker,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler = {});

    ~TDistributedThrottlerBucket() override;

    //! Spawns the drain fiber.
    void Start();

    //! Enqueues a quota request; resolves when the bucket grants it.
    //! RPC-retry idempotency is the service's job, not this one (see
    //! IResponseKeeper in the surrounding service).
    TFuture<void> RequestQuota(
        const std::string& clientId,
        const TQuotaClassId& quotaClassId,
        i64 amount,
        ui64 timestamp);

    //! Updates token-bucket and quota-class settings.
    void Reconfigure(TDistributedThrottlerBucketConfigPtr config);

    //! Picked up by the drain fiber on the next idle cycle.
    void SetDrainPeriod(TDuration drainPeriod);

    //! Stops the drain fiber.
    void Stop();

    //! For tests: exposes the token bucket so they can fake time via SetLastUpdated().
    NConcurrency::IReconfigurableThroughputThrottlerPtr GetTokenBucket() const;

private:
    //! Token-bucket wait of the chunk currently in flight, cancelled together
    //! with the request. Null while nothing is in flight.
    using TThrottleHolderPtr = std::shared_ptr<NThreading::TAtomicObject<TFuture<void>>>;

    struct TPendingRequest
    {
        ui64 Timestamp;
        TInstant EnqueueTime;
        std::string ClientId;
        i64 Amount;
        TPromise<void> Promise;
        bool Started = false;
        //! Total amount already granted to this request by previous chunks;
        //! refunded to the token bucket if the request dies before completion.
        i64 Granted = 0;
        //! Virtual time actually charged for those chunks. Kept alongside
        //! #Granted because chunks may have been charged at different weights.
        double ChargedVirtualTime = 0.0;
        TThrottleHolderPtr ThrottleHolder;

        bool operator>(const TPendingRequest& other) const;
    };

    using TRequestQueue = std::priority_queue<
        TPendingRequest,
        std::vector<TPendingRequest>,
        std::greater<TPendingRequest>>;

    struct TClassQueue
    {
        TRequestQueue Requests;
        bool InFlight = false;
        i64 PendingAmount = 0;
        i64 PendingRequestCount = 0;
        NProfiling::TCounter GrantedCounter;
        NProfiling::TCounter RefundedCounter;
        NProfiling::TGauge PendingRequestsGauge;
        NProfiling::TGauge PendingAmountGauge;
        NProfiling::TEventTimer WaitTimeTimer;
        NProfiling::TGauge WeightGauge;
    };

    struct TDispatch
    {
        TQuotaClassId ClassId;
        TPendingRequest Request;
        i64 GrantAmount;
        bool Final;
        double ChargeWeight;
    };

    static void DrainLoop(TWeakPtr<TDistributedThrottlerBucket> weakThis);

    TQuotaClassId ResolveClassId(const TQuotaClassId& quotaClassId);
    TClassQueue& EnsureClassQueue(const TQuotaClassId& classId, double weight);
    void UpdatePendingSensors(TClassQueue& classQueue);
    void RefreshClassActivity(const TQuotaClassId& classId);
    std::optional<TDispatch> TryTakeDispatch();
    void CompleteDispatch(TDispatch& dispatch, const TError& error);
    void RefundGranted(TClassQueue& classQueue, const TQuotaClassId& classId, const TPendingRequest& request);
    void MaybeRemoveRetiredClass(const TQuotaClassId& classId);
    void RemoveDrainedRetiredClasses();

    std::atomic<TDuration> DrainPeriod_;
    const IInvokerPtr Invoker_;
    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler_;

    NConcurrency::IReconfigurableThroughputThrottlerPtr TokenBucket_;
    NProfiling::TCounter UnknownClassRequestsCounter_;
    NProfiling::TCounter ClasslessRequestsCounter_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TQuotaClassId, TClassQueue> ClassQueues_;
    TWeightedFairQueueScheduler Scheduler_;
    std::optional<i64> MaxGrantAmount_;
    bool HasConfiguredClasses_ = false;

    TFuture<void> DrainLoopResult_;
};

DEFINE_REFCOUNTED_TYPE(TDistributedThrottlerBucket);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
