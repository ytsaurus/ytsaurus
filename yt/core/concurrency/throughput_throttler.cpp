#include "stdafx.h"
#include "throughput_throttler.h"
#include "periodic_executor.h"

#include <core/misc/singleton.h>

#include <core/concurrency/thread_affinity.h>

#include <core/actions/invoker_util.h>

#include <core/profiling/profiler.h>

#include <queue>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TLimitedThroughputThrottler
    : public IThroughputThrottler
{
public:
    TLimitedThroughputThrottler(
        TThroughputThrottlerConfigPtr config,
        const NLogging::TLogger& logger,
        const NProfiling::TProfiler& profiler)
        : Config_(config)
        , Logger(logger)
        , Profiler(profiler)
        , ValueCounter_("/value")
    {
        if (Config_->Limit) {
            ThroughputPerPeriod_ = static_cast<i64>(Config_->Period.SecondsFloat() * (*Config_->Limit));
            Available_ = ThroughputPerPeriod_;

            PeriodicExecutor_ = New<TPeriodicExecutor>(
                GetSyncInvoker(),
                BIND(&TLimitedThroughputThrottler::OnTick, MakeWeak(this)),
                config->Period);
            PeriodicExecutor_->Start();
        }
    }

    virtual TFuture<void> Throttle(i64 count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(count >= 0);

        Profiler.Increment(ValueCounter_, count);

        if (count == 0 || !Config_->Limit) {
            return VoidFuture;
        }

        TGuard<TSpinLock> guard(SpinLock_);

        if (Available_ > 0) {
            // Execute immediately.
            Available_ -= count;
            return VoidFuture;
        }

        // Enqueue request to be executed later.
        LOG_DEBUG("Started waiting for throttler (Count: %v)", count);
        TRequest request{count, NewPromise<void>()};
        Requests_.push(request);
        return request.Promise;
    }

private:
    struct TRequest
    {
        i64 Count;
        TPromise<void> Promise;
    };

    TThroughputThrottlerConfigPtr Config_;
    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;
    NProfiling::TAggregateCounter ValueCounter_;

    i64 ThroughputPerPeriod_ = -1;
    TPeriodicExecutorPtr PeriodicExecutor_;

    //! Protects the section immediately following it.
    TSpinLock SpinLock_;
    i64 Available_ = -1;
    std::queue<TRequest> Requests_;


    void OnTick()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::vector<TPromise<void>> releaseList;

        {
            TGuard<TSpinLock> guard(SpinLock_);
            Available_ += ThroughputPerPeriod_;
            while (!Requests_.empty() && Available_ > 0) {
                auto& request = Requests_.front();
                LOG_DEBUG("Finished waiting for throttler (Count: %v)", request.Count);
                Available_ -= request.Count;
                releaseList.push_back(std::move(request.Promise));
                Requests_.pop();
            }
        }

        for (auto promise : releaseList) {
            promise.Set();
        }
    }
};

IThroughputThrottlerPtr CreateLimitedThrottler(
    TThroughputThrottlerConfigPtr config,
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler)
{
    return New<TLimitedThroughputThrottler>(
        config,
        logger,
        profiler);
}

////////////////////////////////////////////////////////////////////////////////

class TUnlimitedThroughtputThrottler
    : public IThroughputThrottler
{
public:
    virtual TFuture<void> Throttle(i64 count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(count >= 0);

        return VoidFuture;
    }
};

IThroughputThrottlerPtr GetUnlimitedThrottler()
{
    return RefCountedSingleton<TUnlimitedThroughtputThrottler>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
