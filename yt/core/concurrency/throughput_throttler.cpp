#include "throughput_throttler.h"
#include "periodic_executor.h"
#include "config.h"

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/singleton.h>

#include <yt/core/profiling/profiler.h>

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

    virtual bool TryAcquire(i64 count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(count >= 0);

        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (Available_ < count) {
                return false;
            }
            Available_ -= count;
        }

        Profiler.Increment(ValueCounter_, count);
        return true;
    }

    virtual void Acquire(i64 count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(count >= 0);

        {
            TGuard<TSpinLock> guard(SpinLock_);
            Available_ -= count;
        }

        Profiler.Increment(ValueCounter_, count);
    }

    virtual bool IsOverdraft() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock_);
        return Available_ < 0;
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

        std::vector<TPromise<void>> readyList;
        std::vector<TPromise<void>> canceledList;

        {
            TGuard<TSpinLock> guard(SpinLock_);
            Available_ += ThroughputPerPeriod_;
            if (Available_ > ThroughputPerPeriod_) {
                Available_ = ThroughputPerPeriod_;
            }
            while (!Requests_.empty() && Available_ > 0) {
                auto& request = Requests_.front();
                LOG_DEBUG("Finished waiting for throttler (Count: %v)", request.Count);
                if (request.Promise.IsCanceled()) {
                    canceledList.push_back(std::move(request.Promise));
                } else {
                    Available_ -= request.Count;
                    readyList.push_back(std::move(request.Promise));
                }
                Requests_.pop();
            }
        }

        for (auto& promise : readyList) {
            promise.Set();
        }

        for (auto& promise : canceledList) {
            promise.Set(TError(NYT::EErrorCode::Canceled, "Throttled request canceled"));
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

    virtual bool TryAcquire(i64 count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(count >= 0);

        return true;
    }

    virtual void Acquire(i64 count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(count >= 0);
    }

    virtual bool IsOverdraft() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return false;
    }
};

IThroughputThrottlerPtr GetUnlimitedThrottler()
{
    return RefCountedSingleton<TUnlimitedThroughtputThrottler>();
}

////////////////////////////////////////////////////////////////////////////////

class TCombinedThroughtputThrottler
    : public IThroughputThrottler
{
public:
    explicit TCombinedThroughtputThrottler(const std::vector<IThroughputThrottlerPtr>& throttlers)
        : Throttlers_(throttlers)
    { }

    virtual TFuture<void> Throttle(i64 count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(count >= 0);

        std::vector<TFuture<void>> asyncResults;
        for (const auto& throttler : Throttlers_) {
            asyncResults.push_back(throttler->Throttle(count));
        }
        return Combine(asyncResults);
    }

    virtual bool TryAcquire(i64 /*count*/) override
    {
        YUNREACHABLE();
    }

    virtual void Acquire(i64 count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(count >= 0);

        for (const auto& throttler : Throttlers_) {
            throttler->Acquire(count);
        }
    }

    virtual bool IsOverdraft() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        for (const auto& throttler : Throttlers_) {
            if (throttler->IsOverdraft()) {
                return true;
            }
        }
        return false;
    }

private:
    const std::vector<IThroughputThrottlerPtr> Throttlers_;

};

IThroughputThrottlerPtr CreateCombinedThrottler(
    const std::vector<IThroughputThrottlerPtr>& throttler)
{
    return New<TCombinedThroughtputThrottler>(throttler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
