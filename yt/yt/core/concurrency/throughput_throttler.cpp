#include "throughput_throttler.h"
#include "config.h"

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/singleton.h>

#include <yt/core/profiling/profiler.h>

#include <queue>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TThrottlerRequest)

struct TThrottlerRequest
    : public TRefCounted
{
    explicit TThrottlerRequest(i64 count)
        : Count(count)
    { }

    i64 Count;
    TPromise<void> Promise;
    std::atomic_flag Set = ATOMIC_FLAG_INIT;
};

DEFINE_REFCOUNTED_TYPE(TThrottlerRequest)

class TReconfigurableThroughputThrottler
    : public IReconfigurableThroughputThrottler
{
public:
    TReconfigurableThroughputThrottler(
        TThroughputThrottlerConfigPtr config,
        const NLogging::TLogger& logger,
        const NProfiling::TProfiler& profiler)
        : Logger(logger)
        , Profiler(profiler)
        , ValueCounter_("/value")
        , QueueSizeCounter_("/queue_size")

    {
        Reconfigure(config);
    }

    virtual TFuture<void> Throttle(i64 count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(count >= 0);

        // Fast lane.
        if (count == 0) {
            return VoidFuture;
        }

        Profiler.Increment(ValueCounter_, count);

        if (Limit_.load() < 0) {
            return VoidFuture;
        }

        while (true) {
            TryUpdateAvailable();
            auto available = Available_.load();
            if (available <= 0) {
                break;
            }
            if (Available_.compare_exchange_strong(available, available - count)) {
                return VoidFuture;
            }
        }

        // Slow lane.
        TGuard<TSpinLock> guard(SpinLock_);

        if (!Limit_.load()) {
            return VoidFuture;
        }

        // Enqueue request to be executed later.
        YT_LOG_DEBUG("Started waiting for throttler (Count: %v)", count);
        auto promise = NewPromise<void>();
        auto request = New<TThrottlerRequest>(count);
        promise.OnCanceled(BIND([weakRequest = MakeWeak(request), count, this, this_ = MakeStrong(this)] (const TError& error) {
            auto request = weakRequest.Lock();
            if (request && !request->Set.test_and_set()) {
                request->Promise.Set(TError(NYT::EErrorCode::Canceled, "Throttled request canceled")
                    << error);
                QueueTotalCount_ -= count;
                Profiler.Update(QueueSizeCounter_, QueueTotalCount_);
            }
        }));

        request->Promise = std::move(promise);
        Requests_.push(request);
        QueueTotalCount_ += count;
        Profiler.Update(QueueSizeCounter_, QueueTotalCount_);

        ScheduleUpdate();

        return request->Promise;
    }

    virtual bool TryAcquire(i64 count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(count >= 0);

        // Fast lane (only).
        if (count == 0) {
            return true;
        }

        if (Limit_.load() >= 0) {
            while (true) {
                TryUpdateAvailable();
                auto available = Available_.load();
                if (available <= 0) {
                    return false;
                }
                if (Available_.compare_exchange_weak(available, available - count)) {
                    break;
                }
            }
        }

        Profiler.Increment(ValueCounter_, count);
        return true;
    }

    virtual i64 TryAcquireAvailable(i64 count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(count >= 0);

        // Fast lane (only).
        if (count == 0) {
            return 0;
        }

        if (Limit_.load() >= 0) {
            while (true) {
                TryUpdateAvailable();
                auto available = Available_.load();
                if (available <= 0) {
                    return 0;
                }
                i64 acquire = std::min(count, available);
                if (Available_.compare_exchange_weak(available, available - acquire)) {
                    count = acquire;
                    break;
                }
            }
        }

        Profiler.Increment(ValueCounter_, count);
        return count;
    }

    virtual void Acquire(i64 count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(count >= 0);

        // Fast lane (only).
        if (count == 0) {
            return;
        }

        TryUpdateAvailable();
        if (Limit_.load() >= 0) {
            Available_ -= count;
        }

        Profiler.Increment(ValueCounter_, count);
    }

    virtual bool IsOverdraft() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // Fast lane (only).
        TryUpdateAvailable();
        return Available_ < 0;
    }

    virtual void Reconfigure(TThroughputThrottlerConfigPtr config) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // Slow lane (only).
        TGuard<TSpinLock> guard(SpinLock_);

        auto limit = config->Limit;
        Limit_ = limit.value_or(-1);
        TDelayedExecutor::CancelAndClear(UpdateCookie_);
        auto now = NProfiling::GetInstant();
        if (limit) {
            Period_ = config->Period;
            auto millisecondsPassed = (now - LastUpdated_.load()).MilliSeconds();
            auto newAvailable = Available_.load() + static_cast<i64>(millisecondsPassed * *limit / 1000);
            newAvailable = std::min(newAvailable, static_cast<i64>(Period_.load().SecondsFloat()) * *limit);
            Available_ = newAvailable;
        } else {
            Available_ = 0;
        }
        LastUpdated_ = now;
        ProcessRequests(std::move(guard));
    }

    virtual i64 GetQueueTotalCount() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // Fast lane (only).
        return QueueTotalCount_;
    }

private:
    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler;

    NProfiling::TMonotonicCounter ValueCounter_;
    NProfiling::TSimpleGauge QueueSizeCounter_;

    std::atomic<TInstant> LastUpdated_ = TInstant::Zero();
    std::atomic<i64> Available_ = {0};
    std::atomic<i64> QueueTotalCount_ = {0};

    //! Protects the section immediately following it.
    TSpinLock SpinLock_;
    // -1 indicates no limit
    std::atomic<i64> Limit_;
    std::atomic<TDuration> Period_;
    TDelayedExecutorCookie UpdateCookie_;

    std::queue<TThrottlerRequestPtr> Requests_;

    void ScheduleUpdate()
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        if (UpdateCookie_) {
            return;
        }

        auto limit = Limit_.load();
        YT_VERIFY(limit >= 0);

        auto delay = (-Available_ + limit) * 1000 / limit;
        if (delay < 0) {
            delay = 0;
        }

        UpdateCookie_ = TDelayedExecutor::Submit(
            BIND(&TReconfigurableThroughputThrottler::Update, MakeWeak(this)),
            TDuration::MilliSeconds(delay));
    }

    void TryUpdateAvailable()
    {
        auto limit = Limit_.load();
        if (limit < 0) {
            return;
        }

        auto period = Period_.load();
        auto current = NProfiling::GetInstant();
        auto lastUpdated = LastUpdated_.load();

        auto millisecondsPassed = (current - lastUpdated).MilliSeconds();
        auto deltaAvailable = static_cast<i64>(millisecondsPassed * limit / 1000);
        if (deltaAvailable == 0) {
            return;
        }

        current = lastUpdated + TDuration::MilliSeconds(deltaAvailable * 1000 / limit);
        if (LastUpdated_.compare_exchange_strong(lastUpdated, current)) {
            auto available = Available_.load();
            auto throughputPerPeriod = static_cast<i64>(period.SecondsFloat()) * limit;

            while (true) {
                auto newAvailable = available + deltaAvailable;
                if (newAvailable > throughputPerPeriod) {
                    newAvailable = throughputPerPeriod;
                }
                if (Available_.compare_exchange_weak(available, newAvailable)) {
                    break;
                }
            }
        }
    }

    void Update()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock_);
        UpdateCookie_.Reset();
        TryUpdateAvailable();

        ProcessRequests(std::move(guard));
    }

    void ProcessRequests(TGuard<TSpinLock> guard)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        std::vector<TThrottlerRequestPtr> readyList;

        auto limit = Limit_.load();
        while (!Requests_.empty() && (limit < 0 || Available_ > 0)) {
            const auto& request = Requests_.front();
            if (!request->Set.test_and_set()) {
                YT_LOG_DEBUG("Finished waiting for throttler (Count: %v)", request->Count);
                if (limit) {
                    Available_ -= request->Count;
                }
                readyList.push_back(request);
                QueueTotalCount_ -= request->Count;
                Profiler.Update(QueueSizeCounter_, QueueTotalCount_);
            }
            Requests_.pop();
        }

        if (!Requests_.empty()) {
            ScheduleUpdate();
        }

        guard.Release();

        for (const auto& request : readyList) {
            request->Promise.Set();
        }
    }

};

IReconfigurableThroughputThrottlerPtr CreateReconfigurableThroughputThrottler(
    TThroughputThrottlerConfigPtr config,
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler)
{
    return New<TReconfigurableThroughputThrottler>(
        config,
        logger,
        profiler);
}

IReconfigurableThroughputThrottlerPtr CreateNamedReconfigurableThroughputThrottler(
    TThroughputThrottlerConfigPtr config,
    const TString& name,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler)
{
    logger.AddTag("Throttler: %v", name);
    profiler.SetPathPrefix(profiler.GetPathPrefix() + "/" + CamelCaseToUnderscoreCase(name));

    return CreateReconfigurableThroughputThrottler(config, logger, profiler);
};

////////////////////////////////////////////////////////////////////////////////

class TUnlimitedThroughtputThrottler
    : public IThroughputThrottler
{
public:
    explicit TUnlimitedThroughtputThrottler(
        const NProfiling::TProfiler& profiler = NProfiling::TProfiler())
        : Profiler(profiler)
        , ValueCounter_("/value")
    { }

    virtual TFuture<void> Throttle(i64 count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(count >= 0);

        Profiler.Increment(ValueCounter_, count);
        return VoidFuture;
    }

    virtual bool TryAcquire(i64 count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(count >= 0);

        Profiler.Increment(ValueCounter_, count);
        return true;
    }

    virtual i64 TryAcquireAvailable(i64 count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(count >= 0);

        Profiler.Increment(ValueCounter_, count);
        return count;
    }

    virtual void Acquire(i64 count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(count >= 0);

        Profiler.Increment(ValueCounter_, count);
    }

    virtual bool IsOverdraft() override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return false;
    }

    virtual i64 GetQueueTotalCount() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return 0;
    }

private:
    const NProfiling::TProfiler Profiler;
    NProfiling::TMonotonicCounter ValueCounter_;
};

IThroughputThrottlerPtr GetUnlimitedThrottler()
{
    return RefCountedSingleton<TUnlimitedThroughtputThrottler>();
}

IThroughputThrottlerPtr CreateNamedUnlimitedThroughputThrottler(
    const TString& name,
    NProfiling::TProfiler profiler)
{
    profiler.SetPathPrefix(profiler.GetPathPrefix() + "/" + CamelCaseToUnderscoreCase(name));

    return New<TUnlimitedThroughtputThrottler>(profiler);
};

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
        YT_VERIFY(count >= 0);

        QueueTotalCount_ += count;

        std::vector<TFuture<void>> asyncResults;
        for (const auto& throttler : Throttlers_) {
            asyncResults.push_back(throttler->Throttle(count));
        }

        return Combine(asyncResults).Apply(BIND([weakThis = MakeWeak(this), count] {
            if (auto this_ = weakThis.Lock()) {
                this_->QueueTotalCount_ -= count;
            }
        }));
    }

    virtual bool TryAcquire(i64 /*count*/) override
    {
        YT_ABORT();
    }

    virtual i64 TryAcquireAvailable(i64 /*count*/) override
    {
        YT_ABORT();
    }

    virtual void Acquire(i64 count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(count >= 0);

        for (const auto& throttler : Throttlers_) {
            throttler->Acquire(count);
        }
    }

    virtual bool IsOverdraft() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        for (const auto& throttler : Throttlers_) {
            if (throttler->IsOverdraft()) {
                return true;
            }
        }
        return false;
    }

    virtual i64 GetQueueTotalCount() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return QueueTotalCount_;
    }

private:
    const std::vector<IThroughputThrottlerPtr> Throttlers_;

    std::atomic<i64> QueueTotalCount_ = {0};
};

IThroughputThrottlerPtr CreateCombinedThrottler(
    const std::vector<IThroughputThrottlerPtr>& throttlers)
{
    return New<TCombinedThroughtputThrottler>(throttlers);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

