#include "throughput_throttler.h"
#include "config.h"

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/singleton.h>

#include <yt/yt/core/profiling/timing.h>

#include <queue>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TThrottlerRequest)

struct TThrottlerRequest
    : public TRefCounted
{
    explicit TThrottlerRequest(i64 amount)
        : Amount(amount)
    { }

    i64 Amount;
    TPromise<void> Promise;
    std::atomic_flag Set = ATOMIC_FLAG_INIT;
    NProfiling::TCpuInstant StartTime = NProfiling::GetCpuInstant();
};

DEFINE_REFCOUNTED_TYPE(TThrottlerRequest)

////////////////////////////////////////////////////////////////////////////////

class TReconfigurableThroughputThrottler
    : public IReconfigurableThroughputThrottler
{
public:
    TReconfigurableThroughputThrottler(
        TThroughputThrottlerConfigPtr config,
        const NLogging::TLogger& logger,
        const NProfiling::TProfiler& profiler)
        : Logger(logger)
        , ValueCounter_(profiler.Counter("/value"))
        , QueueSizeCounter_(profiler.Gauge("/queue_size"))
        , WaitTimer_(profiler.Timer("/wait_time"))
    {
        Reconfigure(config);
    }

    TFuture<void> GetAvailableFuture() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DoThrottle(0);
    }

    TFuture<void> Throttle(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        // Fast lane.
        if (amount == 0) {
            return VoidFuture;
        }

        return DoThrottle(amount);
    }

    bool TryAcquire(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        // Fast lane (only).
        if (amount == 0) {
            return true;
        }

        if (Limit_.load() >= 0) {
            while (true) {
                TryUpdateAvailable();
                auto available = Available_.load();
                if (available < 0) {
                    return false;
                }
                if (Available_.compare_exchange_weak(available, available - amount)) {
                    break;
                }
            }
        }

        ValueCounter_.Increment(amount);
        return true;
    }

    i64 TryAcquireAvailable(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        // Fast lane (only).
        if (amount == 0) {
            return 0;
        }

        if (Limit_.load() >= 0) {
            while (true) {
                TryUpdateAvailable();
                auto available = Available_.load();
                if (available < 0) {
                    return 0;
                }
                i64 acquire = std::min(amount, available);
                if (Available_.compare_exchange_weak(available, available - acquire)) {
                    amount = acquire;
                    break;
                }
            }
        }

        ValueCounter_.Increment(amount);
        return amount;
    }

    void Acquire(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        // Fast lane (only).
        if (amount == 0) {
            return;
        }

        TryUpdateAvailable();
        if (Limit_.load() >= 0) {
            Available_ -= amount;
        }

        ValueCounter_.Increment(amount);
    }

    bool IsOverdraft() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // Fast lane (only).
        TryUpdateAvailable();

        if (Limit_.load() < 0) {
            return false;
        }

        return Available_ <= 0;
    }

    void Reconfigure(TThroughputThrottlerConfigPtr config) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        DoReconfigure(config->Limit, config->Period);
    }

    void SetLimit(std::optional<double> limit) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        DoReconfigure(limit, Period_);
    }

    i64 GetQueueTotalAmount() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // Fast lane (only).
        return QueueTotalAmount_;
    }

    TDuration GetEstimatedOverdraftDuration() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto queueTotalCount = QueueTotalAmount_.load();
        auto limit = Limit_.load();
        if (queueTotalCount == 0 || limit <= 0) {
            return TDuration::Zero();
        }

        return queueTotalCount / limit * TDuration::Seconds(1);
    }

private:
    const NLogging::TLogger Logger;

    NProfiling::TCounter ValueCounter_;
    NProfiling::TGauge QueueSizeCounter_;
    NProfiling::TEventTimer WaitTimer_;

    std::atomic<TInstant> LastUpdated_ = TInstant::Zero();
    std::atomic<i64> Available_ = 0;
    std::atomic<i64> QueueTotalAmount_ = 0;

    //! Protects the section immediately following it.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    // -1 indicates no limit
    std::atomic<double> Limit_;
    std::atomic<TDuration> Period_;
    TDelayedExecutorCookie UpdateCookie_;

    std::queue<TThrottlerRequestPtr> Requests_;

    TFuture<void> DoThrottle(i64 amount)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ValueCounter_.Increment(amount);
        if (Limit_.load() < 0) {
            return VoidFuture;
        }

        while (true) {
            TryUpdateAvailable();

            if (QueueTotalAmount_ > 0) {
                break;
            }

            auto available = Available_.load();
            if (available <= 0) {
                break;
            }

            if (Available_.compare_exchange_strong(available, available - amount)) {
                return VoidFuture;
            }
        }

        // Slow lane.
        auto guard = Guard(SpinLock_);

        if (Limit_.load() < 0) {
            return VoidFuture;
        }

        // Enqueue request to be executed later.
        YT_LOG_DEBUG("Started waiting for throttler (Amount: %v)", amount);
        auto promise = NewPromise<void>();
        auto request = New<TThrottlerRequest>(amount);
        promise.OnCanceled(BIND([weakRequest = MakeWeak(request), amount, this, this_ = MakeStrong(this)] (const TError& error) {
            auto request = weakRequest.Lock();
            if (request && !request->Set.test_and_set()) {
                request->Promise.Set(TError(NYT::EErrorCode::Canceled, "Throttled request canceled")
                    << error);
                QueueTotalAmount_ -= amount;
                QueueSizeCounter_.Update(QueueTotalAmount_);
            }
        }));

        request->Promise = std::move(promise);
        Requests_.push(request);
        QueueTotalAmount_ += amount;
        QueueSizeCounter_.Update(QueueTotalAmount_);

        ScheduleUpdate();

        return request->Promise;
    }

    void DoReconfigure(std::optional<double> limit, TDuration period)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // Slow lane (only).
        auto guard = Guard(SpinLock_);

        Limit_ = limit.value_or(-1);
        Period_ = period;
        TDelayedExecutor::CancelAndClear(UpdateCookie_);
        auto now = GetInstant();
        if (limit && *limit > 0) {
            auto lastUpdated = LastUpdated_.load();
            auto maxAvailable = static_cast<i64>(Period_.load().SecondsFloat()) * *limit;

            if (lastUpdated == TInstant::Zero()) {
                Available_ = maxAvailable;
                LastUpdated_ = now;
            } else {
                auto millisecondsPassed = (now - lastUpdated).MilliSeconds();
                auto deltaAvailable = static_cast<i64>(millisecondsPassed * *limit / 1000);
                auto newAvailable = Available_.load() + deltaAvailable;
                if (newAvailable > maxAvailable) {
                    LastUpdated_ = now;
                    newAvailable = maxAvailable;
                } else {
                    LastUpdated_ = lastUpdated + TDuration::MilliSeconds(deltaAvailable * 1000 / *limit);
                    // Just in case.
                    LastUpdated_ = std::min(LastUpdated_.load(), now);
                }
                Available_ = newAvailable;
            }
        } else {
            Available_ = 0;
            LastUpdated_ = now;
        }
        ProcessRequests(std::move(guard));
    }

    void ScheduleUpdate()
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        if (UpdateCookie_) {
            return;
        }

        auto limit = Limit_.load();
        YT_VERIFY(limit >= 0);

        auto delay = Max<i64>(0, -Available_ * 1000 / limit);
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
        auto current = GetInstant();
        auto lastUpdated = LastUpdated_.load();

        auto millisecondsPassed = (current - lastUpdated).MilliSeconds();
        auto deltaAvailable = static_cast<i64>(millisecondsPassed * limit / 1000);

        if (deltaAvailable == 0) {
            return;
        }

        current = lastUpdated + TDuration::MilliSeconds(deltaAvailable * 1000 / limit);

        if (LastUpdated_.compare_exchange_strong(lastUpdated, current)) {
            auto available = Available_.load();
            auto throughputPerPeriod = static_cast<i64>(period.SecondsFloat() * limit);

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

        auto guard = Guard(SpinLock_);
        UpdateCookie_.Reset();
        TryUpdateAvailable();

        ProcessRequests(std::move(guard));
    }

    void ProcessRequests(TGuard<NThreading::TSpinLock> guard)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        std::vector<TThrottlerRequestPtr> readyList;

        auto limit = Limit_.load();
        while (!Requests_.empty() && (limit < 0 || Available_ >= 0)) {
            const auto& request = Requests_.front();
            if (!request->Set.test_and_set()) {
                auto waitTime = NProfiling::CpuDurationToDuration(NProfiling::GetCpuInstant() - request->StartTime);
                YT_LOG_DEBUG("Finished waiting for throttler (Amount: %v, WaitTime: %v)",
                    request->Amount,
                    waitTime);

                if (limit) {
                    Available_ -= request->Amount;
                }
                readyList.push_back(request);
                QueueTotalAmount_ -= request->Amount;
                QueueSizeCounter_.Update(QueueTotalAmount_);
                WaitTimer_.Record(waitTime);
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
    return CreateReconfigurableThroughputThrottler(
        std::move(config),
        logger.WithTag("Throttler: %v", name),
        profiler.WithPrefix("/" + CamelCaseToUnderscoreCase(name)));
};

////////////////////////////////////////////////////////////////////////////////

class TUnlimitedThroughtputThrottler
    : public IThroughputThrottler
{
public:
    explicit TUnlimitedThroughtputThrottler(
        const NProfiling::TProfiler& profiler = {})
        : ValueCounter_(profiler.Counter("/value"))
    { }

    TFuture<void> Throttle(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        ValueCounter_.Increment(amount);
        return VoidFuture;
    }

    bool TryAcquire(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        ValueCounter_.Increment(amount);
        return true;
    }

    i64 TryAcquireAvailable(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        ValueCounter_.Increment(amount);
        return amount;
    }

    void Acquire(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        ValueCounter_.Increment(amount);
    }

    bool IsOverdraft() override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return false;
    }

    i64 GetQueueTotalAmount() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return 0;
    }

    TDuration GetEstimatedOverdraftDuration() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return TDuration::Zero();
    }

private:
    NProfiling::TCounter ValueCounter_;
};

IThroughputThrottlerPtr GetUnlimitedThrottler()
{
    return LeakyRefCountedSingleton<TUnlimitedThroughtputThrottler>();
}

IThroughputThrottlerPtr CreateNamedUnlimitedThroughputThrottler(
    const TString& name,
    NProfiling::TProfiler profiler)
{
    profiler = profiler.WithPrefix("/" + CamelCaseToUnderscoreCase(name));
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

    TFuture<void> Throttle(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        SelfQueueSize_ += amount;

        std::vector<TFuture<void>> asyncResults;
        for (const auto& throttler : Throttlers_) {
            asyncResults.push_back(throttler->Throttle(amount));
        }

        return AllSucceeded(asyncResults).Apply(BIND([weakThis = MakeWeak(this), amount] (const TError& /* error */ ) {
            if (auto this_ = weakThis.Lock()) {
                this_->SelfQueueSize_ -= amount;
            }
        }));
    }

    bool TryAcquire(i64 /*amount*/) override
    {
        YT_ABORT();
    }

    i64 TryAcquireAvailable(i64 /*amount*/) override
    {
        YT_ABORT();
    }

    void Acquire(i64 amount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(amount >= 0);

        for (const auto& throttler : Throttlers_) {
            throttler->Acquire(amount);
        }
    }

    bool IsOverdraft() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        for (const auto& throttler : Throttlers_) {
            if (throttler->IsOverdraft()) {
                return true;
            }
        }
        return false;
    }

    i64 GetQueueTotalAmount() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto totalQueueSize = SelfQueueSize_.load();
        for (const auto& throttler : Throttlers_) {
            totalQueueSize += std::max<i64>(throttler->GetQueueTotalAmount() - SelfQueueSize_.load(), 0);
        }

        return totalQueueSize;
    }

    TDuration GetEstimatedOverdraftDuration() const override
    {
        TDuration result = TDuration::Zero();
        for (const auto& throttler : Throttlers_) {
            result = std::max(result, throttler->GetEstimatedOverdraftDuration());
        }
        return result;
    }

private:
    const std::vector<IThroughputThrottlerPtr> Throttlers_;

    std::atomic<i64> SelfQueueSize_ = 0;
};

IThroughputThrottlerPtr CreateCombinedThrottler(
    const std::vector<IThroughputThrottlerPtr>& throttlers)
{
    return New<TCombinedThroughtputThrottler>(throttlers);
}

////////////////////////////////////////////////////////////////////////////////

class TStealingThrottler
    : public IThroughputThrottler
{
public:
    TStealingThrottler(
        IThroughputThrottlerPtr stealer,
        IThroughputThrottlerPtr underlying)
        : Stealer_(std::move(stealer))
        , Underlying_(std::move(underlying))
    { }

    TFuture<void> Throttle(i64 amount) override
    {
        auto future = Stealer_->Throttle(amount);
        future.Subscribe(BIND([=, this_ = MakeStrong(this)] (const TError& error) {
            if (error.IsOK()) {
                Underlying_->Acquire(amount);
            }
        }));
        return future;
    }

    bool TryAcquire(i64 amount) override
    {
        if (Stealer_->TryAcquire(amount)) {
            Underlying_->Acquire(amount);
            return true;
        }

        return false;
    }

    i64 TryAcquireAvailable(i64 amount) override
    {
        auto result = Stealer_->TryAcquireAvailable(amount);
        Underlying_->Acquire(result);

        return result;
    }

    void Acquire(i64 amount) override
    {
        Stealer_->Acquire(amount);
        Underlying_->Acquire(amount);
    }

    bool IsOverdraft() override
    {
        return Stealer_->IsOverdraft();
    }

    i64 GetQueueTotalAmount() const override
    {
        return Stealer_->GetQueueTotalAmount();
    }

    TDuration GetEstimatedOverdraftDuration() const override
    {
        return Stealer_->GetEstimatedOverdraftDuration();
    }

private:
    const IThroughputThrottlerPtr Stealer_;
    const IThroughputThrottlerPtr Underlying_;
};

IThroughputThrottlerPtr CreateStealingThrottler(
    IThroughputThrottlerPtr stealer,
    IThroughputThrottlerPtr underlying)
{
    return New<TStealingThrottler>(
        std::move(stealer),
        std::move(underlying));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
