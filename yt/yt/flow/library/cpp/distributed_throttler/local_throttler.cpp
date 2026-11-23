#include "local_throttler.h"

#include "metrics_throttler.h"

namespace NYT::NFlow::NDistributedThrottler {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TLocalThrottler
    : public IThroughputThrottler
{
public:
    TLocalThrottler(
        IThroughputThrottlerPtr underlying,
        TFuture<void> failureFuture,
        NProfiling::TProfiler profiler)
        : Underlying_(std::move(underlying))
        , FailureFuture_(std::move(failureFuture))
        , ConsumedCounter_(profiler.Counter("/consumed"))
        , ReleasedCounter_(profiler.Counter("/released"))
        , WaitTimer_(profiler.Timer("/wait_time"))
    { }

    TFuture<void> Throttle(i64 amount) override
    {
        if (FailureFuture_ && FailureFuture_.IsSet()) {
            return FailureFuture_;
        }

        auto start = TInstant::Now();
        auto future = Underlying_->Throttle(amount);
        auto record = [amount, start, consumed = ConsumedCounter_, waitTimer = WaitTimer_] (const TError& error) mutable {
            waitTimer.Record(TInstant::Now() - start);
            if (error.IsOK()) {
                consumed.Increment(amount);
            }
        };
        if (future.IsSet()) {
            if (FailureFuture_ && FailureFuture_.IsSet()) {
                future = FailureFuture_;
            }
            record(future.GetOrCrash());
            return future;
        }

        return future.Apply(BIND_NO_PROPAGATE(
            [failureFuture = FailureFuture_, record = std::move(record)] (const TError& result) mutable {
                const auto& error = failureFuture && failureFuture.IsSet() ? failureFuture.GetOrCrash() : result;
                record(error);
                error.ThrowOnError();
            }));
    }

    bool TryAcquire(i64 amount) override
    {
        if (GetUnderlyingOrThrow()->TryAcquire(amount)) {
            ConsumedCounter_.Increment(amount);
            return true;
        }
        return false;
    }

    i64 TryAcquireAvailable(i64 amount) override
    {
        auto acquired = GetUnderlyingOrThrow()->TryAcquireAvailable(amount);
        if (acquired > 0) {
            ConsumedCounter_.Increment(acquired);
        }
        return acquired;
    }

    void Acquire(i64 amount) override
    {
        GetUnderlyingOrThrow()->Acquire(amount);
        ConsumedCounter_.Increment(amount);
    }

    void Release(i64 amount) override
    {
        GetUnderlyingOrThrow()->Release(amount);
        ReleasedCounter_.Increment(amount);
    }

    bool IsOverdraft() override
    {
        return GetUnderlyingOrThrow()->IsOverdraft();
    }

    i64 GetQueueTotalAmount() const override
    {
        return GetUnderlyingOrThrow()->GetQueueTotalAmount();
    }

    TDuration GetEstimatedOverdraftDuration() const override
    {
        return GetUnderlyingOrThrow()->GetEstimatedOverdraftDuration();
    }

    i64 GetAvailable() const override
    {
        return GetUnderlyingOrThrow()->GetAvailable();
    }

private:
    const IThroughputThrottlerPtr Underlying_;
    const TFuture<void> FailureFuture_;
    NProfiling::TCounter ConsumedCounter_;
    NProfiling::TCounter ReleasedCounter_;
    NProfiling::TEventTimer WaitTimer_;

    IThroughputThrottler* GetUnderlyingOrThrow() const
    {
        if (FailureFuture_ && FailureFuture_.IsSet()) {
            FailureFuture_.GetOrCrash()
                .ThrowOnError();
        }
        return Underlying_.Get();
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IThroughputThrottlerPtr CreateLocalThrottler(
    IThroughputThrottlerPtr underlying,
    TFuture<void> failureFuture,
    NProfiling::TProfiler profiler)
{
    return New<TLocalThrottler>(std::move(underlying), std::move(failureFuture), std::move(profiler));
}

IThroughputThrottlerPtr CreateMetricsTrackingThrottler(
    IThroughputThrottlerPtr underlying,
    NProfiling::TProfiler profiler)
{
    return CreateLocalThrottler(std::move(underlying), {}, std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
