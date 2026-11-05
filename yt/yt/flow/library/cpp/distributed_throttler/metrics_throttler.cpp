#include "metrics_throttler.h"

namespace NYT::NFlow::NDistributedThrottler {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TMetricsTrackingThrottler
    : public IThroughputThrottler
{
public:
    TMetricsTrackingThrottler(
        IThroughputThrottlerPtr underlying,
        NProfiling::TProfiler profiler)
        : Underlying_(std::move(underlying))
        , ConsumedCounter_(profiler.Counter("/consumed"))
        , ReleasedCounter_(profiler.Counter("/released"))
        , WaitTimer_(profiler.Timer("/wait_time"))
    { }

    TFuture<void> Throttle(i64 amount) override
    {
        auto start = TInstant::Now();
        auto future = Underlying_->Throttle(amount);
        future.Subscribe(BIND(
            [amount, start, consumed = ConsumedCounter_, waitTimer = WaitTimer_] (const TError& error) mutable {
                waitTimer.Record(TInstant::Now() - start);
                if (error.IsOK()) {
                    consumed.Increment(amount);
                }
            }));
        return future;
    }

    bool TryAcquire(i64 amount) override
    {
        if (Underlying_->TryAcquire(amount)) {
            ConsumedCounter_.Increment(amount);
            return true;
        }
        return false;
    }

    i64 TryAcquireAvailable(i64 amount) override
    {
        auto acquired = Underlying_->TryAcquireAvailable(amount);
        if (acquired > 0) {
            ConsumedCounter_.Increment(acquired);
        }
        return acquired;
    }

    void Acquire(i64 amount) override
    {
        Underlying_->Acquire(amount);
        ConsumedCounter_.Increment(amount);
    }

    void Release(i64 amount) override
    {
        Underlying_->Release(amount);
        ReleasedCounter_.Increment(amount);
    }

    bool IsOverdraft() override
    {
        return Underlying_->IsOverdraft();
    }

    i64 GetQueueTotalAmount() const override
    {
        return Underlying_->GetQueueTotalAmount();
    }

    TDuration GetEstimatedOverdraftDuration() const override
    {
        return Underlying_->GetEstimatedOverdraftDuration();
    }

    i64 GetAvailable() const override
    {
        return Underlying_->GetAvailable();
    }

private:
    const IThroughputThrottlerPtr Underlying_;
    NProfiling::TCounter ConsumedCounter_;
    NProfiling::TCounter ReleasedCounter_;
    NProfiling::TEventTimer WaitTimer_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IThroughputThrottlerPtr CreateMetricsTrackingThrottler(
    IThroughputThrottlerPtr underlying,
    NProfiling::TProfiler profiler)
{
    return New<TMetricsTrackingThrottler>(std::move(underlying), std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
