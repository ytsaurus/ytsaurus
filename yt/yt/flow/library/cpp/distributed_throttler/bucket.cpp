#include "bucket.h"

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NFlow::NDistributedThrottler {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

bool TDistributedThrottlerBucket::TPendingRequest::operator>(const TPendingRequest& other) const
{
    if (Timestamp != other.Timestamp) {
        return Timestamp > other.Timestamp;
    }
    return EnqueueTime > other.EnqueueTime;
}

TDistributedThrottlerBucket::TDistributedThrottlerBucket(
    TThroughputThrottlerConfigPtr config,
    TDuration drainPeriod,
    IInvokerPtr invoker,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler)
    : DrainPeriod_(drainPeriod)
    , Invoker_(std::move(invoker))
    , Logger(std::move(logger))
    , TokenBucket_(CreateReconfigurableThroughputThrottler(
        std::move(config),
        Logger,
        profiler))
{ }

TDistributedThrottlerBucket::~TDistributedThrottlerBucket()
{
    Stop();
}

void TDistributedThrottlerBucket::Start()
{
    DrainLoopResult_ = BIND(&TDistributedThrottlerBucket::DrainLoop, MakeWeak(this))
        .AsyncVia(Invoker_)
        .Run();
}

TFuture<void> TDistributedThrottlerBucket::RequestQuota(
    const std::string& clientId,
    i64 amount,
    ui64 timestamp)
{
    auto promise = NewPromise<void>();
    {
        auto guard = Guard(Lock_);
        PendingRequests_.push(TPendingRequest{
            .Timestamp = timestamp,
            .EnqueueTime = TInstant::Now(),
            .ClientId = clientId,
            .Amount = amount,
            .Promise = promise,
        });
    }
    return promise.ToFuture();
}

void TDistributedThrottlerBucket::Reconfigure(TThroughputThrottlerConfigPtr config)
{
    TokenBucket_->Reconfigure(std::move(config));
}

void TDistributedThrottlerBucket::SetDrainPeriod(TDuration drainPeriod)
{
    DrainPeriod_.store(drainPeriod, std::memory_order::relaxed);
}

void TDistributedThrottlerBucket::Stop()
{
    DrainLoopResult_.Cancel(TError("Throttler bucket stopped"));
}

IReconfigurableThroughputThrottlerPtr TDistributedThrottlerBucket::GetTokenBucket() const
{
    return TokenBucket_;
}

void TDistributedThrottlerBucket::DrainLoop(TWeakPtr<TDistributedThrottlerBucket> weakThis)
{
    while (true) {
        try {
            TFuture<void> waitFuture;
            {
                auto strongThis = weakThis.Lock();
                if (!strongThis) {
                    return;
                }
                auto guard = Guard(strongThis->Lock_);
                if (!strongThis->PendingRequests_.empty()) {
                    auto top = strongThis->PendingRequests_.top();
                    strongThis->PendingRequests_.pop();
                    if (top.Promise.IsCanceled()) {
                        continue;
                    }
                    waitFuture = strongThis->TokenBucket_->Throttle(top.Amount);
                    top.Promise.TrySetFrom(waitFuture);
                } else {
                    waitFuture = TDelayedExecutor::MakeDelayed(strongThis->DrainPeriod_.load(std::memory_order::relaxed));
                }
            }
            WaitFor(waitFuture).ThrowOnError();
        } catch (const std::exception& ex) {
            if (auto strongThis = weakThis.Lock()) {
                auto& Logger = strongThis->Logger;
                YT_TLOG_ERROR("Throttler drain loop failed")
                    .With(ex);
            } else {
                return;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
