#include "fair_throttler.h"

#include <yt/yt/core/profiling/timing.h>

#include "private.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TFairThrottlerConfig::TFairThrottlerConfig()
{
    RegisterParameter("total_limit", TotalLimit);

    RegisterParameter("distribution_period", DistributionPeriod)
        .Default(TDuration::MilliSeconds(100));
}

////////////////////////////////////////////////////////////////////////////////

TFairThrottlerBucketConfig::TFairThrottlerBucketConfig()
{
    RegisterParameter("weight", Weight)
        .Default(1.0)
        .GreaterThanOrEqual(0.01)
        .LessThanOrEqual(100);

    RegisterParameter("limit", Limit)
        .Default();

    RegisterParameter("relative_limit", RelativeLimit)
        .Default();
}

std::optional<i64> TFairThrottlerBucketConfig::GetLimit(i64 totalLimit)
{
    if (Limit) {
        return Limit;
    }

    if (RelativeLimit) {
        return totalLimit * *RelativeLimit;
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TBucketThrottleRequest)

struct TBucketThrottleRequest
    : public TRefCounted
{
    explicit TBucketThrottleRequest(i64 count)
        : Pending(count)
    { }

    i64 Pending;
    i64 Reserved = 0;
    TPromise<void> Promise = NewPromise<void>();
    std::atomic<bool> Cancelled = false;
    NProfiling::TCpuInstant StartTime = NProfiling::GetCpuInstant();

    void Cancel(const TError& /*error*/)
    {
        Cancelled = true;
        Promise.TrySet(TError(NYT::EErrorCode::Canceled, "Cancelled"));
    }
};

DEFINE_REFCOUNTED_TYPE(TBucketThrottleRequest)

////////////////////////////////////////////////////////////////////////////////

class TBucketThrottler
    : public IThroughputThrottler
{
public:
    TBucketThrottler(const NProfiling::TProfiler& profiler)
        : Value_(profiler.Counter("/value"))
        , WaitTime_(profiler.Timer("/wait_time"))
    {
        profiler.AddFuncGauge("/queue_size", MakeStrong(this), [this] {
            return GetQueueTotalCount();
        });

        profiler.AddFuncGauge("/throttled", MakeStrong(this), [this] {
            return IsOverdraft();
        });
    }

    TFuture<void> Throttle(i64 count) override
    {
        if (TryAcquire(count)) {
            return VoidFuture;
        }

        auto request = New<TBucketThrottleRequest>(count);
        QueueSize_ += count;

        request->Promise.OnCanceled(BIND(&TBucketThrottleRequest::Cancel, MakeWeak(request)));
        request->Promise.ToFuture().Subscribe(BIND(&TBucketThrottler::OnRequestComplete, MakeWeak(this), count));

        auto guard = Guard(Lock_);
        Queue_.push_back(request);
        return request->Promise.ToFuture();
    }

    bool TryAcquire(i64 count) override
    {
        YT_VERIFY(count >= 0);

        auto available = Quota_.load();
        while (true) {
            if (count > available) {
                return false;
            }

            if (Quota_.compare_exchange_weak(available, available - count)) {
                Value_.Increment(count);
                return true;
            }
        }
    }

    i64 TryAcquireAvailable(i64 count) override
    {
        YT_VERIFY(count >= 0);

        auto available = Quota_.load();
        while (true) {
            if (available < 0) {
                return 0;
            }

            if (available < count) {
                if (Quota_.compare_exchange_weak(available, 0)) {
                    Value_.Increment(available);
                    return available;
                }
            } else {
                if (Quota_.compare_exchange_weak(available, available - count)) {
                    Value_.Increment(count);
                    return count;
                }
            }
        }
    }

    void Acquire(i64 count) override
    {
        YT_VERIFY(count >= 0);

        Quota_ -= count;
        Value_.Increment(count);
    }

    bool IsOverdraft() override
    {
        return GetQueueTotalCount() > 0;
    }

    i64 GetQueueTotalCount() const override
    {
        return Max(-Quota_.load(), 0l) + QueueSize_.load();
    }

    struct TBucketState
    {
        i64 Limit; // Limit on current interation.
        i64 Remaining; // Remaining quota on current iteration.
        i64 Overdraft; // Unpaid overdraft from previous iterations.
        i64 QueueSize; // Total size of all queued requests.
    };

    TBucketState Drain()
    {
        auto quota = Quota_.exchange(0);
        if (quota < 0) {
            Quota_ += quota;
        }

        return TBucketState{
            .Limit = LastLimit_,
            .Remaining = Max(quota, 0l),
            .Overdraft = Max(-quota, 0l),
            .QueueSize = QueueSize_.load(),
        };
    }

    void Refill(i64 redistributedQuota, i64 nextOptimisticLimit)
    {
        LastLimit_ = nextOptimisticLimit;

        auto available = Max(redistributedQuota + nextOptimisticLimit, 0l);
        if (available == 0) {
            Quota_ += redistributedQuota + nextOptimisticLimit;
            return;
        }

        std::vector<TBucketThrottleRequestPtr> readyList;

        auto guard = Guard(Lock_);
        auto satisfyRequests = [&] (i64 quota) {
            while (!Queue_.empty()) {
                auto request = Queue_.front();
                if (request->Cancelled.load()) {
                    quota += request->Reserved;
                    Queue_.pop_front();
                    continue;
                }

                if (request->Pending <= quota) {
                    quota -= request->Pending;
                    Queue_.pop_front();
                    readyList.push_back(std::move(request));
                } else {
                    request->Pending -= quota;
                    request->Reserved += quota;
                    return 0l;
                }
            }

            return quota;
        };

        if (redistributedQuota > 0) {
            satisfyRequests(redistributedQuota);
        }

        Quota_ += satisfyRequests(nextOptimisticLimit);
        guard.Release();

        auto now = NProfiling::GetCpuInstant();
        for (const auto& request : readyList) {
            auto waitTime = NProfiling::CpuDurationToDuration(now - request->StartTime);
            WaitTime_.Record(waitTime);
            Value_.Increment(request->Pending + request->Reserved);
            request->Promise.TrySet();
        }
    }

private:
    NProfiling::TCounter Value_;
    NProfiling::TEventTimer WaitTime_;

    i64 LastLimit_ = 0;
    std::atomic<i64> Quota_ = {};
    std::atomic<i64> QueueSize_ = {};

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::deque<TBucketThrottleRequestPtr> Queue_;

    void OnRequestComplete(i64 count, const TError& /*error*/)
    {
        QueueSize_ -= count;
    }
};

DEFINE_REFCOUNTED_TYPE(TBucketThrottler)

////////////////////////////////////////////////////////////////////////////////

TFairThrottler::TFairThrottler(
    TFairThrottlerConfigPtr config,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler)
    : Logger(std::move(logger))
    , Profiler_(std::move(profiler))
    , Config_(std::move(config))
{
    ScheduleLimitUpdate(TInstant::Now());
}

IThroughputThrottlerPtr TFairThrottler::CreateBucketThrottler(
    const TString& name,
    TFairThrottlerBucketConfigPtr config)
{
    if (!config) {
        config = New<TFairThrottlerBucketConfig>();
    }

    auto guard = Guard(Lock_);

    if (auto it = Buckets_.find(name); it != Buckets_.end()) {
        it->second.Config = std::move(config);
        return it->second.Throttler;
    }

    auto throttler = New<TBucketThrottler>(Profiler_.WithTag("bucket", name));
    Buckets_[name] = TBucket{
        .Config = std::move(config),
        .Throttler = throttler,
    };

    return throttler;
}

void TFairThrottler::Reconfigure(
    TFairThrottlerConfigPtr config,
    const THashMap<TString, TFairThrottlerBucketConfigPtr>& buckets)
{
    for (const auto& [name, config] : buckets) {
        CreateBucketThrottler(name, config);
    }

    auto guard = Guard(Lock_);
    Config_ = std::move(config);
}

void TFairThrottler::DoUpdateLimits()
{
    auto guard = Guard(Lock_);
    if (Buckets_.empty()) {
        return;
    }

    std::vector<double> weights;
    weights.reserve(Buckets_.size());

    std::vector<std::optional<i64>> limits;
    limits.reserve(Buckets_.size());

    std::vector<i64> demands;
    demands.reserve(Buckets_.size());

    std::vector<TBucketThrottler::TBucketState> states;
    states.reserve(Buckets_.size());

    THashMap<TString, i64> bucketDemands;

    for (const auto& [name, bucket] : Buckets_) {
        auto state = bucket.Throttler->Drain();

        weights.push_back(bucket.Config->Weight);

        if (auto limit = bucket.Config->GetLimit(Config_->TotalLimit)) {
            limits.push_back(*limit * Config_->DistributionPeriod.SecondsFloat());
        } else {
            limits.emplace_back();
        }

        demands.push_back(state.Limit - state.Remaining + state.QueueSize + state.Overdraft);
        bucketDemands[name] = demands.back();
        states.push_back(state);
    }

    auto tickLimit = Config_->TotalLimit * Config_->DistributionPeriod.SecondsFloat();
    auto fairLimits = ComputeFairDistribution(tickLimit, weights, demands, limits);

    auto freeQuota = tickLimit - std::accumulate(fairLimits.begin(), fairLimits.end(), 0l);

    std::vector<std::optional<i64>> remainingLimits;
    remainingLimits.reserve(Buckets_.size());

    std::vector<i64> fakeDemands;
    fakeDemands.reserve(Buckets_.size());

    for (int i = 0; i < std::ssize(weights); ++i) {
        fakeDemands.push_back(freeQuota);

        if (limits[i]) {
            remainingLimits.push_back(*limits[i] - fairLimits[i]);
        } else {
            remainingLimits.emplace_back();
        }
    }

    auto freeQuotaLimits = ComputeFairDistribution(freeQuota, weights, fakeDemands, remainingLimits);

    i64 unfairUsage = 0;
    i64 blockedUsage = 0;

    THashMap<TString, i64> bucketUsage;
    THashMap<TString, i64> bucketLimits;

    int i = 0;
    for (const auto& [name, bucket] : Buckets_) {
        auto state = states[i];
        auto usage = state.Limit - state.Remaining;

        unfairUsage += Max(usage - fairLimits[i], 0l);

        auto redistributedQuota = fairLimits[i] - usage;
        blockedUsage += redistributedQuota;

        bucketUsage[name] = usage;

        auto newLimit = fairLimits[i] + freeQuotaLimits[i];
        bucketLimits[name] = newLimit;

        bucket.Throttler->Refill(redistributedQuota, newLimit);

        ++i;
    }

    UnfairBytes_.Increment(unfairUsage);
    BlockedBytes_.Increment(blockedUsage);

    YT_LOG_DEBUG("Fair throttler tick (TickLimit: %v, FreeQuota: %v, BlockedUsage: %v, UnfairUsage: %v, BucketUsage: %v, BucketLimits: %v, BucketDemands: %v)",
        tickLimit, // How many bytes was distributed?
        freeQuota, // How many bytes was left unconsumed?
        blockedUsage, // How many requests blocked for DistributionPeriod?
        unfairUsage, // How many bytes was consumed above fairLimits?
        bucketUsage, // Real bucket usage.
        bucketLimits, // Bucket limits for the next iteration.
        bucketDemands); // Bucket demands during this iteration.
}

void TFairThrottler::UpdateLimits(TInstant at)
{
    DoUpdateLimits();

    auto guard = Guard(Lock_);
    ScheduleLimitUpdate(at + Config_->DistributionPeriod);
}

void TFairThrottler::ScheduleLimitUpdate(TInstant at)
{
    TDelayedExecutor::Submit(
        BIND(&TFairThrottler::UpdateLimits, MakeWeak(this), at),
        at);
}

std::vector<i64> TFairThrottler::ComputeFairDistribution(
    i64 totalLimit,
    const std::vector<double>& weights,
    const std::vector<i64>& demands,
    const std::vector<std::optional<i64>>& limits)
{
    YT_VERIFY(!weights.empty());
    YT_VERIFY(weights.size() == demands.size() && weights.size() == limits.size());

    const auto& Logger = ConcurrencyLogger;

    std::vector<std::pair<double, int>> queue;
    for (int i = 0; i < std::ssize(weights); ++i) {
        queue.emplace_back(Min(demands[i], limits[i].value_or(Max<i64>())) / weights[i], i);
    }
    std::sort(queue.begin(), queue.end());

    std::vector<i64> totalLimits;
    totalLimits.resize(weights.size());

    double remainingWeight = std::accumulate(weights.begin(), weights.end(), 0.0);
    i64 remainingCapacity = totalLimit;

    int i = 0;
    for ( ; i < std::ssize(weights); ++i) {
        auto [targetShare, targetIndex] = queue[i];

        YT_LOG_TRACE("Examining bucket (Index: %v, TargetShare: %v, RemainingWeight: %v, RemainingCapacity: %v)",
            targetIndex,
            targetShare,
            remainingWeight,
            remainingCapacity);

        if (targetShare * remainingWeight >= static_cast<double>(remainingCapacity)) {
            break;
        }

        totalLimits[targetIndex] = Min(demands[targetIndex], limits[targetIndex].value_or(Max<i64>()));
        remainingCapacity -= totalLimits[targetIndex];
        remainingWeight -= weights[targetIndex];

        YT_LOG_TRACE("Satisfied demand (Index: %v, Capacity: %v)",
            targetIndex,
            totalLimits[targetIndex]);
    }

    auto finalShare = Max<double>(remainingCapacity, 0l) / Max(remainingWeight, 0.01);
    for (int j = i; j < std::ssize(weights); j++) {
        auto bucketIndex = queue[j].second;
        totalLimits[bucketIndex] = weights[bucketIndex] * finalShare;

        YT_LOG_TRACE("Distributed remains (Index: %v, Capacity: %v)",
            bucketIndex,
            totalLimits[bucketIndex]);
    }

    return totalLimits;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
