#include "fair_throttler.h"

#include <yt/yt/core/profiling/timing.h>

#include "private.h"

#include <util/random/shuffle.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TFairThrottlerConfig::TFairThrottlerConfig()
{
    RegisterParameter("total_limit", TotalLimit);

    RegisterParameter("distribution_period", DistributionPeriod)
        .Default(TDuration::MilliSeconds(100))
        .GreaterThan(TDuration::Zero());

    RegisterParameter("bucket_accumulation_ticks", BucketAccumulationTicks)
        .Default(5);

    RegisterParameter("global_accumulation_ticks", GlobalAccumulationTicks)
        .Default(5);
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

    RegisterParameter("guarantee", Guarantee)
        .Default();

    RegisterParameter("relative_guaratee", RelativeGuarantee)
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

std::optional<i64> TFairThrottlerBucketConfig::GetGuarantee(i64 totalLimit)
{
    if (Guarantee) {
        return Guarantee;
    }

    if (RelativeGuarantee) {
        return totalLimit * *RelativeGuarantee;
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

struct TLeakyCounter
{
    TLeakyCounter(int windowSize)
        : Window(windowSize)
    { }

    std::atomic<i64> Value = 0;    

    std::vector<i64> Window;
    int WindowPosition = 0;

    i64 Increment(i64 delta)
    {
        return Increment(delta, delta);
    }

    i64 Increment(i64 delta, i64 limit)
    {
        auto maxValue = std::accumulate(Window.begin(), Window.end(), i64(0)) - Window[WindowPosition];

        auto currentValue = Value.load();
        do {
            if (currentValue <= maxValue) {
                break;
            }
        } while (!Value.compare_exchange_strong(currentValue, maxValue));

        Window[WindowPosition] = limit;

        WindowPosition++;
        if (WindowPosition >= std::ssize(Window)) {
            WindowPosition = 0;
        }

        Value += delta;
        return std::max(currentValue - maxValue, 0L);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSharedBucket final
{
    TSharedBucket(int windowSize)
        : Limit(windowSize)
    { }

    TLeakyCounter Limit;
};

DEFINE_REFCOUNTED_TYPE(TSharedBucket)

////////////////////////////////////////////////////////////////////////////////

class TBucketThrottler
    : public IThroughputThrottler
{
public:
    TBucketThrottler(
        const NLogging::TLogger& logger,
        const NProfiling::TProfiler& profiler,
        const TSharedBucketPtr& sharedBucket,
        TFairThrottlerConfigPtr config)
        : Logger(logger)
        , SharedBucket_(sharedBucket)
        , Value_(profiler.Counter("/value"))
        , WaitTime_(profiler.Timer("/wait_time"))
        , Quota_(config->BucketAccumulationTicks)
        , DistributionPeriod_(config->DistributionPeriod)
    {
        profiler.AddFuncGauge("/queue_size", MakeStrong(this), [this] {
            return GetQueueTotalCount();
        });

        profiler.AddFuncGauge("/quota", MakeStrong(this), [this] {
            return Quota_.Value.load();
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

        YT_LOG_DEBUG("Started waiting for throttler (Count: %v)", count);

        auto guard = Guard(Lock_);
        Queue_.push_back(request);
        return request->Promise.ToFuture();
    }

    bool TryAcquire(i64 count) override
    {
        YT_VERIFY(count >= 0);

        auto available = Quota_.Value.load();
        auto globalAvailable = IsLimited() ? 0 : SharedBucket_->Limit.Value.load();

        if (count > available + globalAvailable) {
            return false;
        }

        auto globalConsumed = std::min(count - available, globalAvailable);
        Quota_.Value -= count - globalConsumed;
        SharedBucket_->Limit.Value -= globalConsumed;

        Value_.Increment(count);
        Usage_ += count;

        return true;
    }

    i64 TryAcquireAvailable(i64 count) override
    {
        YT_VERIFY(count >= 0);

        auto available = Quota_.Value.load();
        auto globalAvailable = IsLimited() ? 0 : SharedBucket_->Limit.Value.load();

        auto consumed = std::min(count, available + globalAvailable);

        auto globalConsumed = std::min(consumed - available, globalAvailable);
        Quota_.Value -= count - globalConsumed;
        SharedBucket_->Limit.Value -= globalConsumed;

        Value_.Increment(consumed);
        Usage_ += consumed;

        return consumed;
    }

    void Acquire(i64 count) override
    {
        YT_VERIFY(count >= 0);

        auto available = Quota_.Value.load();
        auto globalAvailable = IsLimited() ? 0 : SharedBucket_->Limit.Value.load();

        auto globalConsumed = std::min(count - available, globalAvailable);
        Quota_.Value -= count - globalConsumed;
        SharedBucket_->Limit.Value -= globalConsumed;

        Value_.Increment(count);
        Usage_ += count;
    }

    bool IsOverdraft() override
    {
        return GetQueueTotalCount() > 0;
    }

    i64 GetQueueTotalCount() const override
    {
        return Max(-Quota_.Value.load(), 0l) + QueueSize_.load();
    }

    TDuration GetEstimatedOverdraftDuration() const override
    {
        auto queueTotalCount = GetQueueTotalCount();
        if (queueTotalCount == 0) {
            return TDuration::Zero();
        }

        auto limit = LastLimit_.load();
        auto distributionPeriod = DistributionPeriod_.load();
        if (limit == 0) {
            return distributionPeriod;
        }

        return queueTotalCount / limit * distributionPeriod;
    }

    struct TBucketState
    {
        i64 Usage; // Quota usage on current iteration.
        i64 Quota;
        i64 Overdraft; // Unpaid overdraft from previous iterations.
        i64 QueueSize; // Total size of all queued requests.
    };

    TBucketState Peek()
    {
        auto quota = Quota_.Value.load();

        return TBucketState{
            .Usage = Usage_.exchange(0),
            .Quota = quota,
            .Overdraft = Max(-quota, 0l),
            .QueueSize = QueueSize_.load(),
        };
    }

    i64 SatisfyRequests(i64 quota)
    {
        std::vector<TBucketThrottleRequestPtr> readyList;

        {
            auto guard = Guard(Lock_);
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
                    quota = 0;
                    break;
                }
            }
        }

        auto now = NProfiling::GetCpuInstant();
        for (const auto& request : readyList) {
            auto waitTime = NProfiling::CpuDurationToDuration(now - request->StartTime);

            WaitTime_.Record(waitTime);
            Value_.Increment(request->Pending + request->Reserved);
            Usage_ += request->Pending + request->Reserved;

            request->Promise.TrySet();
        }

        return quota;
    }

    i64 Refill(i64 quota)
    {
        LastLimit_ = quota;

        if (Quota_.Value.load() < 0) {
            auto remainingQuota = Quota_.Increment(quota);
            return SatisfyRequests(remainingQuota);
        } else {
            auto remainingQuota = SatisfyRequests(quota);
            return Quota_.Increment(remainingQuota, quota);
        }
    }

    void SetDistributionPeriod(TDuration distributionPeriod)
    {
        DistributionPeriod_.store(distributionPeriod);
    }

    void SetLimited(bool limited)
    {
        Limited_ = limited;
    }

    bool IsLimited()
    {
        return Limited_.load();
    }

private:
    NLogging::TLogger Logger;

    TSharedBucketPtr SharedBucket_;

    NProfiling::TCounter Value_;
    NProfiling::TEventTimer WaitTime_;

    TLeakyCounter Quota_;
    std::atomic<i64> LastLimit_ = 0;
    std::atomic<i64> QueueSize_ = 0;
    std::atomic<i64> Usage_ = 0;

    std::atomic<bool> Limited_ = {false};

    std::atomic<TDuration> DistributionPeriod_;

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
    , SharedBucket_(New<TSharedBucket>(config->GlobalAccumulationTicks))
    , Config_(std::move(config))
{
    ScheduleLimitUpdate(TInstant::Now());

    Profiler_.AddFuncGauge("/shared_quota", MakeStrong(this), [this] {
        return SharedBucket_->Limit.Value.load();
    });
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
        it->second.Throttler->SetLimited(config->Limit || config->RelativeLimit);

        it->second.Config = std::move(config);
        it->second.Throttler->SetDistributionPeriod(Config_->DistributionPeriod);
        return it->second.Throttler;
    }

    auto throttler = New<TBucketThrottler>(
        Logger.WithTag("Bucket: %v", name),
        Profiler_.WithTag("bucket", name),
        SharedBucket_,
        Config_);

    throttler->SetLimited(config->Limit || config->RelativeLimit);

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
        auto state = bucket.Throttler->Peek();

        weights.push_back(bucket.Config->Weight);

        if (auto limit = bucket.Config->GetLimit(Config_->TotalLimit)) {
            limits.push_back(*limit * Config_->DistributionPeriod.SecondsFloat());
        } else {
            limits.emplace_back();
        }

        auto guarantee = bucket.Config->GetGuarantee(Config_->TotalLimit);
        auto demand = state.Usage + state.Overdraft + state.QueueSize;
        if (guarantee && *guarantee > demand) {
            demand = *guarantee;
        }

        demands.push_back(demand);
        
        bucketDemands[name] = demands.back();
        states.push_back(state);
    }

    auto tickLimit = Config_->TotalLimit * Config_->DistributionPeriod.SecondsFloat();
    auto tickIncome = ComputeFairDistribution(tickLimit, weights, demands, limits);

    // Distribute remaining quota according to weights and limits.
    auto freeQuota = tickLimit - std::accumulate(tickIncome.begin(), tickIncome.end(), i64(0));
    for (int i = 0; i < std::ssize(tickIncome); i++) {
        demands[i] = freeQuota;

        if (limits[i]) {
            (*limits[i]) -= tickIncome[i];
        }
    }
    auto freeIncome = ComputeFairDistribution(freeQuota, weights, demands, limits);

    THashMap<TString, i64> bucketUsage;
    THashMap<TString, i64> bucketIncome;
    THashMap<TString, i64> bucketQuota;

    i64 leakedQuota = 0;
    int i = 0;
    for (const auto& [name, bucket] : Buckets_) {
        auto state = states[i];
        auto newLimit = tickIncome[i] + freeIncome[i];

        bucketUsage[name] = state.Usage;
        bucketQuota[name] = state.Quota;
        bucketIncome[name] = newLimit;

        leakedQuota += bucket.Throttler->Refill(newLimit);

        ++i;
    }

    i64 droppedQuota = SharedBucket_->Limit.Increment(leakedQuota);

    std::vector<TBucketThrottlerPtr> throttlers;
    for (const auto& [name, bucket] : Buckets_) {
        throttlers.push_back(bucket.Throttler);
    }
    Shuffle(throttlers.begin(), throttlers.end());

    for (const auto& throttler : throttlers) {
        auto limit = SharedBucket_->Limit.Value.load();
        if (limit <= 0) {
            break;
        }

        if (throttler->IsLimited()) {
            continue;
        }

        SharedBucket_->Limit.Value -= limit - throttler->SatisfyRequests(limit);
    }

    YT_LOG_DEBUG("Fair throttler tick (TickLimit: %v, FreeQuota: %v, SharedBucket: %v, DroppedQuota: %v)",
        tickLimit, // How many bytes was distributed?
        freeQuota, // How many bytes was left unconsumed?
        SharedBucket_->Limit.Value.load(),
        droppedQuota);

    YT_LOG_DEBUG("Fair throttler tick details (BucketIncome: %v, BucketUsage: %v, BucketDemands: %v, BucketQuota: %v)",
        bucketIncome,
        bucketUsage,
        bucketDemands,
        bucketQuota);
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
