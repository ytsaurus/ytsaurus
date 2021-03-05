#include "profiler.h"
#include "profile_manager.h"
#include "timing.h"

#include <yt/yt/core/misc/optional.h>
#include <yt/yt/core/misc/farm_hash.h>

#include <yt/yt/core/ypath/token.h>

#include <util/system/sanitizers.h>

namespace NYT::NProfiling {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TTagIdList operator + (const TTagIdList& a, const TTagIdList& b)
{
    auto result = a;
    result += b;
    return result;
}

TTagIdList& operator += (TTagIdList& a, const TTagIdList& b)
{
    a.append(b.begin(), b.end());
    return a;
}

////////////////////////////////////////////////////////////////////////////////

TTimer::TTimer()
    : Start_(0)
    , LastCheckpoint_(0)
{ }

TTimer::TTimer(
    const TYPath& path,
    TCpuInstant start,
    ETimerMode mode,
    const TTagIdList& tagIds)
    : Path_(path)
    , Start_(start)
    , LastCheckpoint_(0)
    , Mode_(mode)
    , TagIds_(tagIds)
{ }

////////////////////////////////////////////////////////////////////////////////

TCounterBase::TCounterBase(
    const TYPath& path,
    const TTagIdList& tagIds,
    TDuration interval)
    : Path_(path)
    , TagIds_(tagIds)
    , Interval_(DurationToCpuDuration(interval))
{ }

TCounterBase::TCounterBase(const TCounterBase& other)
    : Path_(other.Path_)
    , TagIds_(other.TagIds_)
    , Interval_(other.Interval_)
{ }

TCounterBase& TCounterBase::operator=(const TCounterBase& other)
{
    Path_ = other.Path_;
    TagIds_ = other.TagIds_;
    Interval_ = other.Interval_;
    return *this;
}

TCpuInstant TCounterBase::GetUpdateDeadline() const
{
    return Deadline_.load(std::memory_order_relaxed);
}

void TCounterBase::Reset()
{
    Deadline_ = 0;
}

bool TCounterBase::CheckAndPromoteUpdateDeadline(TTscp tscp, bool forceEnqueue)
{
    if (Path_.empty()) {
        return false;
    }

    auto currentDeadline = Deadline_.load(std::memory_order_relaxed);
    if (!forceEnqueue && tscp.Instant < currentDeadline) {
        return false;
    }

    auto newDeadline = tscp.Instant + Interval_;
    if (!Deadline_.compare_exchange_strong(currentDeadline, newDeadline, std::memory_order_relaxed) && !forceEnqueue) {
        return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

TShardedAggregateGaugeBase::TShardedAggregateGaugeBase(
    const NYPath::TYPath& path,
    const TTagIdList& tagIds,
    EAggregateMode mode,
    TDuration interval)
    : TCounterBase(path, tagIds, interval)
    , Mode_(mode)
{ }

TShardedAggregateGaugeBase::TShardedAggregateGaugeBase(const TShardedAggregateGaugeBase& other)
    : TCounterBase(other)
    , Mode_(other.Mode_)
{ }

TShardedAggregateGaugeBase& TShardedAggregateGaugeBase::operator = (const TShardedAggregateGaugeBase& other)
{
    static_cast<TCounterBase&>(*this) = static_cast<const TCounterBase&>(other);
    Mode_ = other.Mode_;
    return *this;
}

Y_NO_SANITIZE("thread")
void TShardedAggregateGaugeBase::UpdateShards(TValue value, TTscp tscp)
{
    auto& shard = DynamicData_->Shards[tscp.ProcessorId];

    shard.Count += 1;

    if (Mode_ == EAggregateMode::All || Mode_ == EAggregateMode::Avg) {
        shard.Sum += value;
    }

    if (Mode_ == EAggregateMode::All || Mode_ == EAggregateMode::Min) {
        auto min = shard.Min.load(std::memory_order_relaxed);
        do {
            if (min <= value) {
                break;
            }
        } while (!shard.Min.compare_exchange_weak(min, value));
    }

    if (Mode_ == EAggregateMode::All || Mode_ == EAggregateMode::Max) {
        auto max = shard.Max.load(std::memory_order_relaxed);
        do {
            if (max >= value) {
                break;
            }
        } while (!shard.Max.compare_exchange_weak(max, value));
    }

    shard.Current = value;
    shard.Instant = tscp.Instant;
}

Y_NO_SANITIZE("thread")
void TShardedAggregateGaugeBase::Reset()
{
    TCounterBase::Reset();
    DynamicData_ = std::make_unique<TDynamicData>();
    for (auto& shard : DynamicData_->Shards) {
        shard.Min = std::numeric_limits<TValue>::max();
        shard.Max = std::numeric_limits<TValue>::min();
        shard.Sum = 0;
        shard.Count = 0;
        shard.Instant = 0;
        shard.Current = 0;
    }
}

Y_NO_SANITIZE("thread")
TShardedAggregateGauge::TStats TShardedAggregateGaugeBase::CollectShardStats()
{
    auto guard = Guard(SpinLock_);
    TStats result;
    result.Min = std::numeric_limits<TValue>::max();
    result.Max = std::numeric_limits<TValue>::min();
    result.Sum = 0;
    result.Count = 0;
    for (auto& shard : DynamicData_->Shards) {
        result.Min = std::min(result.Min, shard.Min.exchange(std::numeric_limits<TValue>::max()));
        result.Max = std::max(result.Max, shard.Max.exchange(std::numeric_limits<TValue>::min()));
        result.Sum += shard.Sum.exchange(0);
        result.Count += shard.Count.exchange(0);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TShardedAggregateGauge::TShardedAggregateGauge(
    const NYPath::TYPath& path,
    const TTagIdList& tagIds,
    EAggregateMode mode,
    TDuration interval)
    : TShardedAggregateGaugeBase(path, tagIds, mode, interval)
{
    Reset();
}

TShardedAggregateGauge::TShardedAggregateGauge(const TShardedAggregateGauge& other)
    : TShardedAggregateGaugeBase(other)
{
    Reset();
}

TShardedAggregateGauge& TShardedAggregateGauge::operator=(const TShardedAggregateGauge& other)
{
    static_cast<TShardedAggregateGaugeBase&>(*this) = static_cast<const TShardedAggregateGaugeBase&>(other);
    Reset();
    return *this;
}

void TShardedAggregateGauge::Update(TValue value, TTscp tscp)
{
    UpdateShards(value, tscp);
}

void TShardedAggregateGauge::Reset()
{
    TShardedAggregateGaugeBase::Reset();
}

Y_NO_SANITIZE("thread")
TValue TShardedAggregateGauge::GetCurrent() const
{
    TCpuInstant latestInstant = 0;
    TValue latestCurrent = 0;
    for (const auto& shard : DynamicData_->Shards) {
        auto instant = shard.Instant.load();
        auto current = shard.Current.load();
        if (instant > latestInstant) {
            latestInstant = instant;
            latestCurrent = current;
        }
    }
    return latestCurrent;
}

////////////////////////////////////////////////////////////////////////////////

TAtomicShardedAggregateGauge::TAtomicShardedAggregateGauge(
    const NYPath::TYPath& path,
    const TTagIdList& tagIds,
    EAggregateMode mode,
    TDuration interval)
    : TShardedAggregateGaugeBase(path, tagIds, mode, interval)
{
    Reset();
}

TAtomicShardedAggregateGauge::TAtomicShardedAggregateGauge(const TAtomicShardedAggregateGauge& other)
    : TShardedAggregateGaugeBase(other)
{
    Reset();
}

TAtomicShardedAggregateGauge& TAtomicShardedAggregateGauge::operator=(const TAtomicShardedAggregateGauge& other)
{
    static_cast<TShardedAggregateGaugeBase&>(*this) = static_cast<const TShardedAggregateGaugeBase&>(other);
    Reset();
    return *this;
}

void TAtomicShardedAggregateGauge::Update(TValue value, TTscp tscp)
{
    Current_ = value;
    UpdateShards(value, tscp);
}

TValue TAtomicShardedAggregateGauge::Increment(TValue delta, TTscp tscp)
{
    auto value = (Current_ += delta);
    UpdateShards(value, tscp);
    return value;
}

void TAtomicShardedAggregateGauge::Reset()
{
    TShardedAggregateGaugeBase::Reset();
    Current_ = 0;
}

TValue TAtomicShardedAggregateGauge::GetCurrent() const
{
    return Current_.load();
}

////////////////////////////////////////////////////////////////////////////////

TShardedMonotonicCounter::TShardedMonotonicCounter(
    const NYPath::TYPath& path,
    const TTagIdList& tagIds,
    TDuration interval)
    : TCounterBase(path, tagIds, interval)
{
    Reset();
}

TShardedMonotonicCounter::TShardedMonotonicCounter(const TShardedMonotonicCounter& other)
    : TCounterBase(other)
{
    Reset();
}

TShardedMonotonicCounter& TShardedMonotonicCounter::operator=(const TShardedMonotonicCounter& other)
{
    static_cast<TCounterBase&>(*this) = static_cast<const TCounterBase&>(other);
    Reset();
    return *this;
}

Y_NO_SANITIZE("thread")
TValue TShardedMonotonicCounter::GetCurrent() const
{
    TValue result = 0;
    for (const auto& shard : DynamicData_->Shards) {
        result += shard.Delta;
    }
    return result;
}

Y_NO_SANITIZE("thread")
void TShardedMonotonicCounter::Increment(TValue delta, TTscp tscp)
{
    DynamicData_->Shards[tscp.ProcessorId].Delta += delta;
}

Y_NO_SANITIZE("thread")
void TShardedMonotonicCounter::Reset()
{
    TCounterBase::Reset();
    DynamicData_ = std::make_unique<TDynamicData>();
    for (auto& shard : DynamicData_->Shards) {
        shard.Delta = 0;
    }
}

////////////////////////////////////////////////////////////////////////////////

TAtomicGauge::TAtomicGauge(
    const NYPath::TYPath& path,
    const TTagIdList& tagIds,
    TDuration interval)
    : TCounterBase(path, tagIds, interval)
{ }

TAtomicGauge::TAtomicGauge(const TAtomicGauge& other)
    : TCounterBase(other)
{
    Reset();
}

TAtomicGauge& TAtomicGauge::operator=(const TAtomicGauge& other)
{
    static_cast<TCounterBase&>(*this) = static_cast<const TCounterBase&>(other);
    Reset();
    return *this;
}

TValue TAtomicGauge::GetCurrent() const
{
    return Current_.load();
}

void TAtomicGauge::Update(TValue value)
{
    Current_ = value;
}

TValue TAtomicGauge::Increment(TValue delta)
{
    return Current_ += delta;
}

void TAtomicGauge::Reset()
{
    Current_ = 0;
}

////////////////////////////////////////////////////////////////////////////////

TLegacyProfiler::TLegacyProfiler(
    const TYPath& pathPrefix,
    const TTagIdList& tagIds,
    bool selfProfiling,
    bool forceEnqueue)
    : PathPrefix_(pathPrefix)
    , Enabled_(true)
    , TagIds_(tagIds)
    , ForceEnqueue_(forceEnqueue)
    , SelfProfiling_(selfProfiling)
{ }

TLegacyProfiler TLegacyProfiler::AppendPath(const TYPath& pathSuffix) const
{
    if (!Enabled_) {
        return {};
    }
    return TLegacyProfiler(PathPrefix_ + pathSuffix, TagIds_, false, ForceEnqueue_);
}

TLegacyProfiler TLegacyProfiler::AddTags(const TTagIdList& tagIds) const
{
    if (!Enabled_) {
        return {};
    }
    auto allTagIds = TagIds_;
    for (auto tagId : tagIds) {
        allTagIds.push_back(tagId);
    }
    return TLegacyProfiler(PathPrefix_, allTagIds, false, ForceEnqueue_);
}

void TLegacyProfiler::Enqueue(
    const NYPath::TYPath& path,
    TValue value,
    EMetricType metricType,
    const TTagIdList& tagIds) const
{
    if (!Enabled_) {
        return;
    }

    TQueuedSample sample;
    sample.Time = GetCpuInstant();
    sample.Path = PathPrefix_ + path;
    sample.Value = value;
    sample.TagIds = TagIds_ + tagIds;
    sample.MetricType = metricType;
    TProfileManager::Get()->Enqueue(sample, SelfProfiling_);
}

TTimer TLegacyProfiler::TimingStart(
    const TYPath& path,
    const TTagIdList& tagIds,
    ETimerMode mode) const
{
    return TTimer(path, GetCpuInstant(), mode, tagIds);
}

TDuration TLegacyProfiler::TimingStop(
    TTimer& timer,
    TStringBuf key) const
{
    return DoTimingStop(timer, key, std::nullopt);
}

TDuration TLegacyProfiler::TimingStop(
    TTimer& timer,
    const TTagIdList& totalTagIds) const
{
    return DoTimingStop(timer, std::nullopt, totalTagIds);
}

TDuration TLegacyProfiler::TimingStop(
    TTimer& timer) const
{
    return DoTimingStop(timer, std::nullopt, std::nullopt);
}

TDuration TLegacyProfiler::DoTimingStop(
    TTimer& timer,
    const std::optional<TStringBuf>& key,
    const std::optional<TTagIdList>& totalTagIds) const
{
    // Failure here means that the timer was not started or already stopped.
    YT_ASSERT(timer.Start_ != 0);

    auto now = GetCpuInstant();
    auto cpuDuration = now - timer.Start_;
    auto value = CpuDurationToValue(cpuDuration);
    YT_ASSERT(value >= 0);

    auto path = key ? timer.Path_ + "/" + ToYPathLiteral(*key) : timer.Path_;
    auto tagIds = totalTagIds ? timer.TagIds_ + *totalTagIds : timer.TagIds_;
    Enqueue(path, value, EMetricType::Gauge, tagIds);

    timer.Start_ = 0;

    return CpuDurationToDuration(cpuDuration);
}

TDuration TLegacyProfiler::TimingCheckpoint(
    TTimer& timer,
    TStringBuf key) const
{
    return DoTimingCheckpoint(timer, key, std::nullopt);
}

TDuration TLegacyProfiler::TimingCheckpoint(
    TTimer& timer,
    const TTagIdList& tagIds) const
{
    return DoTimingCheckpoint(timer, std::nullopt, tagIds);
}

TDuration TLegacyProfiler::DoTimingCheckpoint(
    TTimer& timer,
    const std::optional<TStringBuf>& key,
    const std::optional<TTagIdList>& checkpointTagIds) const
{
    // Failure here means that the timer was not started or already stopped.
    YT_ASSERT(timer.Start_ != 0);

    auto now = GetCpuInstant();

    // Upon receiving the first checkpoint Simple timer
    // is automatically switched into Sequential.
    if (timer.Mode_ == ETimerMode::Simple) {
        timer.Mode_ = ETimerMode::Sequential;
    }

    auto path = key ? timer.Path_ + "/" + ToYPathLiteral(*key) : timer.Path_;
    auto tagIds = checkpointTagIds ? timer.TagIds_ + *checkpointTagIds : timer.TagIds_;
    switch (timer.Mode_) {
        case ETimerMode::Sequential: {
            auto lastCheckpoint = timer.LastCheckpoint_ == 0 ? timer.Start_ : timer.LastCheckpoint_;
            auto duration = CpuDurationToValue(now - lastCheckpoint);
            YT_ASSERT(duration >= 0);
            Enqueue(path, duration, EMetricType::Gauge, tagIds);
            timer.LastCheckpoint_ = now;
            return CpuDurationToDuration(duration);
        }

        case ETimerMode::Parallel: {
            auto duration = CpuDurationToValue(now - timer.Start_);
            YT_ASSERT(duration >= 0);
            Enqueue(path, duration, EMetricType::Gauge, tagIds);
            return CpuDurationToDuration(duration);
        }

        default:
            YT_ABORT();
    }
}

void TLegacyProfiler::Update(TShardedAggregateGauge& gauge, TValue value, TTscp tscp) const
{
    gauge.Update(value, tscp);
    OnCounterUpdated(gauge, tscp);
}

void TLegacyProfiler::Update(TAtomicShardedAggregateGauge& gauge, TValue value, TTscp tscp) const
{
    gauge.Update(value, tscp);
    OnCounterUpdated(gauge, EMetricType::Gauge, tscp);
}

TValue TLegacyProfiler::Increment(TAtomicShardedAggregateGauge& gauge, TValue delta, TTscp tscp) const
{
    auto result = gauge.Increment(delta, tscp);
    OnCounterUpdated(gauge, EMetricType::Gauge, tscp);
    return result;
}

void TLegacyProfiler::Update(TAtomicGauge& gauge, TValue value, TTscp tscp) const
{
    gauge.Update(value);
    OnCounterUpdated(gauge, EMetricType::Gauge, tscp);
}

TValue TLegacyProfiler::Increment(TAtomicGauge& gauge, TValue delta, TTscp tscp) const
{
    auto result = gauge.Increment(delta);
    OnCounterUpdated(gauge, EMetricType::Gauge, tscp);
    return result;
}

void TLegacyProfiler::Reset(TShardedMonotonicCounter& counter, TTscp tscp) const
{
    counter.Reset();
    OnCounterUpdated(counter, EMetricType::Counter, tscp);
}

void TLegacyProfiler::Increment(TShardedMonotonicCounter& counter, TValue delta, TTscp tscp) const
{
    counter.Increment(delta, tscp);
    OnCounterUpdated(counter, EMetricType::Counter, tscp);
}

template <class T>
bool TLegacyProfiler::OnCounterUpdatedPrologue(T& counter, TTscp tscp) const
{
    if (!Enabled_) {
        return false;
    }

    if (!counter.CheckAndPromoteUpdateDeadline(tscp, ForceEnqueue_)) {
        return false;
    }

    return true;
}

void TLegacyProfiler::OnCounterUpdated(TShardedAggregateGauge& counter, TTscp tscp) const
{
    if (!OnCounterUpdatedPrologue(counter, tscp)) {
        return;
    }

    auto stats = counter.CollectShardStats();
    if (stats.Count == 0) {
        return;
    }

    auto min = stats.Min;
    auto max = stats.Max;
    auto avg = stats.Sum / stats.Count;
    switch (counter.Mode_) {
        case EAggregateMode::All:
            Enqueue(counter.Path_ + "/min", min, EMetricType::Gauge, counter.TagIds_);
            Enqueue(counter.Path_ + "/max", max, EMetricType::Gauge, counter.TagIds_);
            Enqueue(counter.Path_ + "/avg", avg, EMetricType::Gauge, counter.TagIds_);
            break;

        case EAggregateMode::Min:
            Enqueue(counter.Path_, min, EMetricType::Gauge, counter.TagIds_);
            break;

        case EAggregateMode::Max:
            Enqueue(counter.Path_, max, EMetricType::Gauge, counter.TagIds_);
            break;

        case EAggregateMode::Avg:
            Enqueue(counter.Path_, avg, EMetricType::Gauge, counter.TagIds_);
            break;

        default:
            YT_ABORT();
    }
}

template <class T>
void TLegacyProfiler::OnCounterUpdated(T& counter, EMetricType metricType, TTscp tscp) const
{
    if (!OnCounterUpdatedPrologue(counter, tscp)) {
        return;
    }

    Enqueue(
        counter.Path_,
        counter.GetCurrent(),
        metricType,
        counter.TagIds_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

////////////////////////////////////////////////////////////////////////////////

size_t THash<NYT::NProfiling::TTagIdList>::operator()(const NYT::NProfiling::TTagIdList& list) const
{
    size_t result = 1;
    for (auto tag : list) {
        result = NYT::FarmFingerprint(result, tag);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////
