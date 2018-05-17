#include "profiler.h"
#include "profile_manager.h"
#include "timing.h"

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/farm_hash.h>

#include <yt/core/ypath/token.h>

namespace NYT {
namespace NProfiling  {

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
    , Deadline_(0)
    , Current_(0)
{ }

TCounterBase::TCounterBase(const TCounterBase& other)
{
    *this = other;
}

TCounterBase& TCounterBase::operator=(const TCounterBase& other)
{
    Path_ = other.Path_;
    TagIds_ = other.TagIds_;
    Interval_ = other.Interval_;
    Deadline_ = 0;
    Current_ = other.Current_.load(std::memory_order_relaxed);
    return *this;
}

TValue TCounterBase::GetCurrent() const
{
    return Current_.load(std::memory_order_relaxed);
}

TCpuInstant TCounterBase::GetUpdateDeadline() const
{
    return Deadline_.load(std::memory_order_relaxed);
}

////////////////////////////////////////////////////////////////////////////////

TAggregateCounter::TAggregateCounter(
    const NYPath::TYPath& path,
    const TTagIdList& tagIds,
    EAggregateMode mode,
    TDuration interval)
    : TCounterBase(path, tagIds, interval)
    , Mode_(mode)
{
    Reset();
}

TAggregateCounter::TAggregateCounter(const TAggregateCounter& other)
    : TCounterBase(other)
{
    *this = other;
    Reset();
}

TAggregateCounter& TAggregateCounter::operator=(const TAggregateCounter& other)
{
    static_cast<TCounterBase&>(*this) = static_cast<const TCounterBase&>(other);
    Mode_ = other.Mode_;
    Reset();
    return *this;
}

void TAggregateCounter::Reset()
{
    Min_ = std::numeric_limits<TValue>::max();
    Max_ = std::numeric_limits<TValue>::min();
    Sum_ = 0;
    SampleCount_ = 0;
}

////////////////////////////////////////////////////////////////////////////////

TMonotonicCounter::TMonotonicCounter(
    const NYPath::TYPath& path,
    const TTagIdList& tagIds,
    TDuration interval)
    : TCounterBase(path, tagIds, interval)
{ }

TMonotonicCounter::TMonotonicCounter(const TMonotonicCounter& other)
    : TCounterBase(other)
{
    *this = other;
}

TMonotonicCounter& TMonotonicCounter::operator=(const TMonotonicCounter& other)
{
    static_cast<TCounterBase&>(*this) = static_cast<const TCounterBase&>(other);
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

TSimpleGauge::TSimpleGauge(
    const NYPath::TYPath& path,
    const TTagIdList& tagIds,
    TDuration interval)
    : TCounterBase(path, tagIds, interval)
{ }

TSimpleGauge::TSimpleGauge(const TSimpleGauge& other)
    : TCounterBase(other)
{
    *this = other;
}

TSimpleGauge& TSimpleGauge::operator=(const TSimpleGauge& other)
{
    static_cast<TCounterBase&>(*this) = static_cast<const TCounterBase&>(other);
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

TProfiler::TProfiler()
    : Enabled_(false)
{ }

TProfiler::TProfiler(
    const TYPath& pathPrefix,
    const TTagIdList& tagIds,
    bool selfProfiling)
    : PathPrefix_(pathPrefix)
    , Enabled_(true)
    , TagIds_(tagIds)
    , SelfProfiling_(selfProfiling)
{ }

void TProfiler::Enqueue(
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

TTimer TProfiler::TimingStart(
    const TYPath& path,
    const TTagIdList& tagIds,
    ETimerMode mode) const
{
    return TTimer(path, GetCpuInstant(), mode, tagIds);
}

TDuration TProfiler::TimingStop(
    TTimer& timer,
    const TStringBuf& key) const
{
    return DoTimingStop(timer, key, Null);
}

TDuration TProfiler::TimingStop(
    TTimer& timer,
    const TTagIdList& totalTagIds) const
{
    return DoTimingStop(timer, Null, totalTagIds);
}

TDuration TProfiler::TimingStop(
    TTimer& timer) const
{
    return DoTimingStop(timer, Null, Null);
}

TDuration TProfiler::DoTimingStop(
    TTimer& timer,
    const TNullable<TStringBuf>& key,
    const TNullable<TTagIdList>& totalTagIds) const
{
    // Failure here means that the timer was not started or already stopped.
    Y_ASSERT(timer.Start_ != 0);

    auto now = GetCpuInstant();
    auto cpuDuration = now - timer.Start_;
    auto value = CpuDurationToValue(cpuDuration);
    Y_ASSERT(value >= 0);

    auto path = key ? timer.Path_ + "/" + ToYPathLiteral(*key) : timer.Path_;
    auto tagIds = totalTagIds ? timer.TagIds_ + *totalTagIds : timer.TagIds_;
    Enqueue(path, value, EMetricType::Gauge, tagIds);

    timer.Start_ = 0;

    return CpuDurationToDuration(cpuDuration);
}

TDuration TProfiler::TimingCheckpoint(
    TTimer& timer,
    const TStringBuf& key) const
{
    return DoTimingCheckpoint(timer, key, Null);
}

TDuration TProfiler::TimingCheckpoint(
    TTimer& timer,
    const TTagIdList& tagIds) const
{
    return DoTimingCheckpoint(timer, Null, tagIds);
}

TDuration TProfiler::DoTimingCheckpoint(
    TTimer& timer,
    const TNullable<TStringBuf>& key,
    const TNullable<TTagIdList>& checkpointTagIds) const
{
    // Failure here means that the timer was not started or already stopped.
    Y_ASSERT(timer.Start_ != 0);

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
            Y_ASSERT(duration >= 0);
            Enqueue(path, duration, EMetricType::Gauge, tagIds);
            timer.LastCheckpoint_ = now;
            return CpuDurationToDuration(duration);
        }

        case ETimerMode::Parallel: {
            auto duration = CpuDurationToValue(now - timer.Start_);
            Y_ASSERT(duration >= 0);
            Enqueue(path, duration, EMetricType::Gauge, tagIds);
            return CpuDurationToDuration(duration);
        }

        default:
            Y_UNREACHABLE();
    }
}

void TProfiler::Update(TAggregateCounter& counter, TValue value) const
{
    counter.Current_ = value;
    OnUpdated(counter, value);
}

TValue TProfiler::Increment(TAggregateCounter& counter, TValue delta) const
{
    auto value = (counter.Current_ += delta);
    OnUpdated(counter, value);
    return value;
}

void TProfiler::Update(TSimpleGauge& counter, TValue value) const
{
    counter.Current_ = value;
    OnUpdated(counter, EMetricType::Gauge);
}

TValue TProfiler::Increment(TSimpleGauge& counter, TValue delta) const
{
    auto result = counter.Current_.fetch_add(delta, std::memory_order_relaxed) + delta;
    OnUpdated(counter, EMetricType::Gauge);
    return result;
}

void TProfiler::Reset(TMonotonicCounter& counter) const
{
    counter.Current_ = 0;
    OnUpdated(counter, EMetricType::Counter);
}

TValue TProfiler::Increment(TMonotonicCounter& counter, TValue delta) const
{
    auto result = counter.Current_.fetch_add(delta, std::memory_order_relaxed) + delta;
    OnUpdated(counter, EMetricType::Counter);
    return result;
}

bool TProfiler::IsCounterEnabled(const TCounterBase& counter) const
{
    return Enabled_ && !counter.Path_.empty();
}

void TProfiler::OnUpdated(TAggregateCounter& counter, TValue value) const
{
    if (!IsCounterEnabled(counter)) {
        return;
    }

    auto mode = counter.Mode_;

    counter.SampleCount_ += 1;
    if (mode == EAggregateMode::All || mode == EAggregateMode::Avg) {
        counter.Sum_ += value;
    }

    if (mode == EAggregateMode::All || mode == EAggregateMode::Min) {
        while (true) {
            auto min = counter.Min_.load(std::memory_order_relaxed);
            if (min <= value) {
                break;
            }
            if (counter.Min_.compare_exchange_weak(min, value)) {
                break;
            }
        }
    }

    if (mode == EAggregateMode::All || mode == EAggregateMode::Max) {
        while (true) {
            auto max = counter.Max_.load(std::memory_order_relaxed);
            if (max >= value) {
                break;
            }
            if (counter.Max_.compare_exchange_weak(max, value)) {
                break;
            }
        }
    }

    auto now = GetCpuInstant();
    if (now < counter.Deadline_.load(std::memory_order_relaxed)) {
        return;
    }

    TGuard<TSpinLock> guard(counter.SpinLock_);

    if (now < counter.Deadline_) {
        return;
    }

    auto sampleCount = counter.SampleCount_.load();
    if (sampleCount == 0) {
        return;
    }

    auto min = counter.Min_.load();
    auto max = counter.Max_.load();
    auto avg = counter.Sum_.load() / sampleCount;
    counter.Reset();
    counter.Deadline_ = now + counter.Interval_;

    guard.Release();

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
            Y_UNREACHABLE();
    }
}

void TProfiler::OnUpdated(TCounterBase& counter, EMetricType metricType) const
{
    if (!IsCounterEnabled(counter)) {
        return;
    }

    auto deadline = counter.Deadline_.load(std::memory_order_relaxed);
    auto now = GetCpuInstant();
    if (now < deadline) {
        return;
    }

    if (!counter.Deadline_.compare_exchange_strong(deadline, now + counter.Interval_, std::memory_order_relaxed)) {
        return;
    }

    Enqueue(
        counter.Path_,
        counter.Current_.load(std::memory_order_relaxed),
        metricType,
        counter.TagIds_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

size_t hash<NYT::NProfiling::TTagIdList>::operator()(const NYT::NProfiling::TTagIdList& list) const
{
    size_t result = 1;
    for (auto tag : list) {
        result = NYT::FarmFingerprint(result, tag);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////
