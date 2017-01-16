#include "profiler.h"
#include "profile_manager.h"
#include "timing.h"

#include <yt/core/misc/nullable.h>

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
    : Start(0)
    , LastCheckpoint(0)
{ }

TTimer::TTimer(
    const TYPath& path,
    TCpuInstant start,
    ETimerMode mode,
    const TTagIdList& tagIds)
    : Path(path)
    , Start(start)
    , LastCheckpoint(0)
    , Mode(mode)
    , TagIds(tagIds)
{ }

////////////////////////////////////////////////////////////////////////////////

TCounterBase::TCounterBase(
    const TYPath& path,
    const TTagIdList& tagIds,
    TDuration interval)
    : Path(path)
    , TagIds(tagIds)
    , Interval(DurationToCpuDuration(interval))
    , Deadline(0)
{ }

TCounterBase::TCounterBase(const TCounterBase& other)
{
    *this = other;
}

TCounterBase& TCounterBase::operator=(const TCounterBase& other)
{
    Path = other.Path;
    TagIds = other.TagIds;
    Interval = other.Interval;
    Deadline = 0;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

TAggregateCounter::TAggregateCounter(
    const NYPath::TYPath& path,
    const TTagIdList& tagIds,
    EAggregateMode mode,
    TDuration interval)
    : TCounterBase(path, tagIds, interval)
    , Mode(mode)
    , Current(0)
{
    Reset();
}

TAggregateCounter::TAggregateCounter(const TAggregateCounter& other)
    : TCounterBase(other)
    , Current(0)
{
    *this = other;
    Reset();
}

TAggregateCounter& TAggregateCounter::operator=(const TAggregateCounter& other)
{
    static_cast<TCounterBase&>(*this) = static_cast<const TCounterBase&>(other);
    Current = other.Current;
    Reset();
    return *this;
}

void TAggregateCounter::Reset()
{
    Min = std::numeric_limits<TValue>::max();
    Max = std::numeric_limits<TValue>::min();
    Sum = 0;
    SampleCount = 0;
}

////////////////////////////////////////////////////////////////////////////////

TSimpleCounter::TSimpleCounter(
    const NYPath::TYPath& path,
    const TTagIdList& tagIds,
    TDuration interval)
    : TCounterBase(path, tagIds, interval)
    , Current(0)
{ }

TSimpleCounter::TSimpleCounter(const TSimpleCounter& other)
    : TCounterBase(other)
{
    *this = other;
}

TSimpleCounter& TSimpleCounter::operator=(const TSimpleCounter& other)
{
    static_cast<TCounterBase&>(*this) = static_cast<const TCounterBase&>(other);
    Current = other.Current.load();
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
    if (!Enabled_)
        return;

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
    Y_ASSERT(timer.Start != 0);

    auto now = GetCpuInstant();
    auto cpuDuration = now - timer.Start;
    auto value = CpuDurationToValue(cpuDuration);
    Y_ASSERT(value >= 0);

    auto path = key ? timer.Path + "/" + ToYPathLiteral(*key) : timer.Path;
    auto tagIds = totalTagIds ? timer.TagIds + *totalTagIds : timer.TagIds;
    Enqueue(path, value, EMetricType::Gauge, tagIds);

    timer.Start = 0;

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
    Y_ASSERT(timer.Start != 0);

    auto now = GetCpuInstant();

    // Upon receiving the first checkpoint Simple timer
    // is automatically switched into Sequential.
    if (timer.Mode == ETimerMode::Simple) {
        timer.Mode = ETimerMode::Sequential;
    }

    auto path = key ? timer.Path + "/" + ToYPathLiteral(*key) : timer.Path;
    auto tagIds = checkpointTagIds ? timer.TagIds + *checkpointTagIds : timer.TagIds;
    switch (timer.Mode) {
        case ETimerMode::Sequential: {
            auto lastCheckpoint = timer.LastCheckpoint == 0 ? timer.Start : timer.LastCheckpoint;
            auto duration = CpuDurationToValue(now - lastCheckpoint);
            Y_ASSERT(duration >= 0);
            Enqueue(path, duration, EMetricType::Gauge, tagIds);
            timer.LastCheckpoint = now;
            return CpuDurationToDuration(duration);
        }

        case ETimerMode::Parallel: {
            auto duration = CpuDurationToValue(now - timer.Start);
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
    TGuard<TSpinLock> guard(counter.SpinLock);

    if (IsCounterEnabled(counter)) {
        DoUpdate(counter, value);
    } else {
        counter.Current = value;
    }
}

TValue TProfiler::Increment(TAggregateCounter& counter, TValue delta) const
{
    TGuard<TSpinLock> guard(counter.SpinLock);

    auto result = (counter.Current + delta);

    if (IsCounterEnabled(counter)) {
        DoUpdate(counter, counter.Current + delta);
    } else {
        counter.Current = result;
    }

    return result;
}

void TProfiler::Update(TSimpleCounter& counter, TValue value) const
{
    counter.Current = value;

    if (IsCounterEnabled(counter)) {
        OnUpdated(counter);
    }
}

TValue TProfiler::Increment(TSimpleCounter& counter, TValue delta) const
{
    auto result = (counter.Current += delta);

    if (IsCounterEnabled(counter)) {
        OnUpdated(counter);
    }

    return result;
}

bool TProfiler::IsCounterEnabled(const TCounterBase& counter) const
{
    return Enabled_ && !counter.Path.empty();
}

void TProfiler::DoUpdate(TAggregateCounter& counter, TValue value) const
{
    ++counter.SampleCount;
    counter.Current = value;
    counter.Min = std::min(counter.Min, value);
    counter.Max = std::max(counter.Max, value);
    counter.Sum += value;
    auto now = GetCpuInstant();
    if (now > counter.Deadline) {
        auto min = counter.Min;
        auto max = counter.Max;
        auto avg = counter.Sum / counter.SampleCount;
        counter.Reset();
        counter.Deadline = now + counter.Interval;
        switch (counter.Mode) {
            case EAggregateMode::All:
                Enqueue(counter.Path + "/min", min, EMetricType::Gauge, counter.TagIds);
                Enqueue(counter.Path + "/max", max, EMetricType::Gauge, counter.TagIds);
                Enqueue(counter.Path + "/avg", avg, EMetricType::Gauge, counter.TagIds);
                break;

            case EAggregateMode::Min:
                Enqueue(counter.Path, min, EMetricType::Gauge, counter.TagIds);
                break;

            case EAggregateMode::Max:
                Enqueue(counter.Path, max, EMetricType::Gauge, counter.TagIds);
                break;

            case EAggregateMode::Avg:
                Enqueue(counter.Path, avg, EMetricType::Gauge, counter.TagIds);
                break;

            default:
                Y_UNREACHABLE();
        }
    }
}

void TProfiler::OnUpdated(TSimpleCounter& counter) const
{
    auto now = GetCpuInstant();
    if (now < counter.Deadline)
        return;

    TValue sampleValue;
    {
        TGuard<TSpinLock> guard(counter.SpinLock);

        if (now < counter.Deadline)
            return;

        sampleValue = counter.Current;
        counter.Deadline = now + counter.Interval;
    }

    Enqueue(counter.Path, sampleValue, EMetricType::Counter, counter.TagIds);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
