#include "stdafx.h"
#include "profiler.h"
#include "profiling_manager.h"
#include "timing.h"

#include <ytlib/ytree/ypath_client.h>

#include <util/datetime/cputimer.h>

namespace NYT {
namespace NProfiling  {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TTimer::TTimer()
    : Start(0)
    , LastCheckpoint(0)
{ }

TTimer::TTimer(const TYPath& path, TCpuInstant start, ETimerMode mode)
    : Path(path)
    , Start(start)
    , LastCheckpoint(0)
    , Mode(mode)
{ }

////////////////////////////////////////////////////////////////////////////////

TCounterBase::TCounterBase(const TYPath& path, TDuration interval)
    : Path(path)
    , Interval(DurationToCycles(interval))
    , Deadline(0)
{ }

////////////////////////////////////////////////////////////////////////////////

TRateCounter::TRateCounter(const TYPath& path, TDuration interval)
    : TCounterBase(path, interval)
    , Value(0)
    , LastTime(0)
    , LastValue(0)
{ }

////////////////////////////////////////////////////////////////////////////////

TAggregateCounter::TAggregateCounter(
    const NYTree::TYPath& path,
    EAggregateMode mode,
    TDuration interval)
    : TCounterBase(path, interval)
    , Mode(mode)
{
    Reset();
}

void TAggregateCounter::Reset()
{
    Min = std::numeric_limits<TValue>::max();
    Max = std::numeric_limits<TValue>::min();
    Sum = 0;
    SampleCount = 0;
}

////////////////////////////////////////////////////////////////////////////////

TProfiler::TProfiler(const TYPath& pathPrefix, bool selfProfiling)
    : PathPrefix(pathPrefix)
    , SelfProfiling(selfProfiling)
{ }

void TProfiler::Enqueue(const TYPath& path, TValue value)
{
    TQueuedSample sample;
    sample.Time = GetCpuInstant();
    sample.Path = PathPrefix + "/" + path;
    sample.Value = value;
    TProfilingManager::Get()->Enqueue(sample, SelfProfiling);
}

TTimer TProfiler::TimingStart(const TYPath& path, ETimerMode mode)
{
    return TTimer(path, GetCpuInstant(), mode);
}

void TProfiler::TimingStop(TTimer& timer)
{
    // Failure here means that the timer was not started or already stopped.
    YASSERT(timer.Start != 0);

    auto now = GetCpuInstant();
    auto value = CpuDurationToValue(now - timer.Start);
    YASSERT(value >= 0);

    switch (timer.Mode) {
        case ETimerMode::Simple:
            Enqueue(timer.Path, value);
            break;

        case ETimerMode::Sequential:
        case ETimerMode::Parallel:
            Enqueue(timer.Path +  "/total", value);
            break;

        default:
            YUNREACHABLE();
    }

    timer.Start = 0;
}

void TProfiler::TimingCheckpoint(TTimer& timer, const TYPath& pathSuffix)
{
    // Failure here means that the timer was not started or already stopped.
    YASSERT(timer.Start != 0);

    auto now = GetCpuInstant();

    // Upon receiving the first checkpoint Simple timer
    // is automatically switched into Sequential.
    if (timer.Mode == ETimerMode::Simple) {
        timer.Mode = ETimerMode::Sequential;
    }

    auto path = timer.Path + "/" + pathSuffix;
    switch (timer.Mode) {
        case ETimerMode::Sequential: {
            auto lastCheckpoint = timer.LastCheckpoint == 0 ? timer.Start : timer.LastCheckpoint;
            auto duration = CpuDurationToValue(now - lastCheckpoint);
            YASSERT(duration >= 0);
            Enqueue(path, duration);
            timer.LastCheckpoint = now;
            break;
        }

        case ETimerMode::Parallel: {
            auto duration = CpuDurationToValue(now - timer.Start);
            YASSERT(duration >= 0);
            Enqueue(path, duration);
            break;
        }

        default:
            YUNREACHABLE();
    }
}

void TProfiler::Increment(TRateCounter& counter, TValue delta /*= 1*/)
{
    YASSERT(delta >= 0);
    counter.Value += delta;
    auto now = GetCpuInstant();
    if (now > counter.Deadline) {
        if (counter.LastTime != 0) {
            auto counterDelta = counter.Value - counter.LastValue;
            auto timeDelta = now - counter.LastTime;
            auto sampleValue = counterDelta * counter.Interval / timeDelta;
            Enqueue(counter.Path, sampleValue);
        }
        counter.LastTime = now;
        counter.LastValue = counter.Value;
        counter.Deadline = now + counter.Interval;
    }
}

void TProfiler::Aggregate(TAggregateCounter& counter, TValue value)
{
    ++counter.SampleCount;
    counter.Min = std::min(counter.Min, value);
    counter.Max = std::max(counter.Max, value);
    counter.Sum += value;
    auto now = GetCpuInstant();
    if (now > counter.Deadline) {
        TValue min = counter.Min;
        TValue max = counter.Max;
        TValue avg = counter.Sum / counter.SampleCount;
        switch (counter.Mode) {
            case EAggregateMode::All:
                Enqueue(counter.Path + "/min", min);
                Enqueue(counter.Path + "/max", max);
                Enqueue(counter.Path + "/avg", avg);
                break;

            case EAggregateMode::Min:
                Enqueue(counter.Path, min);
                break;

            case EAggregateMode::Max:
                Enqueue(counter.Path, max);
                break;

            case EAggregateMode::Avg:
                Enqueue(counter.Path, avg);
                break;

            default:
                YUNREACHABLE();
        }
        counter.Reset();
        counter.Deadline = now + counter.Interval;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
