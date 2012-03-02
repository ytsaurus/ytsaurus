#include "stdafx.h"
#include "profiler.h"
#include "profiling_manager.h"

#include <ytlib/ytree/ypath_client.h>

#include <util/system/datetime.h>

namespace NYT {
namespace NProfiling  {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TTimer::TTimer()
    : Start(0)
    , LastCheckpoint(0)
{ }

TTimer::TTimer(const NYTree::TYPath& path, ui64 start, ETimerMode mode)
    : Path(path)
    , Start(start)
    , LastCheckpoint(0)
    , Mode(mode)
{ }

////////////////////////////////////////////////////////////////////////////////

TRateCounter::TRateCounter(
    const NYTree::TYPath& path,
    TDuration interval)
    : Path(path)
    , Interval(DurationToCycles(interval))
    , Value(0)
    , LastValue(0)
    , LastTime(0)
    , Deadline(0)
{ }

////////////////////////////////////////////////////////////////////////////////

TProfiler::TProfiler(const TYPath& pathPrefix)
    : PathPrefix(pathPrefix)
{ }

void TProfiler::Enqueue(const TYPath& path, TValue value)
{
    TQueuedSample sample;
    sample.Time = GetCycleCount();
    sample.Path = CombineYPaths(PathPrefix, path);
    sample.Value = value;
    TProfilingManager::Get()->Enqueue(sample);
}

TTimer TProfiler::TimingStart(const TYPath& path, ETimerMode mode)
{
    return TTimer(path, GetCycleCount(), mode);
}

void TProfiler::TimingStop(TTimer& timer)
{
    //YASSERT(timer.Start != 0);
    if (timer.Start == 0) return;

    auto now = GetCycleCount();
    auto duration = CyclesToDuration(now - timer.Start).MicroSeconds();
    YASSERT(duration >= 0);

    switch (timer.Mode) {
        case ETimerMode::Simple:
            Enqueue(timer.Path, duration);
            break;

        case ETimerMode::Sequential:
        case ETimerMode::Parallel:
            Enqueue(CombineYPaths(timer.Path, "total"), duration);
            break;

        default:
            YUNREACHABLE();
    }

    timer.Start = 0;
}

void TProfiler::TimingCheckpoint(TTimer& timer, const TYPath& pathSuffix)
{
    //YASSERT(timer.Start != 0);
    if (timer.Start == 0) return;

    auto now = GetCycleCount();
    // Upon receiving the first checkpoint
    // Simple timer is automatically switched into Sequential.
    if (timer.Mode == ETimerMode::Simple) {
        timer.Mode = ETimerMode::Sequential;
    }
    auto path = CombineYPaths(timer.Path, pathSuffix);
    switch (timer.Mode) {
        case ETimerMode::Sequential: {
            auto lastCheckpoint =
                timer.LastCheckpoint == 0
                ? timer.Start
                : timer.LastCheckpoint;
            auto duration = CyclesToDuration(now - lastCheckpoint).MicroSeconds();
            YASSERT(duration >= 0);
            Enqueue(path, duration);
            timer.LastCheckpoint = now;
            break;
        }

        case ETimerMode::Parallel: {
            auto duration = CyclesToDuration(now - timer.Start).MicroSeconds();
            YASSERT(duration >= 0);
            Enqueue(path, duration);
            break;
        }

        default:
            YUNREACHABLE();
    }
}

void TProfiler::Increment(TRateCounter& counter, i64 delta /*= 1*/)
{
    YASSERT(delta >= 0);
    counter.Value += delta;
    auto now = GetCycleCount();
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
