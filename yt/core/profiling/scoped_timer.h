#pragma once

#include "timing.h"
#include "profiler.h"

#include <yt/core/concurrency/scheduler.h>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

//! Continuously tracks the wall time passes since the instance has been created.
class TScopedTimer
{
public:
    TScopedTimer()
    {
        Restart();
    }

    TInstant GetStart() const
    {
        return CpuInstantToInstant(StartTime_);
    }

    TDuration GetElapsed() const
    {
        return CpuDurationToDuration(Duration_ + GetCurrentDuration());
    }

    TValue GetElapsedValue() const
    {
        return DurationToValue(GetElapsed());
    }

    void Start()
    {
        StartTime_ = GetCpuInstant();
    }

    void Stop()
    {
        Duration_ += GetCurrentDuration();
        StartTime_ = 0;
    }

    void Restart()
    {
        Duration_ = 0;
        Start();
    }

private:
    TCpuDuration GetCurrentDuration() const
    {
        return Max<TCpuDuration>(GetCpuInstant() - StartTime_, 0);
    }

    TCpuInstant StartTime_;
    TCpuDuration Duration_;

};

class TCpuTimer
    : private NConcurrency::TContextSwitchGuard
{
public:
    TCpuTimer()
        : NConcurrency::TContextSwitchGuard(
            [this] () noexcept { Timer_.Stop(); },
            [this] () noexcept { Timer_.Start(); })
    { }

    TValue GetCpuValue()
    {
        return Timer_.GetElapsedValue();
    }

private:
    TScopedTimer Timer_;
};

class TAggregatingTimingGuard
{
public:
    explicit TAggregatingTimingGuard(TDuration* value)
        : Value_(value)
        , StartInstant_(GetCpuInstant())
    { }

    ~TAggregatingTimingGuard()
    {
        *Value_ += CpuDurationToDuration(GetCpuInstant() - StartInstant_);
    }

private:
    TDuration* const Value_;
    const TCpuInstant StartInstant_;
    
};

class TProfilingTimingGuard
{
public:
    TProfilingTimingGuard(
        const TProfiler& profiler,
        TSimpleCounter* counter)
        : Profiler_(profiler)
        , Counter_(counter)
        , StartInstant_(GetCpuInstant())
    { }

    ~TProfilingTimingGuard()
    {
        auto duration = CpuDurationToDuration(GetCpuInstant() - StartInstant_);
        Profiler_.Increment(*Counter_, duration.MicroSeconds());
    }

private:
    const TProfiler& Profiler_;
    TSimpleCounter* const Counter_;
    const TCpuInstant StartInstant_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
