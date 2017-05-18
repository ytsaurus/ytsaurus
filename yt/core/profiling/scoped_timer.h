#pragma once

#include "timing.h"
#include "profiler.h"

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
        auto cpuDuration = static_cast<TCpuDuration>(GetCpuInstant() - StartTime_);
        return cpuDuration < 0 ? TDuration::Zero() : CpuDurationToDuration(cpuDuration);
    }

    void Restart()
    {
        StartTime_ = GetCpuInstant();
    }

private:
    TCpuInstant StartTime_;

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
