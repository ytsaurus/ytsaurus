#pragma once

#include "timing.h"

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

//! Continuously tracks the wall time passes since the instance has been created.
class TScopedTimer
{
public:
    TScopedTimer()
        : StartTime_(GetCpuInstant())
    { }

    TInstant GetStart() const
    {
        return CpuInstantToInstant(StartTime_);
    }

    TDuration GetElapsed() const
    {
        return CpuDurationToDuration(GetCpuInstant() - StartTime_);
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
    TDuration* Value_;
    TCpuInstant StartInstant_;
    
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
