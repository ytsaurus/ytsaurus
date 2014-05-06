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

class TScopedRaiiTimer
{
public:
    explicit TScopedRaiiTimer(TDuration* value)
        : Value_(value)
        , StartTime_(GetCpuInstant())
    { }

    ~TScopedRaiiTimer()
    {
        *Value_ += CpuDurationToDuration(GetCpuInstant() - StartTime_);
    }

private:
    TDuration* Value_;
    TCpuInstant StartTime_;
    
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
