#pragma once

#include "timing.h"

namespace NYT {
namespace NProfiling {


class TSingleTimer
{
public:
    TSingleTimer():
        StartTime_(GetCpuInstant())
    { }

    TDuration ElapsedTime() const
        { return CpuDurationToDuration(GetCpuInstant() - StartTime_); }

    Stroka ElapsedTimeAsString() const
        { return Repr(ElapsedTime()); }

    void Restart()
        { StartTime_ = GetCpuInstant(); }

private:
    Stroka Repr(const TDuration& duration) const {
        return ToString(duration.MilliSeconds() / 1000.0);
    }

    TCpuInstant StartTime_;
};

} // namespace NProfiling
} // namespace NYT
