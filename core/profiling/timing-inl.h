#pragma once
#ifndef TIMING_INL_H_
#error "Direct inclusion of this file is not allowed, include timing.h"
// For the sake of sane code completion.
#include "timing.h"
#endif
#undef TIMING_INL_H_

#include "profiler.h"

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TCpuInstant GetCpuInstant()
{
    // See datetime.h
    unsigned hi, lo;
    __asm__ __volatile__("rdtsc"
    : "=a"(lo), "=d"(hi));
    return static_cast<unsigned long long>(lo) | (static_cast<unsigned long long>(hi) << 32);
}

////////////////////////////////////////////////////////////////////////////////

template <class TTimer>
TValueIncrementingTimingGuard<TTimer>::TValueIncrementingTimingGuard(TDuration* value)
    : Value_(value)
{ }

template <class TTimer>
TValueIncrementingTimingGuard<TTimer>::~TValueIncrementingTimingGuard()
{
    *Value_ += Timer_.GetElapsedTime();
}

////////////////////////////////////////////////////////////////////////////////

template <class TTimer>
TCounterIncrementingTimingGuard<TTimer>::TCounterIncrementingTimingGuard(
    const TProfiler& profiler,
    TMonotonicCounter* counter)
    : Profiler_(profiler)
    , Counter_(counter)
{ }

template <class TTimer>
TCounterIncrementingTimingGuard<TTimer>::~TCounterIncrementingTimingGuard()
{
    Profiler_.Increment(*Counter_, Timer_.GetElapsedValue());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
