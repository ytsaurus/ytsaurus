#pragma once

#include "public.h"

#include <yt/core/ytree/public.h>

#include <yt/core/concurrency/scheduler.h>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

//! Returns the current processor clock (rdtsc).
TCpuInstant GetCpuInstant();

//! Returns the current time (obtained via #GetCpuInstant).
TInstant GetInstant();

//! Converts a number of processor ticks into a regular duration.
TDuration CpuDurationToDuration(TCpuDuration duration);

//! Converts a regular duration into the number of processor ticks.
TCpuDuration DurationToCpuDuration(TDuration duration);

//! Converts a processor clock into the regular time instant.
TInstant CpuInstantToInstant(TCpuInstant instant);

//! Converts a regular time instant into the processor clock.
TCpuInstant InstantToCpuInstant(TInstant instant);

//! Converts a duration to TValue suitable for profiling.
/*!
 *  The current implementation just returns microseconds.
 */
TValue DurationToValue(TDuration duration);

//! Converts a TValue to duration.
/*!
 *  The current implementation assumes that #value is given in microseconds.
 */
TDuration ValueToDuration(TValue value);

//! Converts a CPU duration into TValue suitable for profiling.
TValue CpuDurationToValue(TCpuDuration duration);

////////////////////////////////////////////////////////////////////////////////

//! Continuously tracks the wall time passed since construction.
class TWallTimer
{
public:
    TWallTimer();

    TInstant GetStartTime() const;
    TDuration GetElapsedTime() const;
    TValue GetElapsedValue() const;

    void Start();
    void Stop();
    void Restart();

private:
    TCpuDuration GetCurrentDuration() const;

    TCpuInstant StartTime_;
    TCpuDuration Duration_;

};

//! Similar to TWallTimer but excludes the time passed while the fiber was inactive.
class TCpuTimer
    : public TWallTimer
    , private NConcurrency::TContextSwitchGuard
{
public:
    TCpuTimer();

};

//! Upon destruction, increments the value by the wall time passed since construction.
class TWallTimingGuard
    : public TWallTimer
{
public:
    explicit TWallTimingGuard(TDuration* value);
    ~TWallTimingGuard();

private:
    TDuration* const Value_;

};

class TCpuTimingGuard
    : public TCpuTimer
{
public:
    explicit TCpuTimingGuard(TDuration* value);
    ~TCpuTimingGuard();

private:
    TDuration* const Value_;

};

//! Upon destruction, increments the counter by the wall time passed since construction.
class TProfilingTimingGuard
{
public:
    TProfilingTimingGuard(
        const TProfiler& profiler,
        TSimpleCounter* counter);
    ~TProfilingTimingGuard();

private:
    const TProfiler& Profiler_;
    TSimpleCounter* const Counter_;
    const TCpuInstant StartInstant_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT

#define TIMING_INL_H_
#include "timing-inl.h"
#undef TIMING_INL_H_
