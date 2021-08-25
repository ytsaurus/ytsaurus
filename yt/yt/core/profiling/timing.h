#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

//! Returns the current processor clock (rdtsc).
TCpuInstant GetCpuInstant();

//! Returns the current time (obtained via #GetCpuInstant).
TInstant GetInstant();

//! Converts a number of processor ticks into a regular duration.
TDuration CpuDurationToDuration(TCpuDuration cpuDuration);

//! Converts a regular duration into the number of processor ticks.
TCpuDuration DurationToCpuDuration(TDuration duration);

//! Converts a processor clock into the regular time instant.
TInstant CpuInstantToInstant(TCpuInstant cpuInstant);

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
TValue CpuDurationToValue(TCpuDuration cpuDuration);

////////////////////////////////////////////////////////////////////////////////

//! Continuously tracks the wall time passed since construction.
class TWallTimer
{
public:
    TWallTimer(bool start = true);

    TInstant GetStartTime() const;
    TDuration GetElapsedTime() const;
    TValue GetElapsedValue() const;

    TCpuInstant GetStartCpuTime() const;
    TCpuDuration GetElapsedCpuTime() const;

    void Start();
    void StartIfNotActive();
    void Stop();
    void Restart();

    void Persist(const TStreamPersistenceContext& context);

private:
    TCpuDuration GetCurrentDuration() const;

    TCpuInstant StartTime_ = 0;
    TCpuDuration Duration_ = 0;
    bool Active_ = false;
};

////////////////////////////////////////////////////////////////////////////////

//! Upon destruction, increments the value by the elapsed time (measured by the timer)
//! passed since construction.
template <class TTimer>
class TValueIncrementingTimingGuard
{
public:
    explicit TValueIncrementingTimingGuard(TDuration* value);
    ~TValueIncrementingTimingGuard();

    TValueIncrementingTimingGuard(const TValueIncrementingTimingGuard&) = delete;
    TValueIncrementingTimingGuard& operator=(const TValueIncrementingTimingGuard&) = delete;

private:
    TDuration* const Value_;
    TTimer Timer_;
};

////////////////////////////////////////////////////////////////////////////////

//! Similar to TWallTimer but excludes the time passed while the fiber was inactive.
class TFiberWallTimer
    : public TWallTimer
    , private NConcurrency::TContextSwitchGuard
{
public:
    TFiberWallTimer();
};

////////////////////////////////////////////////////////////////////////////////

//! Calls TTimer::Start() on construction and TTimer::Stop() on destruction.
template <class TTimer>
class TTimerGuard
{
public:
    explicit TTimerGuard(TTimer* timer);
    ~TTimerGuard();

private:
    TTimer* const Timer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

#define TIMING_INL_H_
#include "timing-inl.h"
#undef TIMING_INL_H_
