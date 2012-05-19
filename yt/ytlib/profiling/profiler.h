#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>
#include <ytlib/misc/property.h>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  - Simple: Measures the interval between start and stop.
 *  This timer creates a single bucket that stores the above interval.
 *  
 *  - Sequential: Measures intervals between checkpoints
 *  (start being the first checkpoint) and also the total time (between start and stop).
 *  This timer creates a bucket per each checkpoint plus "total" bucket.
 *  
 *  - Parallel: Measures intervals between start and checkpoints
 *  and also the total time (between start and stop).
 *  This timer creates a bucket per each checkpoint plus "total" bucket.
 */
DECLARE_ENUM(ETimerMode,
    (Simple)
    (Sequential)
    (Parallel)
);

//! Timing state.
/*!
 *  Keeps the timing start time and the last checkpoint time.
 *
 *  Thread safety: single
 */
struct TTimer
{
    TTimer();
    TTimer(const NYTree::TYPath& path, TCpuInstant start, ETimerMode mode);

    NYTree::TYPath Path;
    //! Start time.
    TCpuInstant Start;
    //! Last checkpoint time (0 if no checkpoint has occurred yet).
    TCpuInstant LastCheckpoint;
    ETimerMode Mode;
};

////////////////////////////////////////////////////////////////////////////////

//! Base class for all counters.
/*!
 *  Maintains the profiling path and timing information.
 */
struct TCounterBase
{
    TCounterBase(
        const NYTree::TYPath& path,
        TDuration interval = TDuration::MilliSeconds(1000));

    NYTree::TYPath Path;
    //! Interval between samples (in ticks).
    TCpuDuration Interval;
    //! The time when the next sample must be queued (in ticks).
    TCpuInstant Deadline;
};

////////////////////////////////////////////////////////////////////////////////

//! Measures rate of certain event.
/*!
 *  Used to measure rates of high-frequency events. For these events we cannot
 *  afford to use sample-per-instance strategy. Instead we maintain a counter indicating
 *  the total number of events occurred so far and track its increase over
 *  certain fixed intervals of time. E.g. if the interval is 1 second then
 *  this counter will actually be sampling RPS.
 *  
 *  \note Thread safety: single
 */
struct TRateCounter
    : TCounterBase
{
    TRateCounter(
        const NYTree::TYPath& path,
        TDuration interval = TDuration::MilliSeconds(1000));

    //! The time when the last sample was queued (in ticks).
    TCpuInstant LastTime;
    //! The current counter's value.
    TValue Value;
    //! The counter's value at the moment of the last sampling.
    TValue LastValue;
};

////////////////////////////////////////////////////////////////////////////////

/*!
 * - All: The counter creates three buckets with suffixes "min", "max", and "avg"
 *   and enqueues appropriate aggregates.
 *    
 * - Min, Max, Avg: The counter creates a single bucket and enqueues the corresponding
 *   aggregate.
 */
DECLARE_ENUM(EAggregateMode,
    (All)
    (Min)
    (Max)
    (Avg)
);

//! Measures aggregates.
/*!
 *  Used to measure aggregates (min, max, avg) of a rapidly changing value.
 *  The values are aggregated over the time periods specified in the constructor.
 *  
 *  \note Thread safety: single
 */
struct TAggregateCounter
    : TCounterBase
{
    TAggregateCounter(
        const NYTree::TYPath& path,
        EAggregateMode mode = EAggregateMode::Max,
        TDuration interval = TDuration::MilliSeconds(100));

    void Reset();

    EAggregateMode Mode;
    TValue Min;
    TValue Max;
    TValue Sum;
    int SampleCount;
};

////////////////////////////////////////////////////////////////////////////////

//! Provides a client API for profiling infrastructure.
/*!
 *  A profiler maintains a path prefix that is added automatically all enqueued samples.
 *  It allows new samples to be enqueued and time measurements to be performed.
 */
class TProfiler
{
public:
    //! Constructs a new profiler for a given prefix.
    /*!
     *  By default the profiler is enabled.
     */
    TProfiler(
        const NYTree::TYPath& pathPrefix,
        bool selfProfiling = false);

    //! Controls if the profiler is enabled.
    DEFINE_BYVAL_RW_PROPERTY(bool, Enabled);

    //! Enqueues a new sample.
    void Enqueue(const NYTree::TYPath& path, TValue value);

    //! Starts time measurement.
    TTimer TimingStart(
        const NYTree::TYPath& path,
        ETimerMode mode = ETimerMode::Simple);

    //! Marks a checkpoint and enqueues the corresponding sample.
    /*!
     *  If #timer is in Simple mode then it is automatically
     *  switched to Sequential mode.
     */
    void TimingCheckpoint(TTimer& timer, const NYTree::TYPath& pathSuffix);

    //! Stops time measurement and enqueues the "total" sample.
    void TimingStop(TTimer& timer);

    //! Increments the counter and possibly enqueues a rate sample.
    /*!
     *  The default increment is 1, i.e. the counter measures individual events.
     *  Other (positive) values also make sense. E.g. one can set increment to the
     *  number of bytes to be written and thus obtain a throughput counter.
     */
    void Increment(TRateCounter& counter, TValue delta = 1);

    //! Aggregates the value and possibly enqueues samples.
    void Aggregate(TAggregateCounter& counter, TValue value);

private:
    NYTree::TYPath PathPrefix;
    bool SelfProfiling;

};

////////////////////////////////////////////////////////////////////////////////

//! A helper guard for measuring time intervals.
/*!
 *  \note
 *  Keep implementation in header to ensure inlining.
 */
class TTimingGuard
{
public:
    TTimingGuard(TProfiler* profiler, const NYTree::TYPath& path)
        : Profiler(profiler)
        , Timer(profiler->TimingStart(path))
    {
        YASSERT(profiler);
    }

    ~TTimingGuard()
    {
        // Don't measure anything during exception unwinding.
        if (!std::uncaught_exception()) {
            Profiler->TimingStop(Timer);
        }
    }

    void Checkpoint(const NYTree::TYPath& pathSuffix)
    {
        Profiler->TimingCheckpoint(Timer, pathSuffix);
    }

    operator bool() const
    {
        return false;
    }

private:
    TProfiler* Profiler;
    TTimer Timer;

};

////////////////////////////////////////////////////////////////////////////////

//! Measures execution time of the statement that immediately follows this macro.
#define PROFILE_TIMING(path) \
    if (auto PROFILE_TIMING__Guard = NYT::NProfiling::TTimingGuard(&Profiler, path)) \
    { YUNREACHABLE(); } \
    else

//! Must be used inside #PROFILE_TIMING block to mark a checkpoint.
#define PROFILE_TIMING_CHECKPOINT(pathSuffix) \
    PROFILE_TIMING__Guard.Checkpoint(pathSuffix)

////////////////////////////////////////////////////////////////////////////////

TCpuInstant GetCpuInstant();
TValue CpuDurationToValue(TCpuDuration duration);

//! A helper guard for measuring aggregated time intervals.
/*!
 *  \note
 *  Keep implementation in header to ensure inlining.
 */
class TAggregatedTimingGuard
{
public:
    TAggregatedTimingGuard(TProfiler* profiler, TAggregateCounter* counter)
        : Profiler(profiler)
        , Counter(counter)
        , Start(GetCpuInstant())
    {
        YASSERT(profiler);
        YASSERT(counter);
    }

    ~TAggregatedTimingGuard()
    {
        // Don't measure anything during exception unwinding.
        if (!std::uncaught_exception()) {
            auto stop = GetCpuInstant();
            auto value = CpuDurationToValue(stop - Start);
            Profiler->Aggregate(*Counter, value);
        }
    }

    operator bool() const
    {
        return false;
    }

private:
    TProfiler* Profiler;
    TAggregateCounter* Counter;
    TCpuInstant Start;

};

////////////////////////////////////////////////////////////////////////////////

//! Measures aggregated execution time of the statement that immediately follows this macro.
#define PROFILE_AGGREGATED_TIMING(counter) \
    if (auto PROFILE_TIMING__Guard = NYT::NProfiling::TAggregatedTimingGuard(&Profiler, &(counter))) \
    { YUNREACHABLE(); } \
    else

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT

