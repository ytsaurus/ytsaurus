#pragma once

#include "public.h"

#include <core/misc/property.h>
#include <core/misc/nullable.h>

#include <core/ypath/public.h>

#include <atomic>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

TTagIdList  operator +  (const TTagIdList& a, const TTagIdList& b);
TTagIdList& operator += (TTagIdList& a, const TTagIdList& b);

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
DEFINE_ENUM(ETimerMode,
    (Simple)
    (Sequential)
    (Parallel)
);

//! Timing state.
/*!
 *  Keeps the timing start time and the last checkpoint time.
 *
 *  \note Not thread-safe.
 */
struct TTimer
{
    TTimer();
    TTimer(
        const NYPath::TYPath& path,
        TCpuInstant start,
        ETimerMode mode,
        const TTagIdList& tagIds);

    NYPath::TYPath Path;
    //! Start time.
    TCpuInstant Start;
    //! Last checkpoint time (0 if no checkpoint has occurred yet).
    TCpuInstant LastCheckpoint;
    ETimerMode Mode;
    TTagIdList TagIds;

};

////////////////////////////////////////////////////////////////////////////////

//! Base class for all counters.
/*!
 *  Maintains the profiling path and timing information.
 */
struct TCounterBase
{
    TCounterBase(
        const NYPath::TYPath& path,
        const TTagIdList& tagIds,
        TDuration interval);

    TSpinLock SpinLock;
    NYPath::TYPath Path;
    TTagIdList TagIds;
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
 *  \note Thread-safe.
 */
struct TRateCounter
    : public TCounterBase
{
    TRateCounter(
        const NYPath::TYPath& path = "",
        const TTagIdList& tagIds = EmptyTagIds,
        TDuration interval = TDuration::MilliSeconds(1000));

    // NB: Need to write these by hand because of std::atomic.
    TRateCounter(const TRateCounter& other);
    TRateCounter& operator = (const TRateCounter& other);


    //! The current counter's value.
    std::atomic<TValue> Value;

    //! The counter's value at the moment of the last sampling.
    TValue LastValue;

    //! The time when the last sample was queued (in ticks).
    TCpuInstant LastTime;

};

////////////////////////////////////////////////////////////////////////////////

/*!
 * - All: The counter creates three buckets with suffixes "min", "max", and "avg"
 *   and enqueues appropriate aggregates.
 *
 * - Min, Max, Avg: The counter creates a single bucket and enqueues the corresponding
 *   aggregate.
 */
DEFINE_ENUM(EAggregateMode,
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
 *  \note Thread-safe.
 */
struct TAggregateCounter
    : public TCounterBase
{
    TAggregateCounter(
        const NYPath::TYPath& path = "",
        const TTagIdList& tagIds = EmptyTagIds,
        EAggregateMode mode = EAggregateMode::Max,
        TDuration interval = TDuration::MilliSeconds(100));

    void Reset();

    EAggregateMode Mode;
    TValue Current;
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
    //! Constructs a disabled profiler.
    TProfiler();

    //! Constructs a new profiler for a given prefix.
    /*!
     *  By default the profiler is enabled.
     */
    TProfiler(
        const NYPath::TYPath& pathPrefix,
        const TTagIdList& tagIds = EmptyTagIds,
        bool selfProfiling = false);

    //! Path prefix for each enqueued sample.
    DEFINE_BYVAL_RW_PROPERTY(NYPath::TYPath, PathPrefix);

    //! If |false| then no samples are emitted.
    DEFINE_BYVAL_RW_PROPERTY(bool, Enabled);

    //! Tags to append to each enqueued sample.
    DEFINE_BYREF_RW_PROPERTY(TTagIdList, TagIds);


    //! Enqueues a new sample with tags.
    void Enqueue(
        const NYPath::TYPath& path,
        TValue value,
        const TTagIdList& tagIds = EmptyTagIds);


    //! Starts time measurement.
    TTimer TimingStart(
        const NYPath::TYPath& path,
        const TTagIdList& tagIds = EmptyTagIds,
        ETimerMode mode = ETimerMode::Simple);


    //! Marks a checkpoint and enqueues the corresponding sample.
    /*!
     *  Returns the time passed from the previous duration.
     *
     *  If #timer is in Simple mode then it is automatically
     *  switched to Sequential mode.
     */
    TDuration TimingCheckpoint(TTimer& timer, const TStringBuf& key);

    //! Same as above but uses tags instead of keys.
    TDuration TimingCheckpoint(TTimer& timer, const TTagIdList& checkpointTagIds);


    //! Stops time measurement and enqueues the "total" sample.
    //! Returns the total duration.
    TDuration TimingStop(TTimer& timer, const TStringBuf& key);

    //! Same as above but uses tags instead of keys.
    TDuration TimingStop(TTimer& timer, const TTagIdList& totalTagIds);

    //! Same as above but neither tags the point nor changes the path.
    TDuration TimingStop(TTimer& timer);


    //! Increments the counter and possibly enqueues a rate sample.
    //! Returns the incremented value.
    /*!
     *  The default increment is 1, i.e. the counter measures individual events.
     *  Other (positive) values also make sense. E.g. one can set increment to the
     *  number of bytes to be written and thus obtain a throughput counter.
     */
    TValue Increment(TRateCounter& counter, TValue delta = 1);

    //! Aggregates the value and possibly enqueues samples.
    void Aggregate(TAggregateCounter& counter, TValue value);

    //! Aggregates |current + delta| and possibly enqueues samples.
    TValue Increment(TAggregateCounter& counter, TValue delta = 1);

private:
    bool SelfProfiling;

    void DoAggregate(
        TAggregateCounter& counter,
        TGuard<TSpinLock>& guard,
        TValue value,
        TCpuInstant now);

    TDuration DoTimingCheckpoint(
        TTimer& timer,
        const TNullable<TStringBuf>& key,
        const TNullable<TTagIdList>& checkpointTagIds);

    TDuration DoTimingStop(
        TTimer& timer,
        const TNullable<TStringBuf>& key,
        const TNullable<TTagIdList>& totalTagIds);

};

////////////////////////////////////////////////////////////////////////////////

//! A helper guard for measuring time intervals.
/*!
 *  \note
 *  Keep implementation in header to ensure inlining.
 */
class TTimingGuard
    : private TNonCopyable
{
public:
    TTimingGuard(
        TProfiler* profiler,
        const NYPath::TYPath& path,
        const TTagIdList& tagIds = EmptyTagIds)
        : Profiler(profiler)
        , Timer(Profiler->TimingStart(path, tagIds))
    { }

    TTimingGuard(TTimingGuard&& other)
        : Profiler(other.Profiler)
        , Timer(other.Timer)
    {
        other.Profiler = nullptr;
    }


    ~TTimingGuard()
    {
        // Don't measure anything during exception unwinding.
        if (!std::uncaught_exception() && Profiler) {
            Profiler->TimingStop(Timer);
        }
    }

    void Checkpoint(const TStringBuf& key)
    {
        Profiler->TimingCheckpoint(Timer, key);
    }

    //! Needed for PROFILE_TIMING.
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
#define PROFILE_TIMING(...) \
    if (auto PROFILE_TIMING__Guard = NYT::NProfiling::TTimingGuard(&Profiler, __VA_ARGS__)) \
    { YUNREACHABLE(); } \
    else

//! Must be used inside #PROFILE_TIMING block to mark a checkpoint.
#define PROFILE_TIMING_CHECKPOINT(key) \
    PROFILE_TIMING__Guard.Checkpoint(key)

////////////////////////////////////////////////////////////////////////////////

TCpuInstant GetCpuInstant();
TValue CpuDurationToValue(TCpuDuration duration);

//! A helper guard for measuring aggregated time intervals.
/*!
 *  \note
 *  Keep implementation in header to ensure inlining.
 */
class TAggregatedTimingGuard
    : private TNonCopyable
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

    TAggregatedTimingGuard(TAggregatedTimingGuard&& other)
        : Profiler(other.Profiler)
        , Counter(other.Counter)
        , Start(other.Start)
    {
        other.Profiler = nullptr;
    }

    ~TAggregatedTimingGuard()
    {
        // Don't measure anything during exception unwinding.
        if (!std::uncaught_exception() && Profiler) {
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

