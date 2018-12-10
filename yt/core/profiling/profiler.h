#pragma once

#include "public.h"

#include <yt/core/misc/optional.h>
#include <yt/core/misc/property.h>

#include <yt/core/ypath/public.h>

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
class TTimer
{
public:
    TTimer();
    TTimer(
        const NYPath::TYPath& path,
        TCpuInstant start,
        ETimerMode mode,
        const TTagIdList& tagIds);

private:
    NYPath::TYPath Path_;
    //! Start time.
    TCpuInstant Start_;
    //! Last checkpoint time (0 if no checkpoint has occurred yet).
    TCpuInstant LastCheckpoint_;
    ETimerMode Mode_;
    TTagIdList TagIds_;

    friend class TProfiler;
};

////////////////////////////////////////////////////////////////////////////////

//! Base class for all counters.
/*!
 *  Maintains the profiling path and timing information.
 */
class TCounterBase
{
public:
    TCounterBase(
        const NYPath::TYPath& path,
        const TTagIdList& tagIds,
        TDuration interval);
    TCounterBase(const TCounterBase& other);
    TCounterBase& operator = (const TCounterBase& other);

    TValue GetCurrent() const;
    TCpuInstant GetUpdateDeadline() const;

private:
    TSpinLock SpinLock_;
    NYPath::TYPath Path_;
    TTagIdList TagIds_;
    //! Interval between samples (in ticks).
    TCpuDuration Interval_;
    //! The time when the next sample must be queued (in ticks).
    std::atomic<TCpuInstant> Deadline_;
    std::atomic<TValue> Current_;

    friend class TProfiler;
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
class TAggregateGauge
    : public TCounterBase
{
public:
    TAggregateGauge(
        const NYPath::TYPath& path = NYPath::TYPath(),
        const TTagIdList& tagIds = EmptyTagIds,
        EAggregateMode mode = EAggregateMode::Max,
        TDuration interval = TDuration::MilliSeconds(1000));
    TAggregateGauge(const TAggregateGauge& other);
    TAggregateGauge& operator = (const TAggregateGauge& other);

private:
    EAggregateMode Mode_;
    std::atomic<TValue> Min_;
    std::atomic<TValue> Max_;
    std::atomic<TValue> Sum_;
    std::atomic<int> SampleCount_;

    void Reset();

    friend class TProfiler;
};

////////////////////////////////////////////////////////////////////////////////

//! A rudimentary but much cheaper version of TAggregateGauge capable of
//! maintaining just the value itself but not any of its aggregates.
class TMonotonicCounter
    : public TCounterBase
{
public:
    TMonotonicCounter(
        const NYPath::TYPath& path = NYPath::TYPath(),
        const TTagIdList& tagIds = EmptyTagIds,
        TDuration interval = TDuration::MilliSeconds(1000));
    TMonotonicCounter(const TMonotonicCounter& other);
    TMonotonicCounter& operator = (const TMonotonicCounter& other);
};

////////////////////////////////////////////////////////////////////////////////

//! Exports single value as-is.
class TSimpleGauge
    : public TCounterBase
{
public:
    TSimpleGauge(
        const NYPath::TYPath& path = NYPath::TYPath(),
        const TTagIdList& tagIds = EmptyTagIds,
        TDuration interval = TDuration::MilliSeconds(1000));
    TSimpleGauge(const TSimpleGauge& other);
    TSimpleGauge& operator = (const TSimpleGauge& other);
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
    DEFINE_BYVAL_RW_PROPERTY(bool, Enabled, false);

    //! Tags to append to each enqueued sample.
    DEFINE_BYREF_RW_PROPERTY(TTagIdList, TagIds);


    //! Constructs a new profiler by adding a suffix to the path.
    TProfiler AppendPath(const NYPath::TYPath& pathSuffix) const;

    //! Constructs a adding more tags.
    TProfiler AddTags(const TTagIdList& tagIds) const;


    //! Enqueues a new sample with tags.
    void Enqueue(
        const NYPath::TYPath& path,
        TValue value,
        EMetricType metricType,
        const TTagIdList& tagIds = EmptyTagIds) const;


    //! Starts time measurement.
    TTimer TimingStart(
        const NYPath::TYPath& path,
        const TTagIdList& tagIds = EmptyTagIds,
        ETimerMode mode = ETimerMode::Simple) const;


    //! Marks a checkpoint and enqueues the corresponding sample.
    /*!
     *  Returns the time passed from the previous duration.
     *
     *  If #timer is in Simple mode then it is automatically
     *  switched to Sequential mode.
     */
    TDuration TimingCheckpoint(TTimer& timer, TStringBuf key) const;

    //! Same as above but uses tags instead of keys.
    TDuration TimingCheckpoint(TTimer& timer, const TTagIdList& checkpointTagIds) const;


    //! Stops time measurement and enqueues the "total" sample.
    //! Returns the total duration.
    TDuration TimingStop(TTimer& timer, TStringBuf key) const;

    //! Same as above but uses tags instead of keys.
    TDuration TimingStop(TTimer& timer, const TTagIdList& totalTagIds) const;

    //! Same as above but neither tags the point nor changes the path.
    TDuration TimingStop(TTimer& timer) const;


    //! Updates the counter value and possibly enqueues samples.
    void Update(TAggregateGauge& counter, TValue value) const;

    //! Increments the counter value and possibly enqueues aggregate samples.
    //! Returns the incremented value.
    TValue Increment(TAggregateGauge& counter, TValue delta = 1) const;

    void Update(TSimpleGauge& counter, TValue value) const;
    TValue Increment(TSimpleGauge& counter, TValue value = 1) const;

    void Reset(TMonotonicCounter& counter) const;
    TValue Increment(TMonotonicCounter& counter, TValue delta = 1) const;

private:
    bool SelfProfiling_;


    bool IsCounterEnabled(const TCounterBase& counter) const;

    void OnUpdated(TAggregateGauge& counter, TValue value) const;
    void OnUpdated(TCounterBase& counter, EMetricType metricType) const;

    TDuration DoTimingCheckpoint(
        TTimer& timer,
        const std::optional<TStringBuf>& key,
        const std::optional<TTagIdList>& checkpointTagIds) const;

    TDuration DoTimingStop(
        TTimer& timer,
        const std::optional<TStringBuf>& key,
        const std::optional<TTagIdList>& totalTagIds) const;

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
        const TProfiler* profiler,
        const NYPath::TYPath& path,
        const TTagIdList& tagIds = EmptyTagIds)
        : Profiler_(profiler)
        , Timer_(Profiler_->TimingStart(path, tagIds))
    { }

    TTimingGuard(TTimingGuard&& other)
        : Profiler_(other.Profiler_)
        , Timer_(other.Timer_)
    {
        other.Profiler_ = nullptr;
    }

    ~TTimingGuard()
    {
        // Don't measure anything during exception unwinding.
        if (!std::uncaught_exception() && Profiler_) {
            Profiler_->TimingStop(Timer_);
        }
    }

    void Checkpoint(TStringBuf key)
    {
        Profiler_->TimingCheckpoint(Timer_, key);
    }

    //! Needed for PROFILE_TIMING.
    operator bool() const
    {
        return false;
    }

private:
    const TProfiler* Profiler_;
    TTimer Timer_;

};

////////////////////////////////////////////////////////////////////////////////

//! Measures execution time of the statement that immediately follows this macro.
#define PROFILE_TIMING(...) \
    if (auto PROFILE_TIMING__Guard = NYT::NProfiling::TTimingGuard(&Profiler, __VA_ARGS__)) \
    { Y_UNREACHABLE(); } \
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
    TAggregatedTimingGuard(const TProfiler* profiler, TAggregateGauge* counter)
        : Profiler_(profiler)
        , Counter_(counter)
        , Start_(GetCpuInstant())
    {
        Y_ASSERT(profiler);
        Y_ASSERT(counter);
    }

    TAggregatedTimingGuard(TAggregatedTimingGuard&& other)
        : Profiler_(other.Profiler_)
        , Counter_(other.Counter_)
        , Start_(other.Start_)
    {
        other.Profiler_ = nullptr;
    }

    ~TAggregatedTimingGuard()
    {
        // Don't measure anything during exception unwinding.
        if (!std::uncaught_exception() && Profiler_) {
            auto stop = GetCpuInstant();
            auto value = CpuDurationToValue(stop - Start_);
            Profiler_->Update(*Counter_, value);
        }
    }

    operator bool() const
    {
        return false;
    }

private:
    const TProfiler* Profiler_;
    TAggregateGauge* Counter_;
    TCpuInstant Start_;

};

////////////////////////////////////////////////////////////////////////////////

//! Measures aggregated execution time of the statement that immediately follows this macro.
#define PROFILE_AGGREGATED_TIMING(counter) \
    if (auto PROFILE_TIMING__Guard = NYT::NProfiling::TAggregatedTimingGuard(&Profiler, &(counter))) \
    { Y_UNREACHABLE(); } \
    else

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

//! A hasher for TTagIdList.
template <>
struct THash<NYT::NProfiling::TTagIdList>
{
    size_t operator()(const NYT::NProfiling::TTagIdList& ids) const;
};

////////////////////////////////////////////////////////////////////////////////
