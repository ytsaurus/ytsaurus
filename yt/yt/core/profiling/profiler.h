#pragma once

#include "tscp.h"

#include <yt/core/misc/optional.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

#include <yt/core/ypath/public.h>

#include <yt/core/concurrency/spinlock.h>

#include <atomic>

namespace NYT::NProfiling {

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

    friend class TLegacyProfiler;
};

////////////////////////////////////////////////////////////////////////////////

class TCounterBase
{
public:
    static constexpr auto DefaultInterval = TDuration::MilliSeconds(1000);

    TCounterBase(
        const NYPath::TYPath& path,
        const TTagIdList& tagIds,
        TDuration interval);

    TCounterBase(const TCounterBase& other);
    TCounterBase& operator = (const TCounterBase& other);

    TCpuInstant GetUpdateDeadline() const;

protected:
    void Reset();

private:
    NYPath::TYPath Path_;
    TTagIdList TagIds_;
    //! Interval between samples (in ticks).
    TCpuDuration Interval_;
    //! The time when the next sample must be queued (in ticks).
    std::atomic<TCpuInstant> Deadline_ = 0;

    bool CheckAndPromoteUpdateDeadline(TTscp tscp, bool forceEnqueue);

    friend class TLegacyProfiler;
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

class TShardedAggregateGaugeBase
    : public TCounterBase
{
public:
    TShardedAggregateGaugeBase(
        const NYPath::TYPath& path,
        const TTagIdList& tagIds,
        EAggregateMode mode,
        TDuration interval);

    TShardedAggregateGaugeBase(const TShardedAggregateGaugeBase& other);
    TShardedAggregateGaugeBase& operator = (const TShardedAggregateGaugeBase& other);

protected:
    EAggregateMode Mode_;

    struct alignas(CacheLineSize) TShard
    {
        std::atomic<TValue> Min;
        std::atomic<TValue> Max;
        std::atomic<TValue> Sum;
        std::atomic<int> Count;
        std::atomic<TCpuInstant> Instant;
        std::atomic<TValue> Current;
    };

    struct TDynamicData
        : public TRefTracked<TDynamicData>
    {
        std::array<TShard, TTscp::MaxProcessorId> Shards;
    };

    std::unique_ptr<TDynamicData> DynamicData_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);

    struct TStats
    {
        TValue Min;
        TValue Max;
        TValue Sum;
        int Count;
    };
    TStats CollectShardStats();

    void UpdateShards(TValue value, TTscp tscp);
    void Reset();

    friend class TLegacyProfiler;
};

////////////////////////////////////////////////////////////////////////////////

//! Measures aggregates.
/*!
 *  Used to measure aggregates (min, max, avg) of a rapidly changing value.
 *  The values are aggregated over the time intervals specified in the constructor.
 *
 *  Internally implemented as a sharded collection of per-core counters.
 *  Scales well in case of concurrent access.
 *  The stored value is somewhat racy and is not serializable.
 *
 *  Each instance of TShardedAggregateGauge costs ~1Kb on heap.
 */
class TShardedAggregateGauge
    : public TShardedAggregateGaugeBase
{
public:
    TShardedAggregateGauge(
        const NYPath::TYPath& path = {},
        const TTagIdList& tagIds = {},
        EAggregateMode mode = EAggregateMode::Max,
        TDuration interval = DefaultInterval);

    TShardedAggregateGauge(const TShardedAggregateGauge& other);
    TShardedAggregateGauge& operator = (const TShardedAggregateGauge& other);

    void Update(TValue value, TTscp tscp);
    void Reset();

    TValue GetCurrent() const;
};

////////////////////////////////////////////////////////////////////////////////

//! Similar to TShardedAggregateGauge but maintains a single atomic value.
/*!
 *  Internally implemented as a sharded collection of per-core counters plus
 *  a designated current atomic value.
 *
 *  Slower than TShardedAggregateGauge and does not scale in case of concurrent access.
 *  The stored value is serializable.
 *
 *  Each instance of TAtomicShardedAggregateGauge costs ~1Kb on heap.
 */
class TAtomicShardedAggregateGauge
    : public TShardedAggregateGaugeBase
{
public:
    TAtomicShardedAggregateGauge(
        const NYPath::TYPath& path = {},
        const TTagIdList& tagIds = {},
        EAggregateMode mode = EAggregateMode::Max,
        TDuration interval = DefaultInterval);

    TAtomicShardedAggregateGauge(const TAtomicShardedAggregateGauge& other);
    TAtomicShardedAggregateGauge& operator = (const TAtomicShardedAggregateGauge& other);

    void Update(TValue value, TTscp tscp);
    TValue Increment(TValue delta, TTscp tscp);
    void Reset();

    TValue GetCurrent() const;

private:
    std::atomic<TValue> Current_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! A counter optimized for high-rate increments.
/*!
 *  Typically used for handling non-negative increments (hence the name).
 *
 *  Passing negative deltas is supported by not encouraged since in
 *  case of a jitter it is impossible to observe it by looking at profiling data.
 *
 *  Internally implemented as a sharded collection of per-core counters.
 *  Scales well in case of concurrent access.
 *  The stored value is somewhat racy and is not serializable.
 *
 *  Each instance of TShardedMonotonicCounter costs ~1Kb on heap.
 */
class TShardedMonotonicCounter
    : public TCounterBase
{
public:
    TShardedMonotonicCounter(
        const NYPath::TYPath& path = {},
        const TTagIdList& tagIds = {},
        TDuration interval = DefaultInterval);

    TShardedMonotonicCounter(const TShardedMonotonicCounter& other);
    TShardedMonotonicCounter& operator = (const TShardedMonotonicCounter& other);

    void Increment(TValue delta, TTscp tscp);
    void Reset();

    TValue GetCurrent() const;

private:
    struct alignas(CacheLineSize) TShard
    {
        std::atomic<TValue> Delta;
    };

    struct TDynamicData
        : public TRefTracked<TDynamicData>
    {
        std::array<TShard, TTscp::MaxProcessorId> Shards;
    };

    std::unique_ptr<TDynamicData> DynamicData_;

    friend class TLegacyProfiler;
};

////////////////////////////////////////////////////////////////////////////////

//! A gauge storing just a single value.
/*!
 *  Exports the value as-is, w/o aggregation.
 *
 *  Does not scale in case of concurrent access.
 *  The stored value is serializable.
 *
 *  Instances of TAtomicGauge do not use heap allocations and cost O(1) memory.
 */
class TAtomicGauge
    : public TCounterBase
{
public:
    TAtomicGauge(
        const NYPath::TYPath& path = {},
        const TTagIdList& tagIds = {},
        TDuration interval = DefaultInterval);

    TAtomicGauge(const TAtomicGauge& other);
    TAtomicGauge& operator = (const TAtomicGauge& other);

    void Update(TValue value);
    TValue Increment(TValue delta);
    void Reset();

    TValue GetCurrent() const;

private:
    std::atomic<TValue> Current_ = 0;

    friend class TLegacyProfiler;
};

////////////////////////////////////////////////////////////////////////////////

//! Provides a client API for profiling infrastructure.
/*!
 *  A profiler maintains a path prefix that is added automatically all enqueued samples.
 *  It allows new samples to be enqueued and time measurements to be performed.
 */
class TLegacyProfiler
{
public:
    //! Constructs a disabled profiler.
    TLegacyProfiler() = default;

    //! Constructs a new profiler for a given prefix.
    /*!
     *  By default the profiler is enabled.
     */
    TLegacyProfiler(
        const NYPath::TYPath& pathPrefix,
        const TTagIdList& tagIds = {},
        bool selfProfiling = false,
        bool forceEnqueue = false);

    //! Path prefix for each enqueued sample.
    DEFINE_BYVAL_RW_PROPERTY(NYPath::TYPath, PathPrefix);

    //! If |false| then no samples are emitted.
    DEFINE_BYVAL_RW_PROPERTY(bool, Enabled, false);

    //! Tags to append to each enqueued sample.
    DEFINE_BYREF_RW_PROPERTY(TTagIdList, TagIds);

    //! If |true|, then Enqueue will be invoked on every counter/gauge update.
    //! This value is inheritable via AppendPath and AddTags.
    DEFINE_BYREF_RW_PROPERTY(bool, ForceEnqueue)


    //! Constructs a new profiler by adding a suffix to the path.
    TLegacyProfiler AppendPath(const NYPath::TYPath& pathSuffix) const;

    //! Constructs a new profiler by adding more tags.
    TLegacyProfiler AddTags(const TTagIdList& tagIds) const;


    //! Enqueues a new sample with tags.
    void Enqueue(
        const NYPath::TYPath& path,
        TValue value,
        EMetricType metricType,
        const TTagIdList& tagIds = {}) const;


    //! Starts time measurement.
    TTimer TimingStart(
        const NYPath::TYPath& path,
        const TTagIdList& tagIds = {},
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


    void Update(TShardedAggregateGauge& gauge, TValue value, TTscp tscp = TTscp::Get()) const;

    void Update(TAtomicShardedAggregateGauge& gauge, TValue value, TTscp tscp = TTscp::Get()) const;
    TValue Increment(TAtomicShardedAggregateGauge& gauge, TValue delta = 1, TTscp tscp = TTscp::Get()) const;

    void Update(TAtomicGauge& gauge, TValue value, TTscp tscp = TTscp::Get()) const;
    TValue Increment(TAtomicGauge& gauge, TValue delta = 1, TTscp tscp = TTscp::Get()) const;

    void Increment(TShardedMonotonicCounter& counter, TValue delta = 1, TTscp tscp = TTscp::Get()) const;
    void Reset(TShardedMonotonicCounter& counter, TTscp tscp = TTscp::Get()) const;

private:
    bool SelfProfiling_ = false;

    template <class T>
    bool OnCounterUpdatedPrologue(T& counter, TTscp tscp) const;
    void OnCounterUpdated(TShardedAggregateGauge& counter, TTscp tscp) const;
    template <class T>
    void OnCounterUpdated(T& counter, EMetricType metricType, TTscp tscp) const;

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
        const TLegacyProfiler* profiler,
        const NYPath::TYPath& path,
        const TTagIdList& tagIds = {})
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
    explicit operator bool() const
    {
        return false;
    }

private:
    const TLegacyProfiler* Profiler_;
    TTimer Timer_;

};

////////////////////////////////////////////////////////////////////////////////

//! Measures execution time of the statement that immediately follows this macro.
#define PROFILE_TIMING(...) \
    if (auto PROFILE_TIMING__Guard = NYT::NProfiling::TTimingGuard(&Profiler, __VA_ARGS__)) \
    { YT_ABORT(); } \
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
    TAggregatedTimingGuard(const TLegacyProfiler* profiler, TShardedAggregateGauge* counter)
        : Profiler_(profiler)
        , Counter_(counter)
        , Start_(GetCpuInstant())
    {
        YT_ASSERT(profiler);
        YT_ASSERT(counter);
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

    explicit operator bool() const
    {
        return false;
    }

private:
    const TLegacyProfiler* Profiler_;
    TShardedAggregateGauge* Counter_;
    TCpuInstant Start_;

};

////////////////////////////////////////////////////////////////////////////////

//! Measures aggregated execution time of the statement that immediately follows this macro.
#define PROFILE_AGGREGATED_TIMING(counter) \
    if (auto PROFILE_TIMING__Guard = NYT::NProfiling::TAggregatedTimingGuard(&Profiler, &(counter))) \
    { YT_ABORT(); } \
    else

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

////////////////////////////////////////////////////////////////////////////////

//! A hasher for TTagIdList.
template <>
struct THash<NYT::NProfiling::TTagIdList>
{
    size_t operator()(const NYT::NProfiling::TTagIdList& ids) const;
};

////////////////////////////////////////////////////////////////////////////////
