#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>
#include <ytlib/misc/enum.h>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

//! Describes timer mode.
/*!
 *  - Simple: Measures the interval between start and stop.
 *  This timer creates a single bucket that stores the above interval.
 *  
 *  - Sequential: Measure the intervals between checkpoints
 *  (start being the first checkpoint) and also the total time (between start and stop).
 *  This timer creates a bucket per each checkpoint plus "total" bucket.
 *  
 *  - Parallel: Measures the intervals between start and checkpoints
 *  and also the total time (between start and stop).
 *  This timer creates a bucket per each checkpoint plus "total" bucket.
 */
DECLARE_ENUM(ETimerMode,
    (Simple)
    (Sequential)
    (Parallel)
);

//! Timing state.
struct TTimer
{
    TTimer()
        : Start(0)
        , LastCheckpoint(0)
    { }

    TTimer(const NYTree::TYPath& path, ui64 start, ETimerMode mode)
        : Path(path)
        , Start(start)
        , LastCheckpoint(0)
        , Mode(mode)
    { }

    NYTree::TYPath Path;
    //! Start time.
    ui64 Start;
    //! Last checkpoint time (0 if no checkpoint has occurred yet).
    ui64 LastCheckpoint;
    ETimerMode Mode;
};

////////////////////////////////////////////////////////////////////////////////

//! Provides a client API to profiling infrastructure.
/*!
 *  A profiler maintains a path prefix that is added automatically all enqueued samples.
 *  It allows new samples to be enqueued and time measurements to be performed.
 */
class TProfiler
{
public:
    //! Constructs a new profiler for a given prefix.
    explicit TProfiler(const NYTree::TYPath& pathPrefix);

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

private:
    NYTree::TYPath PathPrefix;

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
    explicit TTimingGuard(TProfiler* profiler, const NYTree::TYPath& path)
        : Profiler(profiler)
        , Timer(profiler->TimingStart(path))
    {
        YASSERT(profiler);
    }

    ~TTimingGuard()
    {
        // Don't measure anything if an exception was raised.
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

//! Enables measuring the execution time of the statement that immediately follows this macro.
#define PROFILE_TIMING(path) \
    if (auto PROFILE_TIMING__Guard = NYT::NProfiling::TTimingGuard(&Profiler, path)) \
    { } \
    else

//! Must be used inside #PROFILE_TIMING block to mark a checkpoint.
#define PROFILE_TIMING_CHECKPOINT(pathSuffix) \
    PROFILE_TIMING__Guard.Checkpoint(pathSuffix)

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
