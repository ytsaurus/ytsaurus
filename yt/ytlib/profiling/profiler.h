#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

//! A profiler maintains a path prefix that is added automatically
//! to all enqueued samples.
class TProfiler
{
public:
	//! Constructs a new profiler for a given prefix.
    explicit TProfiler(const NYTree::TYPath& pathPrefix);

	//! Enqueues a sample.
    void Enqueue(const NYTree::TYPath& path, TValue value);

	//! Starts measuring a time interval.
	//! Returns the CPU clock of the start point.
    TCpuClock StartTiming();

	//! Stops measuring the time interval and enqueues the sample.
    void StopTiming(const NYTree::TYPath& path, TCpuClock start);

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
		, Path(path)
		, Start(profiler->StartTiming())
	{
		YASSERT(profiler);
	}

    ~TTimingGuard()
	{
		// Don't measure anything if an exception was raised.
		if (!std::uncaught_exception()) {
			Profiler->StopTiming(Path, Start);
		}
	}

    operator bool() const
    {
        return false;
    }

private:
    TProfiler* Profiler;
    NYTree::TYPath Path;
    TCpuClock Start;

};

////////////////////////////////////////////////////////////////////////////////

//! Enqueues a new sample.
#define PROFILE_VALUE(path, value) \
    Profiler.Enqueue(path, value)

//! Enqueues the execution duration of the statement
//! that immediately follows this macro.
#define PROFILE_TIMING(path) \
    if (auto timingGuard_##__LINE__ = \
        NYT::NProfiling::TTimingGuard(&Profiler, path)) \
    { } \
    else

//! Starts measuring a time interval.
//! Returns the CPU clock of the start point.
#define PROFILE_TIMING_START() \
	Profiler.StartTiming()

//! Stops measuring the time interval and enqueues the sample.
#define PROFILE_TIMING_STOP(path, start) \
	Profiler.StopTiming(path, start)

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
