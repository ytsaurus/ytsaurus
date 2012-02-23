#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>
#include <ytlib/misc/enum.h>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ETimerMode,
	(Simple)
	(Sequential)
	(Parallel)
);

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
	ui64 Start;
	ui64 LastCheckpoint;
	ETimerMode Mode;
};

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
    TTimer TimingStart(
		const NYTree::TYPath& path,
		ETimerMode mode = ETimerMode::Simple);

	void TimingCheckpoint(TTimer& timer, const NYTree::TYPath& pathSuffix);

	//! Stops measuring the time interval and enqueues the sample.
    void TimingStop(const TTimer& timer);

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

//! Enqueues the execution duration of the statement
//! that immediately follows this macro.
#define PROFILE_TIMING(path) \
    if (auto PROFILE_TIMING__Guard = NYT::NProfiling::TTimingGuard(&Profiler, path)) \
    { } \
    else

#define PROFILE_TIMING_CHECKPOINT(pathSuffix) \
	PROFILE_TIMING__Guard.Checkpoint(pathSuffix)

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
