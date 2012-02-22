#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TProfiler
{
public:
    explicit TProfiler(const NYTree::TYPath& pathPrefix);

    void Enqueue(const NYTree::TYPath& path, TValue value);

    TCpuClock StartTiming();
    void StopTiming(const NYTree::TYPath& path, TCpuClock start);

private:
    NYTree::TYPath PathPrefix;

};

////////////////////////////////////////////////////////////////////////////////

class TScopedProfiler
    : public TProfiler
{
public:
    explicit TScopedProfiler(const NYTree::TYPath& pathPrefix);

    void StartScopedTiming(const NYTree::TYPath& path);
    void StopScopedTiming(const NYTree::TYPath& path);

private:
    yhash_map<NYTree::TYPath, TCpuClock> Starts;

};

////////////////////////////////////////////////////////////////////////////////

// Keep implementation in header to ensure inlining.
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
		// Don't measure the time if an handled exception was raised.
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

#define PROFILE_VALUE(path, value) \
    Profiler.Enqueue(path, value)

#define PROFILE_TIMING(path) \
    if (auto timingGuard_##__LINE__ = \
        NYT::NProfiling::TTimingGuard(&Profiler, path)) \
    { } \
    else

#define PROFILE_TIMING_START() \
	Profiler.StartTiming()

#define PROFILE_TIMING_STOP(path, start) \
	Profiler.StopTiming(path, start)

#define PROFILE_SCOPED_TIMING_START(path) \
    Profiler.StartScopedTiming(path)

#define PROFILE_SCOPED_TIMING_END(path) \
    Profiler.StopScopedTiming(path)

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
