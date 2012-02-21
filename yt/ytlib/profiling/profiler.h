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
    void StopTiming(TCpuClock start, const NYTree::TYPath& path);

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

class TTimingGuard
{
public:
    explicit TTimingGuard(TProfiler* profiler, const NYTree::TYPath& path);
    ~TTimingGuard();

    // Keep implementation in header to ensure inlining.
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

#define PROFILE_TIMING_START(path) \
    Profiler.StartScopedTiming(path)

#define PROFILE_TIMING_END(path) \
    Profiler.StopScopedTiming(path)

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
