#pragma once

#include <yt/yt/library/profiling/impl.h>

#include <library/cpp/safe_stats/safe_stats.h>


namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

NYT::NProfiling::TProfiler CreateSolomonContextProfiler(NSFStats::TSolomonContext solomonCtx);
NSFStats::TSolomonContext UnwrapSolomonContextProfiler(const NYT::NProfiling::TProfiler& profiler);
void FlushProfiler(const NYT::NProfiling::TProfiler& profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
