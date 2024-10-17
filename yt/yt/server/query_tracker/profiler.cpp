#include "private.h"
#include "profiler.h"

#include <util/digest/multi.h>

namespace NYT::NQueryTracker {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TActiveQueriesProfilingCounter::TActiveQueriesProfilingCounter(const NProfiling::TProfiler& profiler)
    : ActiveQueries(profiler.Gauge("/active_queries"))
{ }

TStateTimeProfilingCounter::TStateTimeProfilingCounter(const NProfiling::TProfiler& profiler)
    : StateTime(profiler.WithDefaultDisabled().TimeGaugeSummary("/state_time", ESummaryPolicy::Max | ESummaryPolicy::Avg))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker

////////////////////////////////////////////////////////////////////////////////

size_t THash<NYT::NQueryTracker::TProfilingTags>::operator()(const NYT::NQueryTracker::TProfilingTags& tag) const
{
    return MultiHash(tag.State, tag.Engine, tag.AssignedTracker);
}

////////////////////////////////////////////////////////////////////////////////
