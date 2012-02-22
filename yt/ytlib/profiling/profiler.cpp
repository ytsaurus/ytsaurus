#include "stdafx.h"
#include "profiler.h"
#include "profiling_manager.h"

#include <util/system/datetime.h>

namespace NYT {
namespace NProfiling  {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TProfiler::TProfiler(const TYPath& pathPrefix)
    : PathPrefix(pathPrefix)
{ }

void TProfiler::Enqueue(const TYPath& path, TValue value)
{
	TQueuedSample sample;
	sample.Time = GetCycleCount();
	sample.PathPrefix = PathPrefix;
	sample.Path = path;
	sample.Value = value;
	TProfilingManager::Get()->Enqueue(sample);
}

TCpuClock TProfiler::StartTiming()
{
    return GetCycleCount();
}

void TProfiler::StopTiming(const NYTree::TYPath& path, TCpuClock start)
{
    TCpuClock end = GetCycleCount();
    YASSERT(end >= start);
    Enqueue(path, CyclesToDuration(end - start).MicroSeconds());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
