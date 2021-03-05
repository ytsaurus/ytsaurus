#include "ref_counted_tracker_profiler.h"
#include "ref_counted_tracker.h"
#include "singleton.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/misc/ref_counted_tracker_profiler.h>

#include <yt/yt/library/profiling/producer.h>

namespace NYT {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TRefCountedTrackerProfiler
    : public ISensorProducer
{
public:
    TRefCountedTrackerProfiler()
    {
        TRegistry registry{"/ref_counted_tracker"};
        registry.AddProducer("/total", MakeStrong(this));
    }

    void Collect(ISensorWriter* writer)
    {
        auto statistics = TRefCountedTracker::Get()->GetStatistics().TotalStatistics;

        writer->AddCounter("/objects_allocated", statistics.ObjectsAllocated);
        writer->AddCounter("/objects_freed", statistics.ObjectsFreed);
        writer->AddGauge("/objects_alive", statistics.ObjectsAlive);

        writer->AddCounter("/bytes_allocated", statistics.BytesAllocated);
        writer->AddCounter("/bytes_freed", statistics.BytesFreed);
        writer->AddGauge("/bytes_alive", statistics.BytesAlive);
    }
};

void EnableRefCountedTrackerProfiling()
{
    RefCountedSingleton<TRefCountedTrackerProfiler>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
