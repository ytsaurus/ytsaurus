#include "ref_counted_tracker_profiler.h"
#include "ref_counted_tracker.h"
#include "singleton.h"

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/actions/invoker_util.h>

#include <yt/core/profiling/profiler.h>

#include <yt/core/misc/ref_counted_tracker_profiler.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static constexpr auto ProfilingPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

class TRefCountedTrackerProfiler
    : public TRefCounted
{
public:
    TRefCountedTrackerProfiler()
        : Executor_(New<NConcurrency::TPeriodicExecutor>(
            GetSyncInvoker(),
            BIND(&TRefCountedTrackerProfiler::OnProfiling, MakeWeak(this)),
            ProfilingPeriod))
    {
        Executor_->Start();
    }

private:
    const NConcurrency::TPeriodicExecutorPtr Executor_;
    const NProfiling::TProfiler Profiler_{"/ref_counted_tracker"};
    
    void OnProfiling()
    {
        auto statistics = TRefCountedTracker::Get()->GetStatistics().TotalStatistics;
        Profiler_.Enqueue("/total/objects_allocated", statistics.ObjectsAllocated, NProfiling::EMetricType::Gauge);
        Profiler_.Enqueue("/total/objects_freed", statistics.ObjectsFreed, NProfiling::EMetricType::Gauge);
        Profiler_.Enqueue("/total/objects_alive", statistics.ObjectsAlive, NProfiling::EMetricType::Gauge);
        Profiler_.Enqueue("/total/bytes_allocated", statistics.BytesAllocated, NProfiling::EMetricType::Gauge);
        Profiler_.Enqueue("/total/bytes_freed", statistics.BytesFreed, NProfiling::EMetricType::Gauge);
        Profiler_.Enqueue("/total/bytes_alive", statistics.BytesAlive, NProfiling::EMetricType::Gauge);
    }
};

void EnableRefCountedTrackerProfiling()
{
    RefCountedSingleton<TRefCountedTrackerProfiler>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
