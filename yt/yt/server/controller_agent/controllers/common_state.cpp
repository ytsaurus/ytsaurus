#include "common_state.h"

#include "common_profilers.h"
#include "vanilla_controller.h"

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

namespace {

NConcurrency::TActionQueuePtr ProfilerQueue;

TJobProfilerPtr JobProfilerInstance;
TScheduleJobProfilerPtr ScheduleJobProfilerInstance;

} // namespace

////////////////////////////////////////////////////////////////////////////////

void InitCommonState(const NProfiling::TProfiler& profiler)
{
    InitVanillaProfilers(profiler);

    ProfilerQueue = New<NConcurrency::TActionQueue>("ControllerProfilers");

    // NB(pogorelov): We have tests with multidaemon mode, so several CAs may run in the same process.
    if (!JobProfilerInstance) {
        JobProfilerInstance = New<TJobProfiler>(ProfilerQueue->GetInvoker());
    }
    if (!ScheduleJobProfilerInstance) {
        ScheduleJobProfilerInstance = New<TScheduleJobProfiler>(ProfilerQueue->GetInvoker());
    }
}

const TJobProfilerPtr& GetJobProfiler()
{
    YT_ASSERT(JobProfilerInstance);
    return JobProfilerInstance;
}

const TScheduleJobProfilerPtr& GetScheduleJobProfiler()
{
    YT_ASSERT(ScheduleJobProfilerInstance);
    return ScheduleJobProfilerInstance;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
