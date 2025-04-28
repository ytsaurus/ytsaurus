#include "common_state.h"

#include "common_profilers.h"
#include "vanilla_controller.h"

namespace NYT::NControllerAgent::NControllers {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

namespace {

TJobProfilerPtr JobProfilerInstance;

} // namespace

////////////////////////////////////////////////////////////////////////////////

void InitCommonState(const NProfiling::TProfiler& profiler)
{
    InitVanillaProfilers(profiler);

    // NB(pogorelov): We have tests with multidaemon mode, so several CAs may run in the same process.
    if (!JobProfilerInstance) {
        JobProfilerInstance = New<TJobProfiler>();
    }
}

const TJobProfilerPtr& GetJobProfiler()
{
    YT_ASSERT(JobProfilerInstance);
    return JobProfilerInstance;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
