#include "common_state.h"

#include "vanilla_controller.h"

namespace NYT::NControllerAgent::NControllers {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

void InitCommonState(const NProfiling::TProfiler& profiler)
{
    InitVanillaProfilers(profiler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
