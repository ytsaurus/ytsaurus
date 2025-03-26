#pragma once

#include "private.h"

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateVanillaController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

void InitVanillaProfilers(const NProfiling::TProfiler& profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
