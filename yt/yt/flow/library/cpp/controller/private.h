#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>
#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, ControllerLogger, "FlowController");
YT_DEFINE_GLOBAL(const NLogging::TLogger, BalancerLogger, "FlowBalancer");
YT_DEFINE_GLOBAL(const NLogging::TLogger, WorkerTrackerLogger, "FlowWorkerTracker");
YT_DEFINE_GLOBAL(const NLogging::TLogger, PublicControllerLogger, "PublicFlowController");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, ControllerProfiler, "", "yt.flow.controller");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
