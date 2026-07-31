#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>
#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, ControllerLogger, "FlowController");
YT_DEFINE_GLOBAL(const NLogging::TLogger, BalancerLogger, "FlowBalancer");
YT_DEFINE_GLOBAL(const NLogging::TLogger, WorkerTrackerLogger, "FlowWorkerTracker");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
