#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>
#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, ControllerLogger, "FlowController");
YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, BalancerLogger, "FlowBalancer");
YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, WorkerTrackerLogger, "FlowWorkerTracker");

////////////////////////////////////////////////////////////////////////////////

//! Whether a failed commit is worth retrying rather than treating as a breakdown. A tablet in the
//! middle of a smooth movement rejects everything sent to it and comes back within seconds,
//! carrying a redirection hint; anything else (a tablet that is genuinely down, a bad request)
//! outlives a scheduling iteration.
bool IsTransientTabletError(const TError& error);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
