#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ChaosNodeLogger;
extern const NProfiling::TRegistry ChaosNodeProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
