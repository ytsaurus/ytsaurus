#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NCellarNode {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger CellarNodeLogger;
extern const NProfiling::TProfiler CellarNodeProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode
