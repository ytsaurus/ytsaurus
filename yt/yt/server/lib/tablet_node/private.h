#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger TabletNodeLogger;
extern const NProfiling::TProfiler TabletNodeProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
