#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/profiler.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger TableServerLogger;
extern const NProfiling::TProfiler TableServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer

