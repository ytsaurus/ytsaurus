#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/profiler.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger SecurityServerLogger;
extern const NLogging::TLogger AccessLogger;
extern const NProfiling::TProfiler SecurityProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
