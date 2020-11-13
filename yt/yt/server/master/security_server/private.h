#pragma once

#include "public.h"

#include <yt/server/lib/hydra/entity_map.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger SecurityServerLogger;
extern const NLogging::TLogger AccessLogger;
extern const NProfiling::TRegistry SecurityProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
