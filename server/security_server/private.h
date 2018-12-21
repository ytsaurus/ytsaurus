#pragma once

#include "public.h"

#include <yt/server/hydra/entity_map.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger SecurityServerLogger;
extern const NProfiling::TProfiler SecurityServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
