#pragma once

#include "public.h"

#include <yt/server/hydra/entity_map.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger SecurityServerLogger;
extern const NProfiling::TProfiler SecurityServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
