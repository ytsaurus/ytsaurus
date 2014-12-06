#pragma once

#include "public.h"

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

#include <server/hydra/entity_map.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLog::TLogger SecurityServerLogger;
extern NProfiling::TProfiler SecurityServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
