#pragma once

#include "public.h"

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

#include <ytlib/meta_state/map.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger SecurityServerLogger;
extern NProfiling::TProfiler SecurityServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT