#pragma once

#include "public.h"

#include <ytlib/logging/log.h>

#include <ytlib/profiling/profiler.h>

#include <ytlib/meta_state/map.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger SecurityServerLogger;
extern NProfiling::TProfiler SecurityServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT