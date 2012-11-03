#pragma once

#include "public.h"

#include <ytlib/logging/log.h>

#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ObjectServerLogger;
extern NProfiling::TProfiler ObjectServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

