#pragma once

#include "public.h"

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLog::TLogger ObjectServerLogger;
extern NProfiling::TProfiler ObjectServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

