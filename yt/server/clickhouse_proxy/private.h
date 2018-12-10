#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NClickHouseProxy {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ClickHouseProxyLogger;
extern const NProfiling::TProfiler ClickHouseProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseProxy
