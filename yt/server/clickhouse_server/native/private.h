#pragma once

#include <yt/core/logging/log.h>
#include <yt/core/profiling/profiler.h>

namespace NYT::NClickHouseServer::NNative {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ServerLogger;
extern const NProfiling::TProfiler ServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NNative
