#pragma once

#include <yt/core/logging/log.h>
#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ServerLogger;
extern const NProfiling::TProfiler ServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
