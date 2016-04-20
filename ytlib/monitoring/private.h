#pragma once

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NMonitoring {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger MonitoringLogger;
extern const NProfiling::TProfiler MonitoringProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NJournalClient
} // namespace NYT
