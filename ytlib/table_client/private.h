#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger TableClientLogger;
extern const NProfiling::TProfiler TableClientProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
