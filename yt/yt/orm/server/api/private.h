#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NOrm::NServer::NApi {

////////////////////////////////////////////////////////////////////////////////

inline const NProfiling::TProfiler Profiler("/api");

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Api");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NApi
