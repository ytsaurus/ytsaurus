#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, ChaosServerLogger, "ChaosServer");
inline const NProfiling::TProfiler ChaosServerProfiler("/chaos_server");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
