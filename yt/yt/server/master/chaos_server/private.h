#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ChaosServerLogger;
extern const NProfiling::TProfiler ChaosServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
