#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, SecurityServerLogger, "SecurityServer");
YT_DEFINE_GLOBAL(const NLogging::TLogger, AccessLogger, "Access");
inline const NProfiling::TProfiler SecurityProfiler("/security");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
