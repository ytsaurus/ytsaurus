#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger SecurityServerLogger("SecurityServer");
inline const NLogging::TLogger AccessLogger("Access");

// NB: changing this value without reign promotion can lead to abnormalities in profiling during rolling update.
static constexpr int AccountProfilingProducerCount = 10;

inline const NProfiling::TProfiler SecurityProfiler("/security");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
