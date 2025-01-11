#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, SecurityServerLogger, "SecurityServer");
YT_DEFINE_GLOBAL(const NLogging::TLogger, AccessLogger, "Access");

// NB: Changing this value without reign promotion can lead to abnormalities in profiling during rolling update.
static constexpr int AccountProfilingProducerCount = 10;

YT_DEFINE_GLOBAL(const NProfiling::TProfiler, SecurityProfiler, "/security");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, AccountProfiler, "/accounts");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
