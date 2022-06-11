#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger HiveServerLogger("HiveServer");
inline const NProfiling::TProfiler HiveServerProfiler("/hive");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
