#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger ObjectServerLogger("ObjectServer");
inline const NProfiling::TProfiler ObjectServerProfiler("/object_server");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

